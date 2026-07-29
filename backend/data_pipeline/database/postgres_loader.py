"""PostgreSQL access for the pipeline.

The workshop supplies every source as a flat file, but the target architecture
requires PostgreSQL to be a real source system feeding bronze. This module
closes that gap: the people-and-places entities are seeded into PostgreSQL once
from their raw exports, and from then on ingestion reads them over JDBC exactly
as it would read a live operational database.

Seeding is idempotent and is skipped automatically when the tables already hold
data, so a normal pipeline run touches PostgreSQL read-only.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .. import config
from ..utils.io import retry_call, stringify_all_columns
from ..utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_LOGGER = get_logger(__name__)

#: Rows per JDBC batch when writing. Large batches cut round trips materially
#: on the 200k row employee export.
_JDBC_BATCH_SIZE = "10000"


# ---------------------------------------------------------------------------
# Connectivity
# ---------------------------------------------------------------------------


def check_connection() -> bool:
    """Return True when PostgreSQL is reachable with the configured settings."""
    try:
        import psycopg2

        def _connect():
            return psycopg2.connect(
                host=config.POSTGRES.host,
                port=config.POSTGRES.port,
                dbname=config.POSTGRES.database,
                user=config.POSTGRES.user,
                password=config.POSTGRES.password,
                sslmode=config.POSTGRES.sslmode,
                connect_timeout=10,
            )

        connection = retry_call(_connect, description="postgres connect")
        connection.close()
        _LOGGER.info("PostgreSQL reachable", extra=config.POSTGRES.masked())
        return True
    except Exception as exc:
        _LOGGER.error(
            "PostgreSQL unreachable",
            extra={**config.POSTGRES.masked(), "error": str(exc)},
        )
        return False


def ensure_schema() -> None:
    """Create the pipeline schema if it does not already exist."""
    import psycopg2

    def _create():
        connection = psycopg2.connect(
            host=config.POSTGRES.host,
            port=config.POSTGRES.port,
            dbname=config.POSTGRES.database,
            user=config.POSTGRES.user,
            password=config.POSTGRES.password,
            sslmode=config.POSTGRES.sslmode,
            connect_timeout=10,
        )
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    f'CREATE SCHEMA IF NOT EXISTS "{config.POSTGRES_SCHEMA}"'
                )
        finally:
            connection.close()

    retry_call(_create, description="create schema")
    _LOGGER.info("Schema ready", extra={"schema": config.POSTGRES_SCHEMA})


def table_row_count(table: str) -> int:
    """Return the row count of a table, or -1 when it does not exist."""
    import psycopg2

    try:
        connection = psycopg2.connect(
            host=config.POSTGRES.host,
            port=config.POSTGRES.port,
            dbname=config.POSTGRES.database,
            user=config.POSTGRES.user,
            password=config.POSTGRES.password,
            sslmode=config.POSTGRES.sslmode,
            connect_timeout=10,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row = cursor.fetchone()
                return int(row[0]) if row else 0
        finally:
            connection.close()
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# Spark JDBC read / write
# ---------------------------------------------------------------------------


def read_table(spark: "SparkSession", table: str) -> "DataFrame":
    """Read a full table over JDBC.

    Args:
        spark: Active Spark session.
        table: Schema qualified table name.
    """

    def _read() -> "DataFrame":
        return (
            spark.read.format("jdbc")
            .option("url", config.POSTGRES.jdbc_url)
            .option("dbtable", table)
            .options(**config.POSTGRES.jdbc_properties)
            .load()
        )

    frame = retry_call(_read, description=f"jdbc read {table}")
    _LOGGER.info("Read table over JDBC", extra={"table": table})
    return frame


def write_table(df: "DataFrame", table: str, mode: str = "overwrite") -> None:
    """Write a dataframe to PostgreSQL over JDBC.

    ``truncate`` is enabled so an overwrite reuses the existing table definition
    instead of dropping and recreating it, which keeps grants and indexes intact.
    """

    def _write() -> None:
        (
            df.write.format("jdbc")
            .option("url", config.POSTGRES.jdbc_url)
            .option("dbtable", table)
            .option("batchsize", _JDBC_BATCH_SIZE)
            .option("truncate", "true")
            .options(**config.POSTGRES.jdbc_properties)
            .mode(mode)
            .save()
        )

    retry_call(_write, description=f"jdbc write {table}")
    _LOGGER.info("Wrote table over JDBC", extra={"table": table, "mode": mode})


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def seed_source(spark: "SparkSession", source: config.SourceEntity, force: bool = False) -> int:
    """Load one source entity's raw export into PostgreSQL.

    Every column is stored as text, deliberately: the staging tables mirror the
    export exactly, and typing remains a silver layer concern.

    Args:
        spark: Active Spark session.
        source: The registered source entity to seed.
        force: Reseed even when the table already holds rows.

    Returns:
        Number of rows written, or 0 when seeding was skipped.
    """
    from ..utils.io import read_source_file

    table = source.postgres_table
    existing = table_row_count(table)

    if existing > 0 and not force:
        _LOGGER.info(
            "Seed skipped, table already populated",
            extra={"table": table, "rows": existing},
        )
        return 0

    if not source.raw_file.exists():
        raise FileNotFoundError(f"Raw export not found: {source.raw_file}")

    frame = stringify_all_columns(read_source_file(spark, source))
    rows = frame.count()
    write_table(frame, table, mode="overwrite")

    _LOGGER.info(
        "Seeded source into PostgreSQL",
        extra={"table": table, "rows": rows, "file": str(source.raw_file)},
    )
    return rows


def ensure_seeded(spark: "SparkSession", force: bool = False) -> dict[str, int]:
    """Seed every source entity that is routed through PostgreSQL.

    Called once at the start of ingestion. Safe to run repeatedly: entities
    whose tables already hold data are skipped.
    """
    ensure_schema()
    seeded: dict[str, int] = {}
    for source in config.SOURCES:
        if source.effective_ingest_mode != "postgres":
            continue
        seeded[source.entity] = seed_source(spark, source, force=force)
    return seeded