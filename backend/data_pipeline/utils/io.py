"""Shared IO helpers used by every layer.

Centralising reads, writes, lineage stamping and retry logic here is what keeps
the three layer jobs free of duplicated boilerplate. If a transformation rule
appears in two layer modules, it belongs in here instead.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable, TypeVar

from .. import config
from .logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_LOGGER = get_logger(__name__)
T = TypeVar("T")

#: Columns added by the pipeline. Excluded from business logic and dedup keys.
LINEAGE_COLUMNS: tuple[str, ...] = (
    "_batch_id",
    "_ingested_at",
    "_processed_at",
    "_source_system",
    "_source_entity",
    "_source_file",
    "_source_layer",
)


# ---------------------------------------------------------------------------
# Resilience
# ---------------------------------------------------------------------------


def retry_call(
    func: Callable[[], T],
    *,
    description: str,
    attempts: int | None = None,
    base_delay: float | None = None,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Call ``func`` with exponential backoff on transient failures.

    Covers the "database connection timeout" and "S3 access" rows of the error
    handling matrix. Non-transient errors still surface, just after the retries
    are exhausted.

    Raises:
        The final exception if every attempt fails.
    """
    max_attempts = attempts or config.RETRY_ATTEMPTS
    delay = base_delay or config.RETRY_BASE_DELAY_SECONDS
    last: BaseException | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except retry_on as exc:
            last = exc
            if attempt == max_attempts:
                break
            wait = delay * (2 ** (attempt - 1))
            _LOGGER.warning(
                "Operation failed, retrying",
                extra={
                    "operation": description,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "wait_s": round(wait, 2),
                    "error": str(exc),
                },
            )
            time.sleep(wait)

    _LOGGER.error(
        "Operation failed after all retries",
        extra={"operation": description, "attempts": max_attempts, "error": str(last)},
    )
    raise last  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Schema shaping
# ---------------------------------------------------------------------------


def stringify_all_columns(df: "DataFrame") -> "DataFrame":
    """Coerce every column to string, serialising nested structures as JSON.

    Bronze must be a faithful copy of the source, so no value may be lost to a
    type guess. Nested JSON (for example the ``organization`` object in
    teams.json) is preserved as a JSON string and unpacked in silver.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, MapType, StructType

    projections = []
    for field in df.schema.fields:
        column = F.col(f"`{field.name}`")
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            projections.append(F.to_json(column).alias(field.name))
        else:
            projections.append(column.cast("string").alias(field.name))
    return df.select(*projections)


def add_lineage_columns(
    df: "DataFrame",
    *,
    source_system: str,
    entity: str,
    source_path: str,
    source_layer: str,
) -> "DataFrame":
    """Stamp provenance onto every row.

    These columns are what allow any gold record to be traced back to the batch
    and file that produced it, satisfying the lineage requirement at row level.
    """
    from pyspark.sql import functions as F

    now = datetime.now(timezone.utc).isoformat()
    return (
        df.withColumn("_batch_id", F.lit(config.BATCH_ID))
        .withColumn("_ingested_at", F.lit(now))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_entity", F.lit(entity))
        .withColumn("_source_file", F.lit(source_path))
        .withColumn("_source_layer", F.lit(source_layer))
    )


def business_columns(df: "DataFrame") -> list[str]:
    """Return the non-lineage columns of a dataframe."""
    return [c for c in df.columns if c not in LINEAGE_COLUMNS]


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def read_source_file(spark: "SparkSession", source: config.SourceEntity) -> "DataFrame":
    """Read one raw source file with no type inference whatsoever.

    CSV is read with an all-string schema; JSON must be inferred by Spark but is
    immediately flattened to strings by the caller.
    """
    path = str(source.raw_file)

    if source.file_format == "csv":
        reader = (
            spark.read.option("header", "true")
            # Type inference is deliberately disabled. See stringify_all_columns.
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option("quote", '"')
            .option("escape", '"')
            # Quoted fields in this dataset contain commas ("August 19, 2023")
            # and the files use CRLF line endings.
            .option("multiLine", "true")
            .option("ignoreLeadingWhiteSpace", "false")
            .option("ignoreTrailingWhiteSpace", "false")
        )
        for key, value in source.read_options.items():
            reader = reader.option(key, value)
        return reader.csv(path)

    if source.file_format == "json":
        reader = (
            # Every JSON file here is a pretty printed top level array.
            spark.read.option("multiLine", "true").option("mode", "PERMISSIVE")
        )
        for key, value in source.read_options.items():
            reader = reader.option(key, value)
        return reader.json(path)

    raise ValueError(
        f"Unsupported format '{source.file_format}' for {source.qualified_name}"
    )


def read_bronze(
    spark: "SparkSession", source: config.SourceEntity, ingest_date: str | None = None
) -> "DataFrame":
    """Read one entity from bronze.

    Reading the entity root rather than a single partition lets Spark expose
    ``ingest_date`` as a column, which silver uses to select the latest batch.
    """
    root = f"{config.lake_root('bronze')}/{source.source_system}/{source.entity}"
    if ingest_date:
        return spark.read.option("basePath", root).parquet(f"{root}/ingest_date={ingest_date}")
    return spark.read.parquet(root)


def read_silver(spark: "SparkSession", entity: str) -> "DataFrame":
    """Read one conformed entity from silver."""
    return spark.read.parquet(config.silver_path(entity))


def read_gold(spark: "SparkSession", dataset: str) -> "DataFrame":
    """Read one curated dataset from gold."""
    return spark.read.parquet(config.gold_path(dataset))


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def write_parquet(
    df: "DataFrame",
    path: str,
    *,
    mode: str = "overwrite",
    partition_by: Iterable[str] | None = None,
    coalesce: int | None = None,
) -> None:
    """Write a dataframe to the lake.

    Overwrite is the default at every layer and is what makes reruns idempotent:
    a second run of the same batch replaces its own output rather than appending
    duplicates.
    """
    frame = df.coalesce(coalesce) if coalesce else df
    writer = frame.write.mode(mode).format(config.WRITE_FORMAT)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    retry_call(lambda: writer.save(path), description=f"write {path}")
    _LOGGER.info("Wrote dataset", extra={"path": path, "mode": mode})


def write_quarantine(
    df: "DataFrame",
    *,
    layer: str,
    entity: str,
) -> int:
    """Persist rejected records to the dead letter path.

    Invalid rows are never dropped silently: they land here with the reason
    attached so they can be inspected, corrected and replayed.
    """
    count = df.count()
    if count == 0:
        return 0
    path = config.quarantine_path(layer, entity)
    write_parquet(df, path, mode="overwrite", coalesce=1)
    _LOGGER.warning(
        "Records quarantined",
        extra={"layer": layer, "entity": entity, "rows": count, "path": path},
    )
    return count


def count_written(spark: "SparkSession", path: str) -> int:
    """Read a written dataset back and count it.

    Counting the persisted output rather than the in-memory dataframe is what
    makes the reconciliation figures real evidence that the write succeeded.
    """
    try:
        return spark.read.format(config.WRITE_FORMAT).load(path).count()
    except Exception as exc:
        _LOGGER.error("Could not verify written output", extra={"path": path, "error": str(exc)})
        return 0


def describe_dataframe(df: "DataFrame", limit: int = 5) -> dict[str, Any]:
    """Return a small, log-safe description of a dataframe's shape."""
    return {
        "columns": len(df.columns),
        "column_names": df.columns[:limit],
        "partitions": df.rdd.getNumPartitions(),
    }