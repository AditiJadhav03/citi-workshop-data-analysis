"""Central configuration for the ACME team-analytics data pipeline.

Every path, credential and runtime setting used anywhere in the pipeline is
resolved here. No other module may read ``os.environ`` or hardcode a path.

The module is environment aware: when ``IS_LOCAL`` is true the data lake lives
on the local filesystem under ``data/``; otherwise it lives in S3 under the
bucket named by ``DATA_LAKE_BUCKET``. Nothing else in the codebase needs to know
which mode is active.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

Layer = Literal["bronze", "silver", "gold", "quarantine", "lineage"]


def _env(key: str, default: str | None = None) -> str | None:
    """Return an environment variable, treating blank strings as unset."""
    value = os.environ.get(key, default)
    if value is not None and value.strip() == "":
        return default
    return value


def _env_bool(key: str, default: bool) -> bool:
    """Return an environment variable coerced to a boolean."""
    raw = _env(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(key: str, default: int) -> int:
    """Return an environment variable coerced to an int."""
    raw = _env(key)
    return int(raw) if raw is not None else default


# ---------------------------------------------------------------------------
# Runtime mode and identity
# ---------------------------------------------------------------------------

#: True when running on a developer machine, False when running in AWS.
IS_LOCAL: bool = _env_bool("IS_LOCAL", True)

#: Processing engine. Set ENGINE=pandas to run the same pipeline without Spark.
ENGINE: str = (_env("ENGINE", "spark") or "spark").lower()

#: Identifier for the current pipeline run. Threaded through every layer so any
#: gold row can be traced back to the exact execution that produced it.
BATCH_ID: str = _env("BATCH_ID") or (
    f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
)

#: Logical partition date for this run. Overridable for backfills.
INGEST_DATE: str = _env("INGEST_DATE") or f"{datetime.now(timezone.utc):%Y-%m-%d}"

LOG_LEVEL: str = (_env("LOG_LEVEL", "INFO") or "INFO").upper()
LOG_FORMAT: str = (_env("LOG_FORMAT", "json") or "json").lower()

#: Force every source through one route regardless of its registry setting.
#: Set to "file" to run the whole pipeline without a database, which is useful
#: on a first local run before PostgreSQL is up. Leave unset in normal use so
#: the architecture's dual-source design is exercised.
INGEST_MODE_OVERRIDE: str | None = _env("INGEST_MODE_OVERRIDE")


# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------

# config.py -> data_pipeline -> backend -> <repo root>
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

DATA_DIR: Path = Path(_env("DATA_DIR") or (PROJECT_ROOT / "data"))

#: Immutable drop zone for the supplied source archive. This is pipeline INPUT,
#: never pipeline output, and is not part of the medallion architecture.
RAW_DIR: Path = Path(_env("RAW_DIR") or (DATA_DIR / "raw"))

#: S3 settings, used only when IS_LOCAL is False.
DATA_LAKE_BUCKET: str | None = _env("DATA_LAKE_BUCKET")
DATA_LAKE_PREFIX: str = _env("DATA_LAKE_PREFIX", "data-lake") or "data-lake"
AWS_REGION: str = _env("AWS_REGION", "us-east-1") or "us-east-1"

#: Override the S3 endpoint to target an emulator such as LocalStack.
#: Leaving this unset targets real AWS S3.
S3_ENDPOINT: str | None = _env("S3_ENDPOINT")

#: URI scheme for S3 access. s3a:// for Spark/Hadoop, s3:// for pandas+s3fs.
_S3_SCHEME: str = "s3a" if ENGINE == "spark" else "s3"


def lake_root(layer: Layer) -> str:
    """Return the base URI or path for a data lake layer.

    Local mode returns a filesystem path (as a string); cloud mode returns an
    S3 URI. Callers never branch on IS_LOCAL themselves.
    """
    if IS_LOCAL:
        return str(DATA_DIR / layer)
    if not DATA_LAKE_BUCKET:
        raise RuntimeError(
            "DATA_LAKE_BUCKET must be set when IS_LOCAL is false. "
            "Export it or run with IS_LOCAL=true."
        )
    return f"{_S3_SCHEME}://{DATA_LAKE_BUCKET}/{DATA_LAKE_PREFIX}/{layer}"


def _join(base: str, *parts: str) -> str:
    """Join URI or path segments with forward slashes."""
    cleaned = [str(p).strip("/") for p in parts if str(p) != ""]
    if not cleaned:
        return base
    return f"{base.rstrip('/')}/{'/'.join(cleaned)}"


def bronze_path(source_system: str, entity: str, ingest_date: str | None = None) -> str:
    """Return the partitioned bronze path for one entity of one source system.

    Bronze is partitioned by ingest date so a rerun overwrites exactly one
    partition, which is what makes ingestion idempotent.
    """
    return _join(
        lake_root("bronze"),
        source_system,
        entity,
        f"ingest_date={ingest_date or INGEST_DATE}",
    )


def silver_path(entity: str) -> str:
    """Return the silver path for a conformed entity."""
    return _join(lake_root("silver"), entity)


def gold_path(dataset: str) -> str:
    """Return the gold path for a curated dataset."""
    return _join(lake_root("gold"), dataset)


def quarantine_path(layer: str, entity: str, batch_id: str | None = None) -> str:
    """Return the dead-letter path for records rejected by validation."""
    return _join(
        lake_root("quarantine"),
        layer,
        entity,
        f"batch_id={batch_id or BATCH_ID}",
    )


def lineage_path() -> str:
    """Return the path of the run manifest that records every stage execution."""
    return lake_root("lineage")


def ensure_local_dirs() -> None:
    """Create local lake directories when running in local mode."""
    if not IS_LOCAL:
        return
    for layer in ("bronze", "silver", "gold", "quarantine", "lineage"):
        Path(lake_root(layer)).mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for PostgreSQL, resolved from the environment."""

    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str

    @property
    def jdbc_url(self) -> str:
        """JDBC URL for Spark's built-in PostgreSQL reader/writer."""
        return (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.sslmode}"
        )

    @property
    def jdbc_properties(self) -> dict[str, str]:
        """Connection properties dict expected by Spark's JDBC API."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }

    @property
    def sqlalchemy_url(self) -> str:
        """SQLAlchemy URL for pandas / psycopg based access."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}?sslmode={self.sslmode}"
        )

    def masked(self) -> dict[str, Any]:
        """Return the config with the password redacted, safe for logging."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": "***",
            "sslmode": self.sslmode,
        }


def get_postgres_config() -> PostgresConfig:
    """Build the PostgreSQL config from the workshop-injected env variables.

    Aurora requires TLS; local containers generally do not, so sslmode is
    derived from IS_LOCAL unless explicitly overridden.
    """
    return PostgresConfig(
        host=_env("POSTGRES_HOST", "localhost") or "localhost",
        port=_env_int("POSTGRES_PORT", 5432),
        database=_env("POSTGRES_NAME", "postgres") or "postgres",
        user=_env("POSTGRES_USER", "postgres") or "postgres",
        password=_env("POSTGRES_PASS", "postgres") or "postgres",
        sslmode=_env("POSTGRES_SSLMODE") or ("disable" if IS_LOCAL else "require"),
    )


POSTGRES: PostgresConfig = get_postgres_config()

#: Schema used for the pipeline's own tables (staging + lineage manifest).
POSTGRES_SCHEMA: str = _env("POSTGRES_SCHEMA", "acme") or "acme"
LINEAGE_TABLE: str = f"{POSTGRES_SCHEMA}.pipeline_run_manifest"


# ---------------------------------------------------------------------------
# Source system registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceEntity:
    """Declarative description of one entity in one source system.

    Holding this as data rather than as branching logic inside the ingestion
    job is what keeps ingestion a single loop with no per-file special cases.
    """

    source_system: str
    entity: str
    file_name: str
    file_format: Literal["csv", "json"]
    ingest_mode: Literal["postgres", "file"]
    business_key: tuple[str, ...]
    #: Column used for incremental loads, when the entity has one.
    watermark_column: str | None = None
    #: Extra reader options merged over the format defaults.
    read_options: dict[str, str] = field(default_factory=dict)

    @property
    def qualified_name(self) -> str:
        """Stable identifier such as ``project_tracking.teams``."""
        return f"{self.source_system}.{self.entity}"

    @property
    def effective_ingest_mode(self) -> str:
        """The route actually used, after any global override is applied."""
        return INGEST_MODE_OVERRIDE or self.ingest_mode

    @property
    def raw_file(self) -> Path:
        """Location of this entity's file in the raw drop zone."""
        return RAW_DIR / self.source_system / self.file_name

    @property
    def postgres_table(self) -> str:
        """Table name used when this entity is staged in PostgreSQL."""
        return f"{POSTGRES_SCHEMA}.{self.entity}"


#: Entities routed through PostgreSQL are the systems of record for people and
#: places; entities routed through files are the operational exports. Together
#: they satisfy the architecture's requirement for both source types.
SOURCES: tuple[SourceEntity, ...] = (
    SourceEntity(
        source_system="employee_directory",
        entity="employees",
        file_name="employees.csv",
        file_format="csv",
        ingest_mode="postgres",
        business_key=("emp_id",),
        watermark_column="hire_date",
    ),
    SourceEntity(
        source_system="vendor_management",
        entity="contractor_roster",
        file_name="contractor_roster.csv",
        file_format="csv",
        ingest_mode="postgres",
        business_key=("contractor_id",),
        watermark_column="start_date",
    ),
    SourceEntity(
        source_system="facilities",
        entity="locations",
        file_name="locations.csv",
        file_format="csv",
        ingest_mode="postgres",
        business_key=("location_code",),
    ),
    SourceEntity(
        source_system="org_structure",
        entity="organizations",
        file_name="organizations.json",
        file_format="json",
        ingest_mode="file",
        business_key=("org_id",),
    ),
    SourceEntity(
        source_system="project_tracking",
        entity="teams",
        file_name="teams.json",
        file_format="json",
        ingest_mode="file",
        business_key=("team_id",),
        watermark_column="formed_date",
    ),
    SourceEntity(
        source_system="project_tracking",
        entity="team_membership",
        file_name="team_membership.csv",
        file_format="csv",
        ingest_mode="file",
        business_key=("team_code", "employee_email", "start_date"),
        watermark_column="start_date",
    ),
    SourceEntity(
        source_system="performance_management",
        entity="monthly_achievements",
        file_name="monthly_achievements.json",
        file_format="json",
        ingest_mode="file",
        # No natural key in the source; a deterministic surrogate hash is built
        # in the silver layer from the full record.
        business_key=("achievement_sk",),
        watermark_column="month",
    ),
)

SOURCES_BY_ENTITY: dict[str, SourceEntity] = {s.entity: s for s in SOURCES}


def get_source(entity: str) -> SourceEntity:
    """Look up a registered source entity by name."""
    try:
        return SOURCES_BY_ENTITY[entity]
    except KeyError as exc:
        known = ", ".join(sorted(SOURCES_BY_ENTITY))
        raise KeyError(f"Unknown source entity '{entity}'. Known: {known}") from exc


#: Curated datasets produced by the gold layer, mapped to the business
#: questions they answer. Used by the gold job and by the notebook.
GOLD_DATASETS: dict[str, str] = {
    "team_members": "Q1 - Who are the members of each team?",
    "team_locations": "Q2 - Where are the teams located?",
    "monthly_team_achievements": "Q3 - Monthly achievements per team",
    "leader_not_colocated": "Q4 - Teams whose leader is not co-located",
    "leader_non_direct_staff": "Q5 - Teams whose leader is non-direct staff",
    "staff_ratio_analysis": "Q6 - Teams with non-direct staff ratio above 20%",
    "organization_reporting_summary": "Q7 - Teams reporting to an org leader",
    "employee_summary": "Supporting - workforce composition",
    "business_answers": "Headline answer to each of the seven questions",
}


# ---------------------------------------------------------------------------
# Data quality thresholds and business rules
# ---------------------------------------------------------------------------

#: Threshold for question 6.
NON_DIRECT_STAFF_RATIO_THRESHOLD: float = float(
    _env("NON_DIRECT_STAFF_RATIO_THRESHOLD", "0.20") or "0.20"
)

#: Canonical corporate email domain. Near-miss domains in the source data are
#: repaired against this value during silver processing.
CANONICAL_EMAIL_DOMAIN: str = _env("CANONICAL_EMAIL_DOMAIN", "acme-inc.com") or "acme-inc.com"
KNOWN_DOMAIN_TYPOS: tuple[str, ...] = ("acmeinc.com", "acme_inc.com", "acme-inc.co")

#: Date formats observed across the source systems, in the order they are tried.
DATE_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%d-%b-%Y",
    "%B %d, %Y",
    "%d %B %Y",
)

#: The same formats in Spark's pattern syntax, used by the silver date parser.
#: Order matters: the first pattern that parses a value wins, so unambiguous
#: ISO-style patterns are tried before the ambiguous US-style ones.
SPARK_DATE_FORMATS: tuple[str, ...] = (
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "MM/dd/yyyy",
    "dd-MMM-yyyy",
    "MMMM d, yyyy",
    "d MMMM yyyy",
)

#: Month formats found in the achievements feed.
SPARK_MONTH_FORMATS: tuple[str, ...] = ("yyyy-MM", "yyyy/MM", "MMMM yyyy")

#: Categorical impact bands used where impact_score is not numeric.
IMPACT_BAND_SCORES: dict[str, float] = {"low": 2.5, "medium": 5.0, "high": 8.0}

#: Fail the run if more than this share of an entity's rows are quarantined.
MAX_QUARANTINE_RATIO: float = float(_env("MAX_QUARANTINE_RATIO", "0.25") or "0.25")

#: Abort when a stage writes zero rows, per the error-handling matrix.
FAIL_ON_ZERO_ROWS: bool = _env_bool("FAIL_ON_ZERO_ROWS", True)

#: Retry policy for transient PostgreSQL and S3 failures.
RETRY_ATTEMPTS: int = _env_int("RETRY_ATTEMPTS", 3)
RETRY_BASE_DELAY_SECONDS: float = float(_env("RETRY_BASE_DELAY_SECONDS", "1.0") or "1.0")

#: Written output format for every lake layer.
WRITE_FORMAT: str = _env("WRITE_FORMAT", "parquet") or "parquet"


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

SPARK_APP_NAME: str = _env("SPARK_APP_NAME", "ACME-Team-Analytics") or "ACME-Team-Analytics"
SPARK_MASTER: str | None = _env("SPARK_MASTER", "local[*]" if IS_LOCAL else None)
SPARK_SHUFFLE_PARTITIONS: int = _env_int("SPARK_SHUFFLE_PARTITIONS", 8 if IS_LOCAL else 64)

# multiLine JSON cannot be split across partitions, so the 41 MB achievements
# file is parsed by a single task. Give the driver room for it.
SPARK_DRIVER_MEMORY: str = _env("SPARK_DRIVER_MEMORY", "4g") or "4g"

POSTGRES_JDBC_PACKAGE: str = (
    _env("POSTGRES_JDBC_PACKAGE", "org.postgresql:postgresql:42.7.3")
    or "org.postgresql:postgresql:42.7.3"
)
# Must match the Hadoop version Spark was built against. Spark 4.x ships with
# Hadoop 3.4.x; a mismatch produces NoSuchMethodError at runtime, not at import.
# Verify with: python -c "import pyspark; print(pyspark.__version__)" and check
# the hadoop-client jars under $SPARK_HOME/jars.
HADOOP_AWS_PACKAGE: str = (
    _env("HADOOP_AWS_PACKAGE", "org.apache.hadoop:hadoop-aws:3.4.1")
    or "org.apache.hadoop:hadoop-aws:3.4.1"
)


def spark_packages() -> str:
    """Return the comma separated Maven coordinates required by this run."""
    packages = [POSTGRES_JDBC_PACKAGE]
    if not IS_LOCAL:
        packages.append(HADOOP_AWS_PACKAGE)
    return ",".join(packages)


def spark_conf() -> dict[str, str]:
    """Return Spark settings appropriate to the current runtime mode."""
    conf: dict[str, str] = {
        "spark.jars.packages": spark_packages(),
        "spark.sql.shuffle.partitions": str(SPARK_SHUFFLE_PARTITIONS),
        "spark.sql.session.timeZone": "UTC",
        # Overwrite only the partitions being written, which is what makes
        # bronze ingestion safely repeatable.
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": "snappy",
        # Strict date parsing: an unparseable date yields null rather than being
        # silently coerced, which is what lets validation detect and quarantine it.
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        # Spark 4 enables ANSI mode by default, which raises on a failed cast and
        # aborts the whole job. This pipeline deliberately relies on the opposite
        # behaviour: a bad value becomes null, validation flags the null, and the
        # offending row is routed to quarantine while good rows keep flowing.
        "spark.sql.ansi.enabled": "false",
        "spark.sql.adaptive.enabled": "true",
    }
    if IS_LOCAL:
        conf["spark.driver.memory"] = SPARK_DRIVER_MEMORY
    if not IS_LOCAL:
        conf.update(
            {
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                    if S3_ENDPOINT
                    else "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
                ),
                "spark.hadoop.fs.s3a.endpoint": (
                    S3_ENDPOINT or f"s3.{AWS_REGION}.amazonaws.com"
                ),
                # Emulators serve buckets as a path segment rather than as a
                # DNS subdomain, so virtual-host addressing has to be disabled.
                "spark.hadoop.fs.s3a.path.style.access": "true" if S3_ENDPOINT else "false",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false" if S3_ENDPOINT else "true",
            }
        )
    return conf


# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------


def validate_config() -> None:
    """Fail fast on a misconfigured environment, before any work is attempted.

    Raises:
        RuntimeError: if required settings for the active mode are missing.
    """
    problems: list[str] = []

    if not IS_LOCAL and not DATA_LAKE_BUCKET:
        problems.append("DATA_LAKE_BUCKET is required when IS_LOCAL is false")

    if not IS_LOCAL:
        for var in ("POSTGRES_HOST", "POSTGRES_NAME", "POSTGRES_USER", "POSTGRES_PASS"):
            if _env(var) is None:
                problems.append(f"{var} is required when IS_LOCAL is false")

    if ENGINE not in {"spark", "pandas"}:
        problems.append(f"ENGINE must be 'spark' or 'pandas', got '{ENGINE}'")

    if not 0 < NON_DIRECT_STAFF_RATIO_THRESHOLD < 1:
        problems.append("NON_DIRECT_STAFF_RATIO_THRESHOLD must be between 0 and 1")

    if IS_LOCAL and not RAW_DIR.exists():
        problems.append(f"Raw data directory not found: {RAW_DIR}")

    if problems:
        raise RuntimeError("Invalid configuration:\n  - " + "\n  - ".join(problems))


def describe() -> dict[str, Any]:
    """Return a log-safe summary of the effective configuration."""
    return {
        "batch_id": BATCH_ID,
        "ingest_date": INGEST_DATE,
        "is_local": IS_LOCAL,
        "engine": ENGINE,
        "project_root": str(PROJECT_ROOT),
        "raw_dir": str(RAW_DIR),
        "bronze_root": lake_root("bronze"),
        "silver_root": lake_root("silver"),
        "gold_root": lake_root("gold"),
        "quarantine_root": lake_root("quarantine"),
        "postgres": POSTGRES.masked(),
        "source_entities": [s.qualified_name for s in SOURCES],
    }