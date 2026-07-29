"""Bronze layer: ingest source systems into the raw zone of the data lake.

Bronze is a faithful, replayable copy of what the source systems produced. Three
rules govern everything in this module:

1. **No type inference.** Every column lands as a string. A value that cannot be
   parsed must survive to silver so that validation can quarantine it with a
   reason; a value nulled out by a schema guess is lost before anyone sees it.
2. **No cleaning.** Trimming, casing, date parsing and deduplication all belong
   to silver. Bronze changes nothing about the payload.
3. **Immutable and partitioned.** Output is written under ``ingest_date=...``,
   so rerunning a date replaces exactly that partition and leaves history alone.

Sources arrive by two routes, matching the target architecture: the people and
places entities are read from PostgreSQL over JDBC, and the operational exports
are read as files from the raw drop zone (local) or S3 (cloud).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from .. import config
from ..database import postgres_loader
from ..pipeline_context import PipelineContext, StageResult
from ..utils.io import (
    add_lineage_columns,
    count_written,
    read_source_file,
    stringify_all_columns,
    write_parquet,
)
from ..utils.logger import get_logger, log_duration

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_LOGGER = get_logger(__name__)


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def _read_from_postgres(ctx: PipelineContext, source: config.SourceEntity) -> tuple["DataFrame", str]:
    """Read one entity from PostgreSQL. Returns the frame and its source path."""
    frame = postgres_loader.read_table(ctx.spark, source.postgres_table)
    return frame, f"postgresql://{config.POSTGRES.host}/{source.postgres_table}"


def _read_from_file(ctx: PipelineContext, source: config.SourceEntity) -> tuple["DataFrame", str]:
    """Read one entity from the raw drop zone."""
    if not source.raw_file.exists():
        raise FileNotFoundError(f"Raw export not found: {source.raw_file}")
    return read_source_file(ctx.spark, source), str(source.raw_file)


_READERS = {
    "postgres": _read_from_postgres,
    "file": _read_from_file,
}


# ---------------------------------------------------------------------------
# Ingestion of a single entity
# ---------------------------------------------------------------------------


def ingest_source(ctx: PipelineContext, source: config.SourceEntity) -> StageResult:
    """Ingest one registered source entity into bronze.

    Failures are contained to this entity: the result is marked FAILED, the
    exception is logged with context, and the orchestrator continues with the
    remaining sources rather than losing the whole run to one bad file.
    """
    result = StageResult(layer="bronze", entity=source.entity)
    target = config.bronze_path(source.source_system, source.entity, ctx.ingest_date)
    result.target_path = target

    try:
        with log_duration(
            _LOGGER,
            "bronze ingest",
            entity=source.qualified_name,
            ingest_mode=source.effective_ingest_mode,
        ) as span:
            reader = _READERS[source.effective_ingest_mode]
            raw, source_path = reader(ctx, source)
            result.source_path = source_path

            # Everything to string, nested structures serialised as JSON.
            frame = stringify_all_columns(raw)

            # Drop lineage columns that may already exist on a reseeded table,
            # so stamping cannot produce duplicates.
            for column in ("_batch_id", "_ingested_at", "_source_system",
                           "_source_entity", "_source_file", "_source_layer"):
                if column in frame.columns:
                    frame = frame.drop(column)

            frame = add_lineage_columns(
                frame,
                source_system=source.source_system,
                entity=source.entity,
                source_path=source_path,
                source_layer=source.effective_ingest_mode,
            ).cache()

            result.rows_read = frame.count()
            result.rows_valid = result.rows_read
            span["rows_read"] = result.rows_read
            span["columns"] = len(frame.columns)

            if result.rows_read == 0:
                _LOGGER.warning(
                    "Source produced zero rows",
                    extra={"entity": source.qualified_name, "path": source_path},
                )

            if ctx.dry_run:
                _LOGGER.info("Dry run, skipping write", extra={"target": target})
                result.rows_written = result.rows_read
            else:
                # The target path already carries ingest_date=..., so a rerun of
                # the same date overwrites only that partition.
                write_parquet(frame, target, mode="overwrite")
                # Count the persisted output, not the in-memory frame, so the
                # reconciliation figure is genuine evidence the write landed.
                result.rows_written = count_written(ctx.spark, target)
                span["rows_written"] = result.rows_written

            frame.unpersist()
            result.notes = {
                "ingest_mode": source.effective_ingest_mode,
                "file_format": source.file_format,
                "business_key": list(source.business_key),
            }

    except Exception as exc:
        _LOGGER.exception(
            "Bronze ingestion failed",
            extra={"entity": source.qualified_name, "target": target},
        )
        return result.finish("FAILED", str(exc))

    if result.rows_written != result.rows_read:
        return result.finish(
            "PARTIAL",
            f"read {result.rows_read} rows but persisted {result.rows_written}",
        )
    return result.finish("SUCCESS")


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------


def run(ctx: PipelineContext) -> Sequence[StageResult]:
    """Ingest every selected source system into bronze.

    This is the contract the orchestrator calls. It returns one StageResult per
    entity, which become rows in the lineage manifest.
    """
    sources = ctx.selected_sources()
    _LOGGER.info(
        "Bronze stage starting",
        extra={
            "entities": [s.qualified_name for s in sources],
            "ingest_date": ctx.ingest_date,
            "target_root": config.lake_root("bronze"),
        },
    )

    # PostgreSQL only becomes a usable source once the operational exports have
    # been staged into it. This is idempotent and no-ops on every run after the
    # first, so a normal run reads PostgreSQL without writing to it.
    if any(s.effective_ingest_mode == "postgres" for s in sources) and not ctx.dry_run:
        if postgres_loader.check_connection():
            seeded = postgres_loader.ensure_seeded(ctx.spark)
            if any(seeded.values()):
                _LOGGER.info("Seeded PostgreSQL source tables", extra={"rows": seeded})
        else:
            _LOGGER.error(
                "PostgreSQL unavailable; entities routed through it will fail. "
                "Start the local database or export the POSTGRES_* variables."
            )

    results = [ingest_source(ctx, source) for source in sources]

    _LOGGER.info(
        "Bronze stage complete",
        extra={
            "entities": len(results),
            "succeeded": sum(1 for r in results if r.status == "SUCCESS"),
            "failed": sum(1 for r in results if r.status == "FAILED"),
            "rows_written": sum(r.rows_written for r in results),
        },
    )
    return results