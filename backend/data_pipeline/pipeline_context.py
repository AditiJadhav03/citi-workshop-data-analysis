"""Shared contract between the orchestrator and the pipeline stages.

``PipelineContext`` carries the run identity, the engine handle and the logger
into every stage, so no stage ever creates its own Spark session or invents its
own batch id. ``StageResult`` is the record each stage returns, and is the
single source of truth for the lineage manifest and the reconciliation tests.

Keeping these in a standalone module avoids a circular import between
``main.py`` and the stage modules that need to type hint the context.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from . import config
from .utils.logger import get_logger

StageStatus = Literal["SUCCESS", "PARTIAL", "FAILED", "SKIPPED"]


@dataclass
class StageResult:
    """Outcome of processing one entity within one layer.

    The row counters are the reconciliation evidence required by the testing
    guide: ``rows_written + rows_quarantined`` must account for ``rows_read``
    at every hop, and any shortfall must be explainable.
    """

    layer: str
    entity: str
    source_path: str = ""
    target_path: str = ""
    rows_read: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0
    rows_written: int = 0
    status: StageStatus = "SUCCESS"
    error: str | None = None
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    ended_at: str | None = None
    duration_s: float = 0.0
    #: True when the stage aggregates its input, so a reduction in row
    #: count is the intended behaviour rather than evidence of data loss.
    is_aggregate: bool = False
    notes: dict[str, Any] = field(default_factory=dict)

    @property
    def quarantine_ratio(self) -> float:
        """Share of input rows that failed validation."""
        return self.rows_quarantined / self.rows_read if self.rows_read else 0.0

    @property
    def reconciles(self) -> bool:
        """True when written and quarantined rows account for all input rows."""
        if self.is_aggregate or self.rows_read == 0:
            return True
        return self.rows_written + self.rows_quarantined == self.rows_read

    def finish(self, status: StageStatus = "SUCCESS", error: str | None = None) -> "StageResult":
        """Stamp the end time and duration, then return self for chaining."""
        ended = datetime.now(timezone.utc)
        self.ended_at = ended.isoformat()
        self.duration_s = round(
            (ended - datetime.fromisoformat(self.started_at)).total_seconds(), 3
        )
        self.status = status
        self.error = error
        return self

    def to_manifest_row(self) -> dict[str, Any]:
        """Flatten into a row for the lineage manifest."""
        row = asdict(self)
        row["batch_id"] = config.BATCH_ID
        row["ingest_date"] = config.INGEST_DATE
        row["is_local"] = config.IS_LOCAL
        row["engine"] = config.ENGINE
        row["quarantine_ratio"] = round(self.quarantine_ratio, 6)
        row["reconciles"] = self.reconciles
        row["notes"] = row["notes"] or {}
        return row


@dataclass
class PipelineContext:
    """Everything a stage needs, created once per run by the orchestrator."""

    batch_id: str
    ingest_date: str
    engine: str
    entities: tuple[str, ...] | None = None
    dry_run: bool = False
    spark: Any = None
    _results: list[StageResult] = field(default_factory=list, repr=False)

    # -- construction -------------------------------------------------------

    @classmethod
    def create(
        cls,
        entities: tuple[str, ...] | None = None,
        dry_run: bool = False,
    ) -> "PipelineContext":
        """Build the context and start the processing engine if required."""
        ctx = cls(
            batch_id=config.BATCH_ID,
            ingest_date=config.INGEST_DATE,
            engine=config.ENGINE,
            entities=entities,
            dry_run=dry_run,
        )
        if config.ENGINE == "spark":
            ctx.spark = ctx._build_spark_session()
        return ctx

    def _build_spark_session(self) -> Any:
        """Create the Spark session via the shared factory in utils."""
        from .utils.spark_session import create_spark_session

        return create_spark_session()

    # -- helpers ------------------------------------------------------------

    def logger(self, name: str):
        """Return a logger bound to the calling module."""
        return get_logger(name)

    def selected_sources(self) -> tuple[config.SourceEntity, ...]:
        """Return the source entities in scope for this run.

        Honours the ``--entities`` filter so a single table can be reprocessed
        without rerunning the whole pipeline.
        """
        if not self.entities:
            return config.SOURCES
        wanted = set(self.entities)
        unknown = wanted - {s.entity for s in config.SOURCES}
        if unknown:
            raise KeyError(f"Unknown entities requested: {sorted(unknown)}")
        return tuple(s for s in config.SOURCES if s.entity in wanted)

    def record(self, result: StageResult) -> StageResult:
        """Register a stage result for the lineage manifest."""
        self._results.append(result)
        return result

    @property
    def results(self) -> list[StageResult]:
        """All stage results collected so far in this run."""
        return list(self._results)

    def stop(self) -> None:
        """Release engine resources."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None