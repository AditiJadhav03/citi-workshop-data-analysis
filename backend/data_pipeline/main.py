"""Entry point for the ACME team-analytics batch pipeline.

Runs the medallion stages in order (ingestion -> silver -> gold), threads a
single ``batch_id`` through all of them, collects a lineage record for every
entity processed, and exits non-zero if anything failed or failed to reconcile.

Usage:
    python -m data_pipeline.main
    python -m data_pipeline.main --stages silver gold
    python -m data_pipeline.main --entities teams team_membership
    python -m data_pipeline.main --ingest-date 2026-07-01 --log-format text
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

# Stage names match the medallion layer names they produce, so the CLI, the
# logs and the lineage manifest all speak one vocabulary.
STAGE_ORDER: tuple[str, ...] = ("bronze", "silver", "gold")

#: Stage name -> module path. Imported lazily so a stage that is still being
#: written cannot break the stages that are finished.
STAGE_MODULES: dict[str, str] = {
    "bronze": "data_pipeline.ingestion.ingestion",
    "silver": "data_pipeline.silver.bronze_to_silver",
    "gold": "data_pipeline.gold.silver_to_gold",
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Define the command line interface."""
    parser = argparse.ArgumentParser(
        prog="data_pipeline",
        description="ACME team analytics medallion pipeline.",
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=STAGE_ORDER,
        default=list(STAGE_ORDER),
        help="Stages to run, always executed in medallion order.",
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        default=None,
        help="Restrict processing to these source entities.",
    )
    parser.add_argument("--batch-id", default=None, help="Reuse an existing batch id.")
    parser.add_argument(
        "--ingest-date", default=None, help="Partition date to write, as YYYY-MM-DD."
    )
    parser.add_argument("--engine", choices=("spark", "pandas"), default=None)
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO, WARNING, ERROR.")
    parser.add_argument("--log-format", choices=("json", "text"), default=None)
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Keep going after a stage fails instead of stopping.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and validate, but write nothing to the lake.",
    )
    return parser


def apply_cli_overrides(args: argparse.Namespace) -> None:
    """Push CLI values into the environment before config is imported.

    ``config`` resolves its settings at import time, so overrides have to be in
    place first. This is the only module permitted to write to os.environ.
    """
    overrides = {
        "BATCH_ID": args.batch_id,
        "INGEST_DATE": args.ingest_date,
        "ENGINE": args.engine,
        "LOG_LEVEL": args.log_level,
        "LOG_FORMAT": args.log_format,
    }
    for key, value in overrides.items():
        if value:
            os.environ[key] = str(value)


# ---------------------------------------------------------------------------
# Lineage manifest
# ---------------------------------------------------------------------------


def write_manifest(results: Sequence[Any], config: Any, logger: Any) -> str | None:
    """Persist one lineage record per stage-entity execution.

    The manifest is the auditable trail required by the lineage feature and
    doubles as the evidence for reconciliation testing. It is append only:
    each run writes its own file keyed by batch id.
    """
    if not results:
        return None

    rows = [r.to_manifest_row() for r in results]
    target = f"{config.lineage_path()}/batch_id={config.BATCH_ID}"

    try:
        if config.IS_LOCAL:
            directory = Path(target)
            directory.mkdir(parents=True, exist_ok=True)
            manifest_file = directory / "manifest.json"
            manifest_file.write_text(json.dumps(rows, indent=2, default=str))
            logger.info("Lineage manifest written", extra={"path": str(manifest_file)})
            return str(manifest_file)

        import pandas as pd  # local import: only needed on the cloud path

        pd.DataFrame(rows).to_parquet(f"{target}/manifest.parquet", index=False)
        logger.info("Lineage manifest written", extra={"path": target})
        return target
    except Exception as exc:  # a manifest failure must not lose the run result
        logger.error(
            "Failed to write lineage manifest",
            extra={"error": str(exc), "path": target},
        )
        return None


def print_summary(results: Sequence[Any], logger: Any) -> None:
    """Emit a per-entity reconciliation summary at the end of the run."""
    if not results:
        logger.warning("No stages produced results")
        return

    header = (
        f"{'LAYER':<10} {'ENTITY':<24} {'READ':>9} {'VALID':>9} "
        f"{'QUAR':>8} {'WRITTEN':>9} {'SECS':>7}  {'RECON':<6} STATUS"
    )
    logger.info("Run summary\n" + header + "\n" + "-" * len(header))
    for r in results:
        logger.info(
            f"{r.layer:<10} {r.entity:<24} {r.rows_read:>9,} {r.rows_valid:>9,} "
            f"{r.rows_quarantined:>8,} {r.rows_written:>9,} {r.duration_s:>7.1f}  "
            f"{'OK' if r.reconciles else 'MISMATCH':<6} {r.status}"
            + (f"  :: {r.error}" if r.error else "")
        )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def load_stage(stage: str) -> Callable[[Any], Sequence[Any]]:
    """Import a stage module and return its ``run`` callable.

    Every stage module must expose ``run(ctx) -> Sequence[StageResult]``. That
    uniform signature is what lets this orchestrator stay free of per-stage
    special cases.
    """
    import importlib

    module = importlib.import_module(STAGE_MODULES[stage])
    runner = getattr(module, "run", None)
    if runner is None or not callable(runner):
        raise AttributeError(
            f"Stage module '{STAGE_MODULES[stage]}' must define run(ctx) -> list[StageResult]"
        )
    return runner


def main(argv: Sequence[str] | None = None) -> int:
    """Run the pipeline. Returns the process exit code."""
    args = build_parser().parse_args(argv)
    apply_cli_overrides(args)

    # Imported after the overrides so config resolves the intended values.
    from . import config
    from .pipeline_context import PipelineContext, StageResult
    from .utils.logger import configure_logging, get_logger

    configure_logging()
    logger = get_logger("data_pipeline.main")
    started = datetime.now(timezone.utc)

    try:
        config.validate_config()
    except RuntimeError as exc:
        logger.error("Configuration invalid", extra={"error": str(exc)})
        return 2

    config.ensure_local_dirs()
    logger.info("Pipeline starting", extra=config.describe())
    if args.dry_run:
        logger.warning("Dry run: no data will be written")

    ordered_stages = [s for s in STAGE_ORDER if s in set(args.stages)]
    ctx = PipelineContext.create(
        entities=tuple(args.entities) if args.entities else None,
        dry_run=args.dry_run,
    )

    exit_code = 0
    try:
        for stage in ordered_stages:
            logger.info("Stage starting", extra={"stage": stage})
            try:
                runner = load_stage(stage)
                for result in runner(ctx) or []:
                    ctx.record(result)
            except ModuleNotFoundError as exc:
                logger.error(
                    "Stage module not found, skipping",
                    extra={"stage": stage, "error": str(exc)},
                )
                ctx.record(StageResult(layer=stage, entity="*").finish("SKIPPED", str(exc)))
                exit_code = 1
                if not args.continue_on_error:
                    break
            except Exception as exc:
                logger.exception("Stage failed", extra={"stage": stage})
                ctx.record(StageResult(layer=stage, entity="*").finish("FAILED", str(exc)))
                exit_code = 1
                if not args.continue_on_error:
                    break
            else:
                logger.info("Stage finished", extra={"stage": stage})

        results = ctx.results

        # Quality gates defined in config, applied uniformly to every entity.
        entity_tolerances: dict[str, float] = {}
        try:
            from .validation.schemas import SCHEMAS

            entity_tolerances = {
                name: s.max_quarantine_ratio
                for name, s in SCHEMAS.items()
                if s.max_quarantine_ratio is not None
            }
        except Exception:  # schemas are optional for stages that do not validate
            pass

        for r in results:
            # A stage that failed outright must fail the run. Checking only
            # the row-count gates misses this, because a failed entity reads
            # zero rows and therefore reconciles trivially.
            if r.status in ("FAILED", "SKIPPED"):
                logger.error(
                    "Stage entity did not complete",
                    extra={"entity": r.entity, "layer": r.layer,
                           "status": r.status, "error": r.error},
                )
                exit_code = 1
            threshold = entity_tolerances.get(r.entity, config.MAX_QUARANTINE_RATIO)
            if r.rows_read and r.quarantine_ratio > threshold:
                logger.error(
                    "Quarantine ratio above threshold",
                    extra={
                        "entity": r.entity,
                        "layer": r.layer,
                        "quarantine_ratio": round(r.quarantine_ratio, 4),
                        "threshold": threshold,
                    },
                )
                exit_code = 1
            if not r.reconciles:
                logger.error(
                    "Row counts do not reconcile",
                    extra={
                        "entity": r.entity,
                        "layer": r.layer,
                        "rows_read": r.rows_read,
                        "rows_written": r.rows_written,
                        "rows_quarantined": r.rows_quarantined,
                    },
                )
                exit_code = 1
            if config.FAIL_ON_ZERO_ROWS and r.status == "SUCCESS" and r.rows_written == 0:
                logger.error("Stage wrote zero rows", extra={"entity": r.entity, "layer": r.layer})
                exit_code = 1

        if not args.dry_run:
            write_manifest(results, config, logger)
        print_summary(results, logger)

    finally:
        ctx.stop()

    duration = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(
        "Pipeline finished",
        extra={
            "status": "SUCCESS" if exit_code == 0 else "FAILED",
            "duration_s": round(duration, 2),
            "stages_run": ordered_stages,
            "exit_code": exit_code,
        },
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(main())