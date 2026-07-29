"""Silver layer: conform, type, validate and deduplicate the bronze zone.

This is where the source systems stop disagreeing with each other. Bronze holds
seven exports in seven different shapes, all as strings; silver turns them into
typed, conformed entities that share vocabularies and join cleanly.

Every entity follows the same five steps:

1. Read the latest bronze partition.
2. Normalise: parse dates, repair emails, conform categorical vocabularies.
3. Validate against the entity's declared contract.
4. Deduplicate on the declared business key.
5. Write survivors to silver and everything rejected to quarantine, with reasons.

Order matters. Locations, employees and contractors are processed first because
``dim_person`` is built from them, and teams, memberships and achievements all
depend on ``dim_person`` to answer questions about direct versus non-direct
staff.

Known source defects handled here, each documented at its implementation:
  * six date formats across the exports
  * four spellings of the corporate email domain
  * six spellings of full-time employment, seven of contractor engagement
  * LOC-01 mapped to two different cities
  * duplicate emails in the employee directory
  * one third of achievements arriving with no team_id
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from .. import config
from ..pipeline_context import PipelineContext, StageResult
from ..utils import transforms as T
from ..utils.io import (
    add_lineage_columns,
    count_written,
    read_bronze,
    read_silver,
    write_parquet,
    write_quarantine,
)
from ..utils.logger import get_logger, log_duration
from ..validation import schemas
from ..validation.validator import (
    REJECTION_REASON_COLUMN,
    SchemaDriftError,
    deduplicate,
    mark_rejected,
    validate,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_LOGGER = get_logger(__name__)

#: Silver entity names are business names, which do not always match the source
#: export names. This map is the only place the two vocabularies meet.
SILVER_TO_SOURCE: dict[str, str] = {
    "locations": "locations",
    "organizations": "organizations",
    "employees": "employees",
    "contractors": "contractor_roster",
    "teams": "teams",
    "team_membership": "team_membership",
    "achievements": "monthly_achievements",
}


# ---------------------------------------------------------------------------
# Shared plumbing
# ---------------------------------------------------------------------------


def _latest_bronze(ctx: PipelineContext, entity: str) -> "DataFrame":
    """Read the most recent ingest partition of a bronze entity.

    Bronze retains history, so silver deliberately consumes only the newest
    partition. Reprocessing an older date is possible by passing --ingest-date.
    """
    from pyspark.sql import functions as F

    source = config.get_source(SILVER_TO_SOURCE.get(entity, entity))
    frame = read_bronze(ctx.spark, source)

    if "ingest_date" in frame.columns:
        target = ctx.ingest_date
        available = [
            row[0] for row in frame.select("ingest_date").distinct().collect()
        ]
        if target not in {str(a) for a in available}:
            target = max(str(a) for a in available)
            _LOGGER.warning(
                "Requested ingest_date absent from bronze, using latest",
                extra={"entity": entity, "requested": ctx.ingest_date, "using": target},
            )
        frame = frame.filter(F.col("ingest_date") == target).drop("ingest_date")

    return frame


def _stamp(frame: "DataFrame", entity: str) -> "DataFrame":
    """Refresh lineage columns for the silver layer."""
    from pyspark.sql import functions as F

    source = config.SOURCES_BY_ENTITY.get(SILVER_TO_SOURCE.get(entity, entity))
    system = source.source_system if source else "derived"
    for column in ("_source_layer", "_processed_at", "_source_system", "_source_entity"):
        if column in frame.columns:
            frame = frame.drop(column)
    return (
        frame.withColumn("_source_layer", F.lit("bronze"))
        .withColumn("_source_system", F.lit(system))
        .withColumn("_source_entity", F.lit(entity))
        .withColumn("_processed_at", F.current_timestamp())
    )


def _persist(
    ctx: PipelineContext,
    *,
    entity: str,
    normalized: "DataFrame",
    schema_name: str | None = None,
    order_by: Sequence[str] | None = None,
    source_path: str = "",
    extra_rejects: "DataFrame | None" = None,
    notes: dict | None = None,
) -> StageResult:
    """Validate, deduplicate, write and report on one silver entity.

    Every entity shares this tail so that quarantine behaviour, reconciliation
    counting and lineage reporting are identical everywhere and defined once.
    """
    schema = schemas.get_schema(schema_name or entity)
    result = StageResult(
        layer="silver",
        entity=entity,
        source_path=source_path,
        target_path=config.silver_path(entity),
    )

    try:
        with log_duration(_LOGGER, "silver transform", entity=entity) as span:
            frame = _stamp(normalized, entity).cache()

            outcome = validate(frame, schema)
            result.rows_read = outcome.rows_in
            result.rows_valid = outcome.rows_valid

            kept, duplicates = deduplicate(outcome.valid, schema, order_by=order_by)

            rejected = outcome.invalid
            if duplicates is not None:
                rejected = rejected.unionByName(duplicates, allowMissingColumns=True)
            if extra_rejects is not None:
                rejected = rejected.unionByName(extra_rejects, allowMissingColumns=True)

            if ctx.dry_run:
                result.rows_written = kept.count()
                result.rows_quarantined = rejected.count()
            else:
                write_parquet(kept, config.silver_path(entity), mode="overwrite")
                result.rows_written = count_written(ctx.spark, config.silver_path(entity))
                result.rows_quarantined = write_quarantine(
                    rejected, layer="silver", entity=entity
                )

            span["rows_read"] = result.rows_read
            span["rows_written"] = result.rows_written
            span["rows_quarantined"] = result.rows_quarantined

            result.notes = {
                "rule_failures": outcome.rule_failures,
                "warnings": outcome.warnings,
                "business_key": list(schema.business_key),
                **(notes or {}),
            }
            frame.unpersist()

    except SchemaDriftError as exc:
        _LOGGER.error("Schema drift detected", extra={"entity": entity, "error": str(exc)})
        return result.finish("FAILED", f"schema drift: {exc}")
    except Exception as exc:
        _LOGGER.exception("Silver transform failed", extra={"entity": entity})
        return result.finish("FAILED", str(exc))

    if not result.reconciles:
        return result.finish(
            "PARTIAL",
            f"{result.rows_read} in, {result.rows_written} written, "
            f"{result.rows_quarantined} quarantined",
        )
    return result.finish("SUCCESS")


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------


def process_locations(ctx: PipelineContext) -> StageResult:
    """Conform the facilities reference table.

    The export contains ten rows for nine location codes: LOC-01 appears twice,
    mapped to both Austin and Dallas. A dimension key that resolves to two
    values would fan out every join that touches it, so the collision is broken
    deterministically by keeping the alphabetically first city and recording the
    discarded row in quarantine rather than dropping it.
    """
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "locations")
    normalized = raw.select(
        T.clean_string(F.col("location_code")).alias("location_code"),
        T.clean_string(F.col("city")).alias("city"),
        T.clean_string(F.col("country")).alias("country"),
        F.upper(T.clean_string(F.col("region"))).alias("region"),
        T.clean_string(F.col("timezone")).alias("timezone"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    )

    return _persist(
        ctx,
        entity="locations",
        normalized=normalized,
        order_by=("city",),  # deterministic winner for the LOC-01 collision
        source_path=config.bronze_path("facilities", "locations", ctx.ingest_date),
        notes={"known_defect": "LOC-01 maps to both Austin and Dallas; first by city wins"},
    )


def process_organizations(ctx: PipelineContext) -> StageResult:
    """Conform the organisation reference table."""
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "organizations")
    normalized = raw.select(
        T.clean_string(F.col("org_id")).alias("org_id"),
        T.clean_string(F.col("org_name")).alias("org_name"),
        T.normalize_email(F.col("org_leader_email")).alias("org_leader_email"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    )

    return _persist(
        ctx,
        entity="organizations",
        normalized=normalized,
        source_path=config.bronze_path("org_structure", "organizations", ctx.ingest_date),
    )


# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------


def process_employees(ctx: PipelineContext) -> StageResult:
    """Conform the employee directory.

    Emails arrive under four spellings of the corporate domain and are repaired
    to the canonical one; employment type arrives under six spellings of
    full-time and is folded onto a single token. Both matter because email is
    the join key to memberships and employment type distinguishes direct staff.
    """
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "employees")
    normalized = raw.select(
        T.clean_string(F.col("emp_id")).alias("emp_id"),
        T.clean_string(F.col("full_name")).alias("full_name"),
        T.normalize_email(F.col("email")).alias("email"),
        T.clean_string(F.col("department")).alias("department"),
        T.clean_string(F.col("location_code")).alias("location_code"),
        T.normalize_employment_type(F.col("employment_type")).alias("employment_type"),
        T.clean_string(F.col("manager_emp_id")).alias("manager_emp_id"),
        T.parse_date(F.col("hire_date")).alias("hire_date"),
        T.normalize_status(F.col("status")).alias("status"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    )

    return _persist(
        ctx,
        entity="employees",
        normalized=normalized,
        # Prefer the active record when one emp_id appears more than once.
        order_by=("status", "hire_date"),
        source_path=config.bronze_path("employee_directory", "employees", ctx.ingest_date),
    )


def process_contractors(ctx: PipelineContext) -> StageResult:
    """Conform the vendor roster into non-direct staff records."""
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "contractor_roster")
    normalized = raw.select(
        T.clean_string(F.col("contractor_id")).alias("contractor_id"),
        T.clean_string(F.col("full_name")).alias("full_name"),
        T.normalize_email(F.col("email")).alias("email"),
        T.clean_string(F.col("agency")).alias("agency"),
        T.clean_string(F.col("location_code")).alias("location_code"),
        T.normalize_engagement_type(F.col("engagement_type")).alias("engagement_type"),
        T.parse_date(F.col("start_date")).alias("start_date"),
        T.parse_date(F.col("end_date")).alias("end_date"),
        T.normalize_status(F.col("status")).alias("status"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    )

    return _persist(
        ctx,
        entity="contractors",
        normalized=normalized,
        order_by=("status", "start_date"),
        source_path=config.bronze_path("vendor_management", "contractor_roster", ctx.ingest_date),
    )


def build_dim_person(ctx: PipelineContext) -> StageResult:
    """Unify employees and contractors into one person dimension.

    This is the single most important derived entity in the pipeline. Five of
    the seven business questions turn on whether a person is direct staff, and
    defining that here once means the gold layer never has to re-derive it.

    Two source problems are resolved:

    * The directory contains roughly 49k duplicate email addresses, while email
      is the only key memberships carry. One record per address is chosen
      deterministically, preferring direct over non-direct and active over
      inactive, so reruns produce identical results.
    * A person appearing in both the directory and the vendor roster is treated
      as direct staff, since an employment record outranks an agency engagement.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    employees = read_silver(ctx.spark, "employees")
    contractors = read_silver(ctx.spark, "contractors")

    direct = employees.select(
        F.col("emp_id").alias("person_id"),
        F.col("email"),
        F.col("full_name"),
        F.col("location_code"),
        F.col("status"),
        F.lit("DIRECT").alias("staff_type"),
        F.lit("employee_directory").alias("person_source"),
        F.col("department").alias("department"),
        F.lit(None).cast("string").alias("agency"),
    )
    non_direct = contractors.select(
        F.col("contractor_id").alias("person_id"),
        F.col("email"),
        F.col("full_name"),
        F.col("location_code"),
        F.col("status"),
        F.lit("NON_DIRECT").alias("staff_type"),
        F.lit("vendor_management").alias("person_source"),
        F.lit(None).cast("string").alias("department"),
        F.col("agency"),
    )

    combined = direct.unionByName(non_direct).filter(F.col("email").isNotNull())

    # DIRECT sorts before NON_DIRECT and ACTIVE before other statuses, so the
    # ordering below encodes the precedence rules described in the docstring.
    window = Window.partitionBy("email").orderBy(
        F.col("staff_type").asc(),
        F.when(F.col("status") == "ACTIVE", 0).otherwise(1).asc(),
        F.col("person_id").asc_nulls_last(),
    )
    ranked = combined.withColumn("_rank", F.row_number().over(window))

    people = ranked.filter(F.col("_rank") == 1).drop("_rank")
    collisions = ranked.filter(F.col("_rank") > 1).transform(
        lambda df: mark_rejected(df, "duplicate_email_lower_precedence")
    )

    result = StageResult(
        layer="silver",
        entity="dim_person",
        source_path=f"{config.silver_path('employees')} + {config.silver_path('contractors')}",
        target_path=config.silver_path("dim_person"),
    )

    try:
        with log_duration(_LOGGER, "silver transform", entity="dim_person") as span:
            stamped = _stamp(people, "dim_person")
            result.rows_read = combined.count()
            result.rows_valid = result.rows_read

            if ctx.dry_run:
                result.rows_written = stamped.count()
                result.rows_quarantined = collisions.count()
            else:
                write_parquet(stamped, config.silver_path("dim_person"), mode="overwrite")
                result.rows_written = count_written(ctx.spark, config.silver_path("dim_person"))
                result.rows_quarantined = write_quarantine(
                    collisions, layer="silver", entity="dim_person"
                )

            span["rows_written"] = result.rows_written
            result.notes = {
                "direct_source": "employees",
                "non_direct_source": "contractors",
                "precedence": "DIRECT > NON_DIRECT, ACTIVE > inactive, then person_id",
            }
    except Exception as exc:
        _LOGGER.exception("dim_person build failed")
        return result.finish("FAILED", str(exc))

    return result.finish("SUCCESS" if result.reconciles else "PARTIAL")


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------


def process_teams(ctx: PipelineContext) -> StageResult:
    """Flatten and conform team master data.

    The source nests organisation as an object. Bronze preserved it as a JSON
    string rather than guessing a struct schema, so it is unpacked here with
    explicit path expressions.
    """
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "teams")

    # The organisation column survives bronze as serialised JSON.
    org_id = T.clean_string(F.get_json_object(F.col("organization"), "$.org_id"))
    org_name = T.clean_string(F.get_json_object(F.col("organization"), "$.org_name"))

    normalized = raw.select(
        T.normalize_team_id(F.col("team_id")).alias("team_id"),
        T.clean_string(F.col("team_name")).alias("team_name"),
        org_id.alias("org_id"),
        org_name.alias("org_name"),
        T.normalize_email(F.col("team_leader_email")).alias("team_leader_email"),
        T.clean_string(F.col("primary_office")).alias("primary_office"),
        T.clean_string(F.col("reports_to_type")).alias("reports_to_type"),
        T.normalize_email(F.col("reporting_manager_email")).alias("reporting_manager_email"),
        T.parse_date(F.col("formed_date")).alias("formed_date"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    )

    return _persist(
        ctx,
        entity="teams",
        normalized=normalized,
        order_by=("formed_date",),
        source_path=config.bronze_path("project_tracking", "teams", ctx.ingest_date),
    )


def process_membership(ctx: PipelineContext) -> StageResult:
    """Conform team rosters.

    Two source defects are handled without losing rows. Emails arrive padded
    with whitespace, which silently orphans them from the person dimension, and
    roughly a quarter of allocations exceed 100 percent. The allocation is
    capped for downstream use while the original is retained in
    ``allocation_pct_raw``, because dropping the membership would remove real
    people from real teams over a data entry error in an unrelated field.
    """
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "team_membership")
    allocation_raw = T.parse_int(F.col("allocation_pct"))

    normalized = raw.select(
        T.normalize_team_id(F.col("team_code")).alias("team_id"),
        T.normalize_email(F.col("employee_email")).alias("employee_email"),
        T.normalize_role(F.col("role")).alias("role"),
        allocation_raw.alias("allocation_pct_raw"),
        F.when(allocation_raw > 100, F.lit(100))
        .when(allocation_raw < 0, F.lit(0))
        .otherwise(allocation_raw)
        .alias("allocation_pct"),
        (allocation_raw > 100).alias("allocation_was_capped"),
        T.parse_date(F.col("start_date")).alias("start_date"),
        T.parse_date(F.col("end_date")).alias("end_date"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    ).withColumn(
        # An assignment with no end date, or an end date in the future, is live.
        "is_active",
        F.col("end_date").isNull() | (F.col("end_date") >= F.current_date()),
    )

    return _persist(
        ctx,
        entity="team_membership",
        normalized=normalized,
        order_by=("role", "allocation_pct"),
        source_path=config.bronze_path("project_tracking", "team_membership", ctx.ingest_date),
        notes={"known_defect": "allocation_pct above 100 is capped, original kept in allocation_pct_raw"},
    )


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------


def process_achievements(ctx: PipelineContext) -> StageResult:
    """Conform achievements and attempt to attribute the unlabelled ones.

    A third of this feed arrives with no team_id. Joining on team_name is not an
    option: 25,000 teams share only 200 distinct names, so a name join would
    multiply each unattributed row roughly a hundredfold and corrupt every
    downstream metric.

    Attribution is instead attempted through the reporter. A row is resolved
    only when the reporter belongs to exactly one team whose name also matches
    the name on the achievement. Anything still ambiguous is quarantined with a
    reason rather than guessed at, because a wrong attribution is worse than a
    missing one.

    The mixed-type impact score is resolved separately: numeric values are kept,
    the Low / Medium / High labels are mapped to their band midpoints, and the
    original form is recorded in ``impact_source_type`` so any average can be
    recomputed on numeric values alone.
    """
    from pyspark.sql import functions as F

    raw = _latest_bronze(ctx, "achievements")

    base = raw.select(
        T.normalize_team_id(F.col("team_id")).alias("source_team_id")
        if "team_id" in raw.columns
        else F.lit(None).cast("string").alias("source_team_id"),
        T.clean_string(F.col("team_name")).alias("team_name"),
        T.month_key(F.col("month")).alias("month_key"),
        T.clean_string(F.col("title")).alias("title"),
        T.clean_string(F.col("category")).alias("category"),
        T.parse_impact_score(F.col("impact_score")).alias("impact_score"),
        T.impact_source_type(F.col("impact_score")).alias("impact_source_type"),
        T.normalize_email(F.col("reported_by")).alias("reported_by"),
        *[F.col(c) for c in raw.columns if c.startswith("_")],
    ).withColumn(
        # Deterministic key: the feed has no natural one, and a random or
        # monotonic id would change on every run and break idempotency.
        "achievement_sk",
        T.surrogate_key(
            F.col("month_key"),
            F.col("title"),
            F.col("category"),
            F.col("reported_by"),
            F.col("team_name"),
            F.col("source_team_id"),
        ),
    ).cache()

    unattributed = base.filter(F.col("source_team_id").isNull())
    unattributed_count = unattributed.count()

    resolved_map = None
    if unattributed_count:
        memberships = read_silver(ctx.spark, "team_membership").select(
            F.col("team_id").alias("cand_team_id"),
            F.col("employee_email").alias("cand_email"),
        )
        teams = read_silver(ctx.spark, "teams").select(
            F.col("team_id").alias("team_lookup_id"),
            F.col("team_name").alias("team_lookup_name"),
        )

        candidates = (
            unattributed.select("achievement_sk", "reported_by", "team_name")
            .join(memberships, F.col("reported_by") == F.col("cand_email"), "inner")
            .join(
                teams,
                (F.col("cand_team_id") == F.col("team_lookup_id"))
                & (F.col("team_name") == F.col("team_lookup_name")),
                "inner",
            )
            .groupBy("achievement_sk")
            .agg(F.collect_set("cand_team_id").alias("candidate_team_ids"))
        )

        # Only an unambiguous single candidate is accepted.
        resolved_map = candidates.filter(F.size("candidate_team_ids") == 1).select(
            F.col("achievement_sk").alias("resolved_sk"),
            F.col("candidate_team_ids").getItem(0).alias("resolved_team_id"),
        )

    if resolved_map is not None:
        enriched = base.join(
            resolved_map, base["achievement_sk"] == resolved_map["resolved_sk"], "left"
        ).drop("resolved_sk")
        normalized = enriched.withColumn(
            "team_id", F.coalesce(F.col("source_team_id"), F.col("resolved_team_id"))
        ).withColumn(
            "team_id_origin",
            F.when(F.col("source_team_id").isNotNull(), F.lit("SOURCE"))
            .when(F.col("resolved_team_id").isNotNull(), F.lit("RESOLVED_VIA_REPORTER"))
            .otherwise(F.lit("UNRESOLVED")),
        ).drop("resolved_team_id")
    else:
        normalized = base.withColumn("team_id", F.col("source_team_id")).withColumn(
            "team_id_origin",
            F.when(F.col("source_team_id").isNotNull(), F.lit("SOURCE")).otherwise(
                F.lit("UNRESOLVED")
            ),
        )

    normalized = normalized.drop("source_team_id")
    resolved_count = normalized.filter(
        F.col("team_id_origin") == "RESOLVED_VIA_REPORTER"
    ).count()

    _LOGGER.info(
        "Achievement attribution",
        extra={
            "unattributed_in_source": unattributed_count,
            "resolved_via_reporter": resolved_count,
            "still_unresolved": unattributed_count - resolved_count,
        },
    )

    result = _persist(
        ctx,
        entity="achievements",
        normalized=normalized,
        order_by=("month_key",),
        source_path=config.bronze_path(
            "performance_management", "monthly_achievements", ctx.ingest_date
        ),
        notes={
            "unattributed_in_source": unattributed_count,
            "resolved_via_reporter": resolved_count,
            "attribution_rule": "reporter belongs to exactly one team matching the achievement's team_name",
        },
    )
    base.unpersist()
    return result


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------

#: Processing order. Reference data and people precede the entities that join
#: to them, and dim_person precedes everything that classifies staff.
_PIPELINE: tuple[tuple[str, object], ...] = (
    ("locations", process_locations),
    ("organizations", process_organizations),
    ("employees", process_employees),
    ("contractors", process_contractors),
    ("dim_person", build_dim_person),
    ("teams", process_teams),
    ("team_membership", process_membership),
    ("achievements", process_achievements),
)


def run(ctx: PipelineContext) -> Sequence[StageResult]:
    """Transform every bronze entity into its conformed silver form.

    A failure in one entity is contained: it is recorded and the remaining
    entities still run, except where a later entity genuinely depends on the
    failed one, in which case its own read will fail and be reported the same
    way.
    """
    _LOGGER.info(
        "Silver stage starting",
        extra={
            "entities": [name for name, _ in _PIPELINE],
            "source_root": config.lake_root("bronze"),
            "target_root": config.lake_root("silver"),
        },
    )

    selected: set[str] | None = None
    if ctx.entities:
        source_names = {s.entity for s in ctx.selected_sources()}
        selected = {
            silver_name
            for silver_name, source_name in SILVER_TO_SOURCE.items()
            if source_name in source_names
        }

    results: list[StageResult] = []

    for name, processor in _PIPELINE:
        # dim_person is derived and always runs when its inputs were selected.
        if selected is not None and name not in selected and name != "dim_person":
            continue
        results.append(processor(ctx))  # type: ignore[operator]

    _LOGGER.info(
        "Silver stage complete",
        extra={
            "entities": len(results),
            "succeeded": sum(1 for r in results if r.status == "SUCCESS"),
            "failed": sum(1 for r in results if r.status == "FAILED"),
            "rows_written": sum(r.rows_written for r in results),
            "rows_quarantined": sum(r.rows_quarantined for r in results),
        },
    )
    return results