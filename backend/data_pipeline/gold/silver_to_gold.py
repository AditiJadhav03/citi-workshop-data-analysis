"""Gold layer: curated datasets that answer the business questions.

Each dataset here exists to answer a specific question, and its grain is stated
explicitly because getting the grain wrong is the easiest way to produce a
number that looks plausible and is wrong.

    team_members                    Q1  one row per team per person
    team_locations                  Q2  one row per team
    monthly_team_achievements       Q3  one row per team per month
    leader_not_colocated            Q4  one row per team
    leader_non_direct_staff         Q5  one row per team
    staff_ratio_analysis            Q6  one row per team
    organization_reporting_summary  Q7  one row per organisation
    employee_summary                --  workforce composition
    business_answers                --  one row per question, the headline number

Gold performs joins and aggregation only. Cleaning, typing and classification
all happened in silver; if a transformation is needed here, it belongs upstream.

Two definitions in the questions are genuinely ambiguous, so both readings are
computed and the one used for the headline answer is named explicitly:

    "leader not co-located with team members"
        Primary reading: the leader's own location differs from the location
        where most of the team sits. The comparison against the team's
        registered primary office is also produced, since the two can disagree.

    "non-direct staff to employees ratio"
        Primary reading: non-direct staff as a share of all team members. The
        stricter reading, non-direct divided by direct, is produced alongside.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from .. import config
from ..pipeline_context import PipelineContext, StageResult
from ..utils.io import count_written, read_silver, write_parquet
from ..utils.logger import get_logger, log_duration

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_LOGGER = get_logger(__name__)


# ---------------------------------------------------------------------------
# Shared plumbing
# ---------------------------------------------------------------------------


def _publish(
    ctx: PipelineContext,
    dataset: str,
    frame: "DataFrame",
    *,
    rows_read: int = 0,
    is_aggregate: bool = False,
    notes: dict | None = None,
) -> StageResult:
    """Write one curated dataset and report on it.

    Overwrite is deliberate and is what makes the gold layer idempotent: a
    rerun replaces the dataset outright, so no combination of reruns can
    produce duplicate rows.
    """
    from pyspark.sql import functions as F

    result = StageResult(
        layer="gold",
        entity=dataset,
        source_path=config.lake_root("silver"),
        target_path=config.gold_path(dataset),
        is_aggregate=is_aggregate,
    )
    try:
        with log_duration(_LOGGER, "gold build", dataset=dataset) as span:
            stamped = (
                frame.withColumn("_batch_id", F.lit(ctx.batch_id))
                .withColumn("_processed_at", F.current_timestamp())
                .withColumn("_source_layer", F.lit("silver"))
            )
            if ctx.dry_run:
                result.rows_written = stamped.count()
            else:
                write_parquet(stamped, config.gold_path(dataset), mode="overwrite")
                result.rows_written = count_written(ctx.spark, config.gold_path(dataset))

            result.rows_read = rows_read or result.rows_written
            result.rows_valid = result.rows_written
            span["rows_written"] = result.rows_written
            result.notes = notes or {}
    except Exception as exc:
        _LOGGER.exception("Gold build failed", extra={"dataset": dataset})
        return result.finish("FAILED", str(exc))

    return result.finish("SUCCESS")


def _dominant_member_location(members: "DataFrame") -> "DataFrame":
    """Return the location where most of each team sits.

    Ties are broken by location code so the result is reproducible across runs,
    which matters because this value feeds the co-location answer.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    counts = (
        members.filter(F.col("person_location_code").isNotNull())
        .groupBy("team_id", "person_location_code")
        .agg(F.count("*").alias("headcount"))
    )
    window = Window.partitionBy("team_id").orderBy(
        F.col("headcount").desc(), F.col("person_location_code").asc()
    )
    return (
        counts.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .select(
            F.col("team_id"),
            F.col("person_location_code").alias("dominant_member_location"),
            F.col("headcount").alias("dominant_location_headcount"),
        )
    )


# ---------------------------------------------------------------------------
# Q1 - Who are the members of each team?
# ---------------------------------------------------------------------------


def build_team_members(ctx: PipelineContext) -> tuple["DataFrame", StageResult]:
    """One row per team per person, enriched with staff type and location.

    This is the base fact table: questions 2, 4 and 6 are all derived from it,
    so it is built once and reused rather than recomputed per question.

    A membership whose email does not resolve to the person dimension is kept
    with ``person_found = false`` rather than dropped, so the roster stays
    complete and the resolution gap is measurable instead of invisible.
    """
    from pyspark.sql import functions as F

    membership = read_silver(ctx.spark, "team_membership").alias("m")
    people = read_silver(ctx.spark, "dim_person").alias("p")
    teams = read_silver(ctx.spark, "teams").alias("t")
    locations = read_silver(ctx.spark, "locations").alias("l")

    rows_read = membership.count()

    frame = (
        membership.join(
            people, F.col("m.employee_email") == F.col("p.email"), "left"
        )
        .join(teams, F.col("m.team_id") == F.col("t.team_id"), "left")
        .join(
            locations, F.col("p.location_code") == F.col("l.location_code"), "left"
        )
        .select(
            F.col("m.team_id").alias("team_id"),
            F.col("t.team_name").alias("team_name"),
            F.col("t.org_id").alias("org_id"),
            F.col("t.org_name").alias("org_name"),
            F.col("t.primary_office").alias("team_primary_office"),
            F.col("m.employee_email").alias("employee_email"),
            F.col("p.full_name").alias("full_name"),
            F.col("p.department").alias("department"),
            F.col("p.agency").alias("agency"),
            F.col("m.role").alias("role"),
            # The classification that five of the seven questions depend on.
            # Defined once in silver; gold only reads it.
            F.coalesce(F.col("p.staff_type"), F.lit("UNKNOWN")).alias("staff_type"),
            F.col("p.status").alias("person_status"),
            F.col("p.location_code").alias("person_location_code"),
            F.col("l.city").alias("person_city"),
            F.col("l.country").alias("person_country"),
            F.col("l.region").alias("person_region"),
            F.col("m.allocation_pct").alias("allocation_pct"),
            F.col("m.allocation_was_capped").alias("allocation_was_capped"),
            F.col("m.start_date").alias("start_date"),
            F.col("m.end_date").alias("end_date"),
            F.col("m.is_active").alias("is_active"),
            F.col("p.person_id").isNotNull().alias("person_found"),
            F.col("t.team_id").isNotNull().alias("team_found"),
        )
    ).cache()

    unresolved = frame.filter(~F.col("person_found")).count()
    orphan_teams = frame.filter(~F.col("team_found")).count()
    _LOGGER.info(
        "Team roster resolution",
        extra={
            "memberships": rows_read,
            "unresolved_people": unresolved,
            "memberships_without_team": orphan_teams,
        },
    )

    result = _publish(
        ctx,
        "team_members",
        frame,
        rows_read=rows_read,
        notes={
            "grain": "team_id x employee_email",
            "answers": "Q1",
            "unresolved_people": unresolved,
            "memberships_without_team": orphan_teams,
        },
    )
    return frame, result


# ---------------------------------------------------------------------------
# Q2 - Where are the teams located?
# ---------------------------------------------------------------------------


def build_team_locations(ctx: PipelineContext, members: "DataFrame") -> StageResult:
    """One row per team describing where it sits.

    A team has two notions of location: the office it is registered against and
    where its people actually are. Both are reported, along with how spread out
    the team is, because a single "location" column would hide the difference.
    """
    from pyspark.sql import functions as F

    teams = read_silver(ctx.spark, "teams").alias("t")
    locations = read_silver(ctx.spark, "locations").alias("l")
    rows_read = teams.count()

    spread = members.groupBy("team_id").agg(
        F.count("*").alias("member_count"),
        F.countDistinct("person_location_code").alias("distinct_member_locations"),
        F.collect_set("person_country").alias("member_countries"),
        F.collect_set("person_region").alias("member_regions"),
    )
    dominant = _dominant_member_location(members)

    frame = (
        teams.join(
            locations, F.col("t.primary_office") == F.col("l.location_code"), "left"
        )
        .select(
            F.col("t.team_id").alias("team_id"),
            F.col("t.team_name").alias("team_name"),
            F.col("t.org_id").alias("org_id"),
            F.col("t.org_name").alias("org_name"),
            F.col("t.primary_office").alias("primary_office"),
            F.col("l.city").alias("office_city"),
            F.col("l.country").alias("office_country"),
            F.col("l.region").alias("office_region"),
            F.col("l.timezone").alias("office_timezone"),
        )
        .join(spread, "team_id", "left")
        .join(dominant, "team_id", "left")
        .withColumn(
            "is_distributed", F.coalesce(F.col("distinct_member_locations"), F.lit(0)) > 1
        )
        .withColumn(
            "office_matches_members",
            F.col("primary_office") == F.col("dominant_member_location"),
        )
    )

    return _publish(
        ctx,
        "team_locations",
        frame,
        rows_read=rows_read,
        notes={"grain": "team_id", "answers": "Q2"},
    )


# ---------------------------------------------------------------------------
# Q3 - Monthly achievements per team
# ---------------------------------------------------------------------------


def build_monthly_achievements(ctx: PipelineContext) -> StageResult:
    """One row per team per month.

    Averages are taken over numerically reported scores only. Achievements whose
    impact arrived as a Low / Medium / High label were mapped to band midpoints
    in silver, and mixing an estimate into a mean without saying so would make
    the average look more precise than it is; the counts are reported separately
    so the notebook can show both.
    """
    from pyspark.sql import functions as F

    achievements = read_silver(ctx.spark, "achievements").alias("a")
    teams = read_silver(ctx.spark, "teams").alias("t")
    rows_read = achievements.count()

    joined = achievements.join(
        teams, F.col("a.team_id") == F.col("t.team_id"), "left"
    ).select(
        F.col("a.team_id").alias("team_id"),
        F.coalesce(F.col("t.team_name"), F.col("a.team_name")).alias("team_name"),
        F.col("t.org_id").alias("org_id"),
        F.col("t.org_name").alias("org_name"),
        F.col("a.month_key").alias("month_key"),
        F.col("a.title").alias("title"),
        F.col("a.category").alias("category"),
        F.col("a.impact_score").alias("impact_score"),
        F.col("a.impact_source_type").alias("impact_source_type"),
        F.col("a.team_id_origin").alias("team_id_origin"),
    )

    numeric_only = F.when(F.col("impact_source_type") == "NUMERIC", F.col("impact_score"))

    frame = (
        joined.groupBy("team_id", "team_name", "org_id", "org_name", "month_key")
        .agg(
            F.count("*").alias("achievement_count"),
            F.round(F.avg(numeric_only), 3).alias("avg_impact_score_numeric"),
            F.round(F.avg("impact_score"), 3).alias("avg_impact_score_all"),
            F.round(F.sum("impact_score"), 2).alias("total_impact_score"),
            F.max("impact_score").alias("max_impact_score"),
            F.countDistinct("category").alias("distinct_categories"),
            F.collect_set("category").alias("categories"),
            F.sum(F.when(F.col("impact_source_type") == "BAND", 1).otherwise(0)).alias(
                "banded_score_count"
            ),
            F.sum(
                F.when(F.col("team_id_origin") == "RESOLVED_VIA_REPORTER", 1).otherwise(0)
            ).alias("attributed_via_reporter"),
        )
        .orderBy("team_id", "month_key")
    )

    return _publish(
        ctx,
        "monthly_team_achievements",
        frame,
        is_aggregate=True,
        rows_read=rows_read,
        notes={"grain": "team_id x month_key", "answers": "Q3"},
    )


# ---------------------------------------------------------------------------
# Q4 - Teams whose leader is not co-located with the team
# ---------------------------------------------------------------------------


def build_leader_not_colocated(ctx: PipelineContext, members: "DataFrame") -> StageResult:
    """One row per team, flagging leaders who sit away from their team.

    The leader's location comes from the person dimension rather than from the
    membership roster, because a leader may not appear on the roster of the team
    they lead. Teams whose leader cannot be located are flagged rather than
    counted either way, so the headline number never rests on a guess.
    """
    from pyspark.sql import functions as F

    teams = read_silver(ctx.spark, "teams").alias("t")
    people = read_silver(ctx.spark, "dim_person").alias("p")
    locations = read_silver(ctx.spark, "locations").alias("l")
    rows_read = teams.count()

    dominant = _dominant_member_location(members)

    frame = (
        teams.join(
            people, F.col("t.team_leader_email") == F.col("p.email"), "left"
        )
        .join(locations, F.col("p.location_code") == F.col("l.location_code"), "left")
        .select(
            F.col("t.team_id").alias("team_id"),
            F.col("t.team_name").alias("team_name"),
            F.col("t.team_leader_email").alias("team_leader_email"),
            F.col("p.location_code").alias("leader_location_code"),
            F.col("l.city").alias("leader_city"),
            F.col("l.region").alias("leader_region"),
            F.col("t.primary_office").alias("team_primary_office"),
            F.col("p.person_id").isNotNull().alias("leader_resolved"),
        )
        .join(dominant, "team_id", "left")
        .withColumn(
            # Primary definition: leader sits where most of the team sits.
            "is_colocated_with_members",
            F.when(
                F.col("leader_location_code").isNull()
                | F.col("dominant_member_location").isNull(),
                None,
            ).otherwise(F.col("leader_location_code") == F.col("dominant_member_location")),
        )
        .withColumn(
            # Secondary definition, kept because the two can disagree.
            "is_colocated_with_office",
            F.when(
                F.col("leader_location_code").isNull()
                | F.col("team_primary_office").isNull(),
                None,
            ).otherwise(F.col("leader_location_code") == F.col("team_primary_office")),
        )
        .withColumn(
            "leader_not_colocated", F.col("is_colocated_with_members") == F.lit(False)
        )
    )

    return _publish(
        ctx,
        "leader_not_colocated",
        frame,
        rows_read=rows_read,
        notes={
            "grain": "team_id",
            "answers": "Q4",
            "primary_definition": "leader location differs from dominant member location",
        },
    )


# ---------------------------------------------------------------------------
# Q5 - Teams whose leader is non-direct staff
# ---------------------------------------------------------------------------


def build_leader_non_direct_staff(ctx: PipelineContext) -> StageResult:
    """One row per team, classifying the leader as direct or non-direct staff.

    Roughly 3.6k team leader addresses are absent from the employee directory
    and present in the vendor roster instead, which is precisely what this
    question is asking about. Those leaders are classified as non-direct rather
    than treated as missing data.
    """
    from pyspark.sql import functions as F

    teams = read_silver(ctx.spark, "teams").alias("t")
    people = read_silver(ctx.spark, "dim_person").alias("p")
    rows_read = teams.count()

    frame = (
        teams.join(people, F.col("t.team_leader_email") == F.col("p.email"), "left")
        .select(
            F.col("t.team_id").alias("team_id"),
            F.col("t.team_name").alias("team_name"),
            F.col("t.org_id").alias("org_id"),
            F.col("t.org_name").alias("org_name"),
            F.col("t.team_leader_email").alias("team_leader_email"),
            F.col("p.full_name").alias("leader_name"),
            F.col("p.agency").alias("leader_agency"),
            F.col("p.status").alias("leader_status"),
            F.coalesce(F.col("p.staff_type"), F.lit("UNRESOLVED")).alias(
                "leader_staff_type"
            ),
            F.col("p.person_id").isNotNull().alias("leader_resolved"),
        )
        .withColumn(
            "leader_is_non_direct", F.col("leader_staff_type") == F.lit("NON_DIRECT")
        )
    )

    return _publish(
        ctx,
        "leader_non_direct_staff",
        frame,
        rows_read=rows_read,
        notes={"grain": "team_id", "answers": "Q5"},
    )


# ---------------------------------------------------------------------------
# Q6 - Teams with a non-direct staff ratio above the threshold
# ---------------------------------------------------------------------------


def build_staff_ratio_analysis(ctx: PipelineContext, members: "DataFrame") -> StageResult:
    """One row per team with its direct and non-direct composition.

    Members whose staff type could not be resolved are counted separately and
    excluded from the denominator, so an unresolved person neither inflates nor
    deflates the ratio.
    """
    from pyspark.sql import functions as F

    threshold = config.NON_DIRECT_STAFF_RATIO_THRESHOLD

    frame = (
        members.groupBy("team_id", "team_name", "org_id", "org_name")
        .agg(
            F.count("*").alias("member_count"),
            F.sum(F.when(F.col("staff_type") == "DIRECT", 1).otherwise(0)).alias(
                "direct_count"
            ),
            F.sum(F.when(F.col("staff_type") == "NON_DIRECT", 1).otherwise(0)).alias(
                "non_direct_count"
            ),
            F.sum(F.when(F.col("staff_type") == "UNKNOWN", 1).otherwise(0)).alias(
                "unknown_count"
            ),
            F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_member_count"),
        )
        .withColumn("classified_count", F.col("direct_count") + F.col("non_direct_count"))
        .withColumn(
            # Primary reading: non-direct as a share of all classified members.
            "non_direct_ratio",
            F.when(
                F.col("classified_count") > 0,
                F.round(F.col("non_direct_count") / F.col("classified_count"), 4),
            ),
        )
        .withColumn(
            # Stricter reading of "non-direct staff to employees".
            "non_direct_to_direct_ratio",
            F.when(
                F.col("direct_count") > 0,
                F.round(F.col("non_direct_count") / F.col("direct_count"), 4),
            ),
        )
        .withColumn(
            "exceeds_threshold", F.col("non_direct_ratio") > F.lit(threshold)
        )
        .withColumn("threshold_used", F.lit(threshold))
    )

    return _publish(
        ctx,
        "staff_ratio_analysis",
        frame,
        is_aggregate=True,
        rows_read=members.count(),
        notes={
            "grain": "team_id",
            "answers": "Q6",
            "threshold": threshold,
            "primary_definition": "non_direct / (direct + non_direct)",
        },
    )


# ---------------------------------------------------------------------------
# Q7 - Teams reporting to an organisation leader
# ---------------------------------------------------------------------------


def build_organization_reporting_summary(ctx: PipelineContext) -> StageResult:
    """One row per organisation summarising how its teams report.

    The source carries a ``reports_to_type`` label, but a label is a claim. Each
    claim is checked against the organisation reference data by testing whether
    the team's reporting manager really is that organisation's leader, and both
    the claimed and the verified counts are published. Where they disagree, the
    verified figure is the one to quote.
    """
    from pyspark.sql import functions as F

    teams = read_silver(ctx.spark, "teams").alias("t")
    orgs = read_silver(ctx.spark, "organizations").alias("o")
    rows_read = teams.count()

    per_team = teams.join(orgs, F.col("t.org_id") == F.col("o.org_id"), "left").select(
        F.col("t.team_id").alias("team_id"),
        F.col("t.org_id").alias("org_id"),
        F.coalesce(F.col("o.org_name"), F.col("t.org_name")).alias("org_name"),
        F.col("o.org_leader_email").alias("org_leader_email"),
        F.col("t.reporting_manager_email").alias("reporting_manager_email"),
        F.col("t.reports_to_type").alias("reports_to_type"),
    )

    per_team = per_team.withColumn(
        "claims_org_leader", F.col("reports_to_type") == F.lit("Org Leader")
    ).withColumn(
        "verified_org_leader",
        F.col("reporting_manager_email").isNotNull()
        & (F.col("reporting_manager_email") == F.col("org_leader_email")),
    )

    frame = (
        per_team.groupBy("org_id", "org_name", "org_leader_email")
        .agg(
            F.count("*").alias("team_count"),
            F.sum(F.col("claims_org_leader").cast("int")).alias("teams_claiming_org_leader"),
            F.sum(F.col("verified_org_leader").cast("int")).alias("teams_verified_org_leader"),
            F.sum(
                (F.col("claims_org_leader") & ~F.col("verified_org_leader")).cast("int")
            ).alias("teams_claim_unverified"),
            F.countDistinct("reports_to_type").alias("distinct_reporting_types"),
        )
        .withColumn(
            "verified_share",
            F.round(F.col("teams_verified_org_leader") / F.col("team_count"), 4),
        )
        .orderBy("org_id")
    )

    return _publish(
        ctx,
        "organization_reporting_summary",
        frame,
        is_aggregate=True,
        rows_read=rows_read,
        notes={
            "grain": "org_id",
            "answers": "Q7",
            "note": "claimed and verified counts are both published",
        },
    )


# ---------------------------------------------------------------------------
# Supporting - workforce composition
# ---------------------------------------------------------------------------


def build_employee_summary(ctx: PipelineContext) -> StageResult:
    """Workforce composition by department, location and staff type."""
    from pyspark.sql import functions as F

    people = read_silver(ctx.spark, "dim_person").alias("p")
    locations = read_silver(ctx.spark, "locations").alias("l")
    rows_read = people.count()

    frame = (
        people.join(locations, F.col("p.location_code") == F.col("l.location_code"), "left")
        .groupBy(
            F.coalesce(F.col("p.department"), F.lit("UNASSIGNED")).alias("department"),
            F.col("p.staff_type").alias("staff_type"),
            F.col("p.location_code").alias("location_code"),
            F.col("l.city").alias("city"),
            F.col("l.country").alias("country"),
            F.col("l.region").alias("region"),
        )
        .agg(
            F.count("*").alias("headcount"),
            F.sum(F.when(F.col("p.status") == "ACTIVE", 1).otherwise(0)).alias("active_headcount"),
        )
        .orderBy(F.col("headcount").desc())
    )

    return _publish(
        ctx,
        "employee_summary",
        frame,
        is_aggregate=True,
        rows_read=rows_read,
        notes={"grain": "department x staff_type x location_code"},
    )


# ---------------------------------------------------------------------------
# The headline answers
# ---------------------------------------------------------------------------


def build_business_answers(ctx: PipelineContext) -> StageResult:
    """One row per business question holding its headline number.

    Computing the answers inside the pipeline rather than inside the notebook
    means the numbers are versioned, reproducible and testable, and the notebook
    becomes presentation rather than calculation.
    """
    from pyspark.sql import functions as F

    from ..utils.io import read_gold

    members = read_gold(ctx.spark, "team_members")
    locations_ds = read_gold(ctx.spark, "team_locations")
    achievements = read_gold(ctx.spark, "monthly_team_achievements")
    colocation = read_gold(ctx.spark, "leader_not_colocated")
    leadership = read_gold(ctx.spark, "leader_non_direct_staff")
    ratios = read_gold(ctx.spark, "staff_ratio_analysis")
    org_summary = read_gold(ctx.spark, "organization_reporting_summary")

    def _scalar(frame: "DataFrame", column) -> int:
        row = frame.agg(column.alias("value")).collect()[0]
        return int(row["value"] or 0)

    answers = [
        (
            "Q1",
            "Who are the members of each team?",
            _scalar(members, F.countDistinct("team_id")),
            "teams with a roster",
            f"{members.count()} membership rows in gold.team_members",
        ),
        (
            "Q2",
            "Where are the teams located?",
            _scalar(locations_ds, F.countDistinct("primary_office")),
            "distinct offices in use",
            f"{_scalar(locations_ds, F.sum(F.col('is_distributed').cast('int')))} teams span more than one location",
        ),
        (
            "Q3",
            "What are the key achievements of each team monthly?",
            _scalar(achievements, F.countDistinct("month_key")),
            "months covered",
            f"{achievements.count()} team-month rows in gold.monthly_team_achievements",
        ),
        (
            "Q4",
            "How many teams have a team leader not co-located with team members?",
            _scalar(colocation, F.sum(F.col("leader_not_colocated").cast("int"))),
            "teams",
            "leader location differs from the dominant member location",
        ),
        (
            "Q5",
            "How many teams have a team leader who is non-direct staff?",
            _scalar(leadership, F.sum(F.col("leader_is_non_direct").cast("int"))),
            "teams",
            "leader resolves to the vendor roster rather than the directory",
        ),
        (
            "Q6",
            "How many teams have a non-direct staff ratio above 20%?",
            _scalar(ratios, F.sum(F.col("exceeds_threshold").cast("int"))),
            "teams",
            f"threshold {config.NON_DIRECT_STAFF_RATIO_THRESHOLD:.0%}, ratio is non_direct / classified members",
        ),
        (
            "Q7",
            "How many teams report to an organisation leader?",
            _scalar(org_summary, F.sum("teams_verified_org_leader")),
            "teams",
            f"verified against organisation reference data; {_scalar(org_summary, F.sum('teams_claiming_org_leader'))} teams claim it in the source",
        ),
    ]

    frame = ctx.spark.createDataFrame(
        answers, schema="question_id string, question string, answer long, unit string, detail string"
    )

    for question_id, question, answer, unit, detail in answers:
        _LOGGER.info(
            "Business answer",
            extra={"question_id": question_id, "answer": answer, "unit": unit, "detail": detail},
        )

    return _publish(
        ctx,
        "business_answers",
        frame,
        rows_read=len(answers),
        notes={"grain": "question_id", "answers": "Q1-Q7"},
    )


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------


def run(ctx: PipelineContext) -> Sequence[StageResult]:
    """Build every curated dataset from the silver layer."""
    _LOGGER.info(
        "Gold stage starting",
        extra={
            "datasets": list(config.GOLD_DATASETS),
            "source_root": config.lake_root("silver"),
            "target_root": config.lake_root("gold"),
        },
    )

    results: list[StageResult] = []

    # team_members is the base fact table for Q2, Q4 and Q6, so it is built
    # first and the frame reused rather than read back three times.
    members, members_result = build_team_members(ctx)
    results.append(members_result)

    if members_result.status == "FAILED":
        _LOGGER.error("team_members failed; dependent datasets cannot be built")
        return results

    builders = (
        ("team_locations", lambda: build_team_locations(ctx, members)),
        ("monthly_team_achievements", lambda: build_monthly_achievements(ctx)),
        ("leader_not_colocated", lambda: build_leader_not_colocated(ctx, members)),
        ("leader_non_direct_staff", lambda: build_leader_non_direct_staff(ctx)),
        ("staff_ratio_analysis", lambda: build_staff_ratio_analysis(ctx, members)),
        ("organization_reporting_summary", lambda: build_organization_reporting_summary(ctx)),
        ("employee_summary", lambda: build_employee_summary(ctx)),
    )

    for name, builder in builders:
        try:
            results.append(builder())
        except Exception as exc:
            _LOGGER.exception("Gold dataset failed", extra={"dataset": name})
            results.append(StageResult(layer="gold", entity=name).finish("FAILED", str(exc)))

    # The headline answers depend on every dataset above having been written.
    if all(r.status != "FAILED" for r in results):
        try:
            results.append(build_business_answers(ctx))
        except Exception as exc:
            _LOGGER.exception("Business answers failed")
            results.append(
                StageResult(layer="gold", entity="business_answers").finish("FAILED", str(exc))
            )

    members.unpersist()

    _LOGGER.info(
        "Gold stage complete",
        extra={
            "datasets": len(results),
            "succeeded": sum(1 for r in results if r.status == "SUCCESS"),
            "failed": sum(1 for r in results if r.status == "FAILED"),
            "rows_written": sum(r.rows_written for r in results),
        },
    )
    return results