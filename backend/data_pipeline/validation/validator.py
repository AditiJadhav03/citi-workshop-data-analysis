"""Schema validation and data quality engine.

Rules are declared as SQL expressions that must evaluate to true for a row to be
considered valid. Validation returns two frames rather than one: the rows that
passed, and the rows that failed annotated with the names of every rule they
broke. Nothing is ever dropped silently.

SQL expressions were chosen over per-row Pydantic models deliberately. Pydantic
validates one Python object at a time, which would mean pulling 244k membership
rows out of the JVM; expression rules stay inside Spark and run vectorised over
the whole dataset. The declarative shape is preserved, so rules remain readable
and unit testable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Sequence

from ..utils.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

_LOGGER = get_logger(__name__)

Severity = Literal["error", "warning"]

#: Column names added to quarantined records.
REJECTION_REASON_COLUMN = "_rejection_reason"
VIOLATIONS_COLUMN = "_violations"


class SchemaDriftError(Exception):
    """Raised when an entity is missing columns its contract requires.

    Treated as fatal for that entity: the file is quarantined whole rather than
    processed against a schema that no longer matches, per the error handling
    matrix.
    """


@dataclass(frozen=True)
class Rule:
    """A single data quality constraint.

    Attributes:
        name: Short identifier that appears in the rejection reason.
        expr: SQL expression that must be true for a valid row.
        description: Human readable explanation, used in documentation.
        severity: ``error`` routes failures to quarantine; ``warning`` only logs.
        null_passes: Result when the expression evaluates to NULL, which happens
            whenever the underlying value is null. Range and format checks
            normally pass on null so that nullability is governed by its own
            explicit rule rather than being enforced twice.
    """

    name: str
    expr: str
    description: str
    severity: Severity = "error"
    null_passes: bool = True


@dataclass(frozen=True)
class EntitySchema:
    """The validation contract for one silver entity."""

    entity: str
    required_columns: tuple[str, ...]
    business_key: tuple[str, ...]
    rules: tuple[Rule, ...] = ()
    description: str = ""
    #: Share of rows that may be quarantined before the run is considered
    #: failed. Defaults to the global threshold in config; an entity with a
    #: known and documented source data defect may declare its own tolerance so
    #: that a source problem is not mistaken for a pipeline bug.
    max_quarantine_ratio: float | None = None

    @property
    def error_rules(self) -> tuple[Rule, ...]:
        """Rules whose failure sends a row to quarantine."""
        return tuple(r for r in self.rules if r.severity == "error")

    @property
    def warning_rules(self) -> tuple[Rule, ...]:
        """Rules whose failure is logged but does not reject the row."""
        return tuple(r for r in self.rules if r.severity == "warning")


@dataclass
class ValidationOutcome:
    """Result of validating one entity."""

    entity: str
    valid: "DataFrame"
    invalid: "DataFrame"
    rows_in: int = 0
    rows_valid: int = 0
    rows_invalid: int = 0
    rule_failures: dict[str, int] = field(default_factory=dict)
    warnings: dict[str, int] = field(default_factory=dict)

    @property
    def reject_ratio(self) -> float:
        """Share of input rows that failed validation."""
        return self.rows_invalid / self.rows_in if self.rows_in else 0.0


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


def check_required_columns(df: "DataFrame", schema: EntitySchema) -> None:
    """Fail fast when the incoming frame is missing contracted columns.

    Raises:
        SchemaDriftError: if any required column is absent.
    """
    missing = [c for c in schema.required_columns if c not in df.columns]
    if missing:
        raise SchemaDriftError(
            f"Entity '{schema.entity}' is missing required columns: {missing}. "
            f"Present: {sorted(df.columns)}"
        )


def _violation_expression(rules: Sequence[Rule]):
    """Build an array column naming every error rule a row breaks."""
    from pyspark.sql import functions as F

    checks = []
    for rule in rules:
        # coalesce guards the NULL case: an expression over a null value yields
        # NULL, which would otherwise be treated as neither pass nor fail.
        passed = F.coalesce(F.expr(rule.expr), F.lit(rule.null_passes))
        checks.append(F.when(~passed, F.lit(rule.name)))
    return F.array_compact(F.array(*checks))


def validate(df: "DataFrame", schema: EntitySchema, *, count: bool = True) -> ValidationOutcome:
    """Split a frame into valid and invalid rows against an entity contract.

    Args:
        df: The normalised frame to validate.
        schema: The contract to apply.
        count: Whether to materialise row counts. Disable in tests where only
            the frames matter.

    Returns:
        A ValidationOutcome holding both frames and the per-rule failure counts.

    Raises:
        SchemaDriftError: if required columns are missing.
    """
    from pyspark.sql import functions as F

    check_required_columns(df, schema)

    if not schema.error_rules:
        outcome = ValidationOutcome(
            entity=schema.entity, valid=df, invalid=df.limit(0)
        )
        if count:
            outcome.rows_in = outcome.rows_valid = df.count()
        return outcome

    annotated = df.withColumn(
        VIOLATIONS_COLUMN, _violation_expression(schema.error_rules)
    ).cache()

    valid = annotated.filter(F.size(F.col(VIOLATIONS_COLUMN)) == 0).drop(VIOLATIONS_COLUMN)
    invalid = annotated.filter(F.size(F.col(VIOLATIONS_COLUMN)) > 0).withColumn(
        REJECTION_REASON_COLUMN, F.concat_ws("; ", F.col(VIOLATIONS_COLUMN))
    )

    outcome = ValidationOutcome(entity=schema.entity, valid=valid, invalid=invalid)

    if count:
        outcome.rows_in = annotated.count()
        outcome.rows_invalid = invalid.count()
        outcome.rows_valid = outcome.rows_in - outcome.rows_invalid
        outcome.rule_failures = _count_rule_failures(annotated, schema)
        outcome.warnings = _count_warnings(annotated, schema)

        _LOGGER.info(
            "Validation complete",
            extra={
                "entity": schema.entity,
                "rows_in": outcome.rows_in,
                "rows_valid": outcome.rows_valid,
                "rows_invalid": outcome.rows_invalid,
                "reject_ratio": round(outcome.reject_ratio, 4),
                "rule_failures": outcome.rule_failures,
                "warnings": outcome.warnings,
            },
        )

    return outcome


def _count_rule_failures(annotated: "DataFrame", schema: EntitySchema) -> dict[str, int]:
    """Count how many rows each error rule rejected.

    Per-rule counts make a spike in quarantine volume diagnosable at a glance
    instead of requiring the quarantine files to be opened.
    """
    from pyspark.sql import functions as F

    if not schema.error_rules:
        return {}
    aggregates = [
        F.sum(F.array_contains(F.col(VIOLATIONS_COLUMN), rule.name).cast("int")).alias(rule.name)
        for rule in schema.error_rules
    ]
    row = annotated.agg(*aggregates).collect()[0].asDict()
    return {name: int(value or 0) for name, value in row.items() if value}


def _count_warnings(annotated: "DataFrame", schema: EntitySchema) -> dict[str, int]:
    """Count rows breaking warning-level rules, which are not rejected."""
    from pyspark.sql import functions as F

    if not schema.warning_rules:
        return {}
    aggregates = [
        F.sum((~F.coalesce(F.expr(rule.expr), F.lit(rule.null_passes))).cast("int")).alias(
            rule.name
        )
        for rule in schema.warning_rules
    ]
    row = annotated.agg(*aggregates).collect()[0].asDict()
    return {name: int(value or 0) for name, value in row.items() if value}


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def deduplicate(
    df: "DataFrame",
    schema: EntitySchema,
    *,
    order_by: Sequence[str] | None = None,
) -> tuple["DataFrame", "DataFrame"]:
    """Keep one row per business key and return the discarded duplicates.

    Deduplicating on the declared business key rather than on the whole row is
    what catches genuine duplicates whose non-key columns happen to differ. The
    surviving row is chosen deterministically so reruns are reproducible, and
    the losing rows are returned so they can be quarantined rather than lost.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not schema.business_key:
        return df, df.limit(0)

    ordering = [F.col(c).asc_nulls_last() for c in (order_by or schema.business_key)]
    window = Window.partitionBy(*schema.business_key).orderBy(*ordering)

    ranked = df.withColumn("_row_rank", F.row_number().over(window))
    kept = ranked.filter(F.col("_row_rank") == 1).drop("_row_rank")
    dropped = ranked.filter(F.col("_row_rank") > 1).withColumn(
        REJECTION_REASON_COLUMN,
        F.concat_ws(
            "", F.lit("duplicate_business_key["), F.lit(",".join(schema.business_key)), F.lit("]")
        ),
    ).drop("_row_rank")

    return kept, dropped


def mark_rejected(df: "DataFrame", reason: str) -> "DataFrame":
    """Attach a fixed rejection reason to a frame bound for quarantine."""
    from pyspark.sql import functions as F

    return df.withColumn(REJECTION_REASON_COLUMN, F.lit(reason))