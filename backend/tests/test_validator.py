"""Unit tests for the schema validation engine.

The engine's contract is that no row is ever lost: every input row emerges
either in the valid frame or in the invalid frame carrying the names of the
rules it broke. These tests exercise that contract, the null-handling rule that
is easy to get subtly wrong, and deduplication on the declared business key.
"""

import pytest

from data_pipeline.validation import schemas
from data_pipeline.validation.validator import (
    REJECTION_REASON_COLUMN,
    EntitySchema,
    Rule,
    SchemaDriftError,
    deduplicate,
    mark_rejected,
    validate,
)


@pytest.fixture
def simple_schema():
    """A small contract used to exercise the engine itself."""
    return EntitySchema(
        entity="widgets",
        required_columns=("widget_id", "size"),
        business_key=("widget_id",),
        rules=(
            Rule("id_required", "widget_id IS NOT NULL", "Key must be present",
                 null_passes=False),
            Rule("id_format", "widget_id RLIKE '^W-[0-9]+$'", "Ids follow W-nnn"),
            Rule("size_in_range", "size BETWEEN 0 AND 100", "Size is a percentage"),
            Rule("size_present", "size IS NOT NULL", "Size should be known",
                 severity="warning"),
        ),
    )


@pytest.fixture
def widgets(spark):
    """Rows covering valid, malformed, out-of-range and null cases."""
    return spark.createDataFrame(
        [
            ("W-001", 50),
            ("W-002", 100),
            ("bad-id", 30),
            ("W-004", 150),
            (None, 20),
            ("W-006", None),
        ],
        "widget_id string, size int",
    )


class TestValidationSplit:
    """Rows are partitioned, never dropped."""

    def test_every_input_row_is_accounted_for(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        assert outcome.rows_valid + outcome.rows_invalid == outcome.rows_in == 6

    def test_valid_rows_pass_every_error_rule(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        ids = sorted(r["widget_id"] for r in outcome.valid.collect())
        assert ids == ["W-001", "W-002", "W-006"]

    def test_invalid_rows_are_retained(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        assert outcome.rows_invalid == 3

    def test_reject_ratio_is_reported(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        assert outcome.reject_ratio == pytest.approx(0.5)


class TestRejectionReasons:
    """Quarantined rows say why they were rejected."""

    def test_rejection_reason_column_is_added(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        assert REJECTION_REASON_COLUMN in outcome.invalid.columns

    def test_reason_names_the_broken_rule(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        reasons = {
            r["widget_id"]: r[REJECTION_REASON_COLUMN] for r in outcome.invalid.collect()
        }
        assert "id_format" in reasons["bad-id"]
        assert "size_in_range" in reasons["W-004"]

    def test_a_row_can_report_several_broken_rules(self, spark, simple_schema):
        frame = spark.createDataFrame([("nope", 500)], "widget_id string, size int")
        outcome = validate(frame, simple_schema)
        reason = outcome.invalid.collect()[0][REJECTION_REASON_COLUMN]
        assert "id_format" in reason and "size_in_range" in reason

    def test_per_rule_failure_counts_are_reported(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        assert outcome.rule_failures["id_format"] == 1
        assert outcome.rule_failures["size_in_range"] == 1
        assert outcome.rule_failures["id_required"] == 1


class TestNullHandling:
    """A rule evaluating to NULL must not silently pass or fail."""

    def test_range_rule_ignores_nulls_by_default(self, widgets, simple_schema):
        # W-006 has a null size. Nullability is governed by its own rule, so a
        # range check must not reject the row as well.
        outcome = validate(widgets, simple_schema)
        assert "W-006" in [r["widget_id"] for r in outcome.valid.collect()]

    def test_null_passes_false_rejects_nulls(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        rejected = [r["widget_id"] for r in outcome.invalid.collect()]
        assert None in rejected

    def test_null_passes_is_configurable(self, spark):
        strict = EntitySchema(
            entity="w",
            required_columns=("size",),
            business_key=("size",),
            rules=(Rule("size_range", "size BETWEEN 0 AND 100", "strict",
                        null_passes=False),),
        )
        frame = spark.createDataFrame([(None,), (50,)], "size int")
        outcome = validate(frame, strict)
        assert outcome.rows_valid == 1 and outcome.rows_invalid == 1


class TestSeverity:
    """Warnings are counted but do not reject the row."""

    def test_warning_does_not_reject(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        valid_ids = [r["widget_id"] for r in outcome.valid.collect()]
        assert "W-006" in valid_ids

    def test_warning_is_counted(self, widgets, simple_schema):
        outcome = validate(widgets, simple_schema)
        assert outcome.warnings.get("size_present") == 1

    def test_severity_split_on_the_schema(self, simple_schema):
        assert len(simple_schema.error_rules) == 3
        assert len(simple_schema.warning_rules) == 1


class TestSchemaDrift:
    """A missing contracted column is fatal for that entity."""

    def test_missing_required_column_raises(self, spark, simple_schema):
        frame = spark.createDataFrame([("W-001",)], "widget_id string")
        with pytest.raises(SchemaDriftError) as exc:
            validate(frame, simple_schema)
        assert "size" in str(exc.value)

    def test_error_names_the_entity(self, spark, simple_schema):
        frame = spark.createDataFrame([("W-001",)], "widget_id string")
        with pytest.raises(SchemaDriftError, match="widgets"):
            validate(frame, simple_schema)


class TestDeduplicate:
    """One row per business key, chosen deterministically."""

    def test_keeps_one_row_per_key(self, spark, simple_schema):
        frame = spark.createDataFrame(
            [("W-001", 10), ("W-001", 20), ("W-002", 30)],
            "widget_id string, size int",
        )
        kept, dropped = deduplicate(frame, simple_schema, order_by=("size",))
        assert kept.count() == 2 and dropped.count() == 1

    def test_dedup_uses_the_business_key_not_the_whole_row(self, spark, simple_schema):
        # Rows differing outside the key are still duplicates. Whole-row dedup
        # would keep both and let a duplicate id through.
        frame = spark.createDataFrame(
            [("W-001", 10), ("W-001", 99)], "widget_id string, size int"
        )
        kept, _ = deduplicate(frame, simple_schema, order_by=("size",))
        assert kept.count() == 1

    def test_winner_is_deterministic(self, spark, simple_schema):
        frame = spark.createDataFrame(
            [("W-001", 90), ("W-001", 10)], "widget_id string, size int"
        )
        first, _ = deduplicate(frame, simple_schema, order_by=("size",))
        second, _ = deduplicate(frame, simple_schema, order_by=("size",))
        assert first.collect()[0]["size"] == second.collect()[0]["size"] == 10

    def test_discarded_duplicates_carry_a_reason(self, spark, simple_schema):
        frame = spark.createDataFrame(
            [("W-001", 10), ("W-001", 20)], "widget_id string, size int"
        )
        _, dropped = deduplicate(frame, simple_schema, order_by=("size",))
        reason = dropped.collect()[0][REJECTION_REASON_COLUMN]
        assert "duplicate_business_key" in reason

    def test_no_rows_lost(self, spark, simple_schema):
        frame = spark.createDataFrame(
            [("W-001", 10), ("W-001", 20), ("W-002", 30)],
            "widget_id string, size int",
        )
        kept, dropped = deduplicate(frame, simple_schema, order_by=("size",))
        assert kept.count() + dropped.count() == frame.count()


class TestMarkRejected:
    """Fixed-reason rejection for whole-frame failures."""

    def test_attaches_the_reason(self, spark):
        frame = spark.createDataFrame([("W-001",)], "widget_id string")
        marked = mark_rejected(frame, "unresolvable_reference")
        assert marked.collect()[0][REJECTION_REASON_COLUMN] == "unresolvable_reference"


class TestRegisteredSchemas:
    """The contracts the pipeline actually uses."""

    def test_every_pipeline_entity_has_a_schema(self):
        expected = {
            "employees", "contractors", "locations", "organizations",
            "teams", "team_membership", "achievements",
        }
        assert expected <= set(schemas.SCHEMAS)

    def test_every_schema_declares_a_business_key(self):
        for name, schema in schemas.SCHEMAS.items():
            assert schema.business_key, f"{name} has no business key"

    def test_every_rule_has_a_description(self):
        for entry in schemas.rule_catalogue():
            assert entry["description"], f"{entry['rule']} has no description"

    def test_rule_names_are_unique_within_an_entity(self):
        for name, schema in schemas.SCHEMAS.items():
            names = [r.name for r in schema.rules]
            assert len(names) == len(set(names)), f"duplicate rule name in {name}"

    def test_allocation_range_is_a_warning(self):
        # Rejecting a quarter of the membership export over an allocation typo
        # would remove real people from real teams, so this rule flags rather
        # than rejects.
        rule = next(
            r for r in schemas.TEAM_MEMBERSHIP.rules if r.name == "allocation_in_range"
        )
        assert rule.severity == "warning"

    def test_achievements_declare_their_own_quarantine_tolerance(self):
        # A third of this feed has no team_id in the source, which is a
        # documented source defect rather than a pipeline fault.
        assert schemas.ACHIEVEMENTS.max_quarantine_ratio == 0.40

    def test_catalogue_covers_every_rule(self):
        total = sum(len(s.rules) for s in schemas.SCHEMAS.values())
        assert len(schemas.rule_catalogue()) == total
