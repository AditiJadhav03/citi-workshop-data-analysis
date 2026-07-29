"""Unit tests for the column transformations.

Every cleaning rule the pipeline applies is defined once in
``data_pipeline.utils.transforms`` and tested here against the exact defect
patterns found while profiling the source systems.
"""

import pytest

from data_pipeline.utils import transforms as T


class TestCleanString:
    """Whitespace and empty-string handling."""

    def test_trims_surrounding_whitespace(self, apply_transform):
        # The membership export pads emails with spaces, which silently
        # orphans them from the person dimension if left in place.
        result = apply_transform(T.clean_string, ["  jeffrey@acme-inc.com  "])
        assert result == ["jeffrey@acme-inc.com"]

    def test_empty_string_becomes_null(self, apply_transform):
        assert apply_transform(T.clean_string, ["", "   "]) == [None, None]

    def test_null_stays_null(self, apply_transform):
        assert apply_transform(T.clean_string, [None]) == [None]

    def test_leaves_clean_values_untouched(self, apply_transform):
        assert apply_transform(T.clean_string, ["EMP-00001"]) == ["EMP-00001"]


class TestNormalizeEmail:
    """Corporate domain repair and casing."""

    @pytest.mark.parametrize(
        "raw",
        ["acmeinc.com", "acme_inc.com", "acme-inc.co"],
    )
    def test_repairs_each_known_domain_typo(self, apply_transform, raw):
        result = apply_transform(T.normalize_email, [f"jane.doe@{raw}"])
        assert result == ["jane.doe@acme-inc.com"]

    def test_lowercases(self, apply_transform):
        result = apply_transform(T.normalize_email, ["DANIELLE.JOHNSON@ACME-INC.COM"])
        assert result == ["danielle.johnson@acme-inc.com"]

    def test_trims_and_repairs_together(self, apply_transform):
        result = apply_transform(T.normalize_email, ["  ANDREW_DAVIS@ACMEINC.COM  "])
        assert result == ["andrew_davis@acme-inc.com"]

    def test_leaves_external_domains_alone(self, apply_transform):
        result = apply_transform(T.normalize_email, ["greg@bluewave.com"])
        assert result == ["greg@bluewave.com"]

    def test_value_without_at_sign_is_null(self, apply_transform):
        assert apply_transform(T.normalize_email, ["not-an-email"]) == [None]


class TestParseDate:
    """All six date formats observed across the source systems."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("2018-08-10", "2018-08-10"),
            ("2021/05/05", "2021-05-05"),
            ("11/18/2016", "2016-11-18"),
            ("12-May-2022", "2022-05-12"),
            ("August 19, 2023", "2023-08-19"),
            ("19 August 2023", "2023-08-19"),
        ],
    )
    def test_parses_every_known_format(self, apply_transform, raw, expected):
        result = apply_transform(T.parse_date, [raw])
        assert str(result[0]) == expected

    def test_unparseable_becomes_null_rather_than_raising(self, apply_transform):
        # Returning null is deliberate: validation then flags the null and the
        # row is quarantined with a reason, instead of the job dying.
        assert apply_transform(T.parse_date, ["not a date"]) == [None]

    def test_empty_and_null_become_null(self, apply_transform):
        assert apply_transform(T.parse_date, ["", None]) == [None, None]

    def test_handles_surrounding_whitespace(self, apply_transform):
        result = apply_transform(T.parse_date, ["  2020-04-05  "])
        assert str(result[0]) == "2020-04-05"


class TestParseMonth:
    """Reporting month parsing."""

    def test_parses_iso_month(self, apply_transform):
        assert apply_transform(T.month_key, ["2026-07"]) == ["2026-07"]

    def test_parses_slash_month(self, apply_transform):
        assert apply_transform(T.month_key, ["2026/05"]) == ["2026-05"]

    def test_unparseable_becomes_null(self, apply_transform):
        assert apply_transform(T.month_key, ["last Tuesday"]) == [None]


class TestNumericParsing:
    """Integer and double coercion."""

    def test_parses_integers(self, apply_transform):
        assert apply_transform(T.parse_int, ["100", "0", "120"]) == [100, 0, 120]

    def test_non_numeric_int_becomes_null(self, apply_transform):
        assert apply_transform(T.parse_int, ["many", "10.5", ""]) == [None, None, None]

    def test_parses_doubles(self, apply_transform):
        assert apply_transform(T.parse_double, ["2.2", "6"]) == [2.2, 6.0]

    def test_non_numeric_double_becomes_null(self, apply_transform):
        assert apply_transform(T.parse_double, ["High"]) == [None]


class TestImpactScore:
    """The mixed-type impact score column."""

    def test_keeps_numeric_values(self, apply_transform):
        assert apply_transform(T.parse_impact_score, ["2.2", "6.5"]) == [2.2, 6.5]

    @pytest.mark.parametrize(
        "label,expected", [("Low", 2.5), ("Medium", 5.0), ("High", 8.0)]
    )
    def test_maps_bands_to_midpoints(self, apply_transform, label, expected):
        assert apply_transform(T.parse_impact_score, [label]) == [expected]

    def test_band_matching_is_case_insensitive(self, apply_transform):
        assert apply_transform(T.parse_impact_score, ["HIGH", "low"]) == [8.0, 2.5]

    def test_missing_stays_null(self, apply_transform):
        assert apply_transform(T.parse_impact_score, [None]) == [None]

    def test_records_how_each_value_arrived(self, apply_transform):
        result = apply_transform(
            T.impact_source_type, ["2.2", "High", None, "gibberish"]
        )
        assert result == ["NUMERIC", "BAND", "MISSING", "UNPARSEABLE"]


class TestCategoricalConformance:
    """Vocabulary folding for the free-text categorical columns."""

    @pytest.mark.parametrize(
        "raw", ["Full-Time", "Full Time", "FT", "Employee", "full-time", "FTE"]
    )
    def test_all_six_employment_spellings_conform(self, apply_transform, raw):
        assert apply_transform(T.normalize_employment_type, [raw]) == ["FULL_TIME"]

    @pytest.mark.parametrize(
        "raw", ["C2C", "contractor", "Temp", "Contractor", "1099", "Consultant", "Vendor"]
    )
    def test_all_seven_engagement_spellings_conform(self, apply_transform, raw):
        assert apply_transform(T.normalize_engagement_type, [raw]) == ["CONTRACTOR"]

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Team Lead", "TEAM_LEAD"),
            ("Member", "MEMBER"),
            ("Contributor", "MEMBER"),
            ("Individual Contributor", "MEMBER"),
            ("SME", "MEMBER"),
        ],
    )
    def test_roles_collapse_to_lead_or_member(self, apply_transform, raw, expected):
        assert apply_transform(T.normalize_role, [raw]) == [expected]

    def test_unknown_value_is_flagged_not_dropped(self, apply_transform):
        # An unexpected new value must stay visible in aggregates rather than
        # silently becoming null.
        assert apply_transform(T.normalize_employment_type, ["Zero Hours"]) == ["UNKNOWN"]

    def test_null_stays_null(self, apply_transform):
        assert apply_transform(T.normalize_role, [None]) == [None]


class TestNormalizeTeamId:
    """The three spellings of a team identifier.

    Regression coverage for two defects found in production runs: the source
    writes the same id three ways, and an early fix used Spark's lpad, which
    truncates values already longer than the target width and collapsed 25,000
    teams onto 999 distinct ids.
    """

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("TM-001", "TM-001"),
            ("tm-126", "TM-126"),
            ("tm685", "TM-685"),
            ("  TM-042  ", "TM-042"),
            ("TM-0042", "TM-042"),
        ],
    )
    def test_folds_every_spelling_to_canonical_form(self, apply_transform, raw, expected):
        assert apply_transform(T.normalize_team_id, [raw]) == [expected]

    @pytest.mark.parametrize("raw", ["TM-1000", "TM-1210", "TM-9999", "tm1222"])
    def test_ids_longer_than_three_digits_are_not_truncated(self, apply_transform, raw):
        # lpad(digits, 3, "0") would turn TM-1000 into TM-100.
        result = apply_transform(T.normalize_team_id, [raw])
        digits = raw.strip().upper().replace("TM-", "").replace("TM", "")
        assert result == [f"TM-{digits}"]

    def test_distinct_ids_stay_distinct(self, apply_transform):
        raw = ["TM-100", "TM-1000", "TM-10000"]
        assert len(set(apply_transform(T.normalize_team_id, raw))) == 3

    def test_unrecognisable_value_becomes_null(self, apply_transform):
        assert apply_transform(T.normalize_team_id, ["TEAM-5", "", None]) == [
            None,
            None,
            None,
        ]


class TestSurrogateKey:
    """Deterministic keys for sources with no natural key."""

    def test_same_input_produces_same_key(self, spark):
        from pyspark.sql import functions as F

        frame = spark.createDataFrame(
            [("2026-07", "title", "cat")], "a string, b string, c string"
        )
        key = T.surrogate_key(F.col("a"), F.col("b"), F.col("c"))
        first = frame.select(key).collect()[0][0]
        second = frame.select(key).collect()[0][0]
        # Determinism is what makes the pipeline idempotent: a generated id
        # would differ on every run and duplicate the achievements feed.
        assert first == second

    def test_different_input_produces_different_key(self, spark):
        from pyspark.sql import functions as F

        frame = spark.createDataFrame(
            [("2026-07", "title one"), ("2026-07", "title two")],
            "a string, b string",
        )
        keys = [r[0] for r in frame.select(T.surrogate_key(F.col("a"), F.col("b"))).collect()]
        assert keys[0] != keys[1]

    def test_nulls_do_not_break_key_generation(self, spark):
        from pyspark.sql import functions as F

        frame = spark.createDataFrame([(None, "title")], "a string, b string")
        key = frame.select(T.surrogate_key(F.col("a"), F.col("b"))).collect()[0][0]
        assert key is not None and len(key) == 64
