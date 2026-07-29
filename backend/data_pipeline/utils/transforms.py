"""Reusable column transformations for the silver layer.

Every cleaning rule in the pipeline is defined here exactly once and imported
where needed. If a normalisation appears inline inside a layer job, it belongs
in this module instead.

All functions take and return Spark Columns, so they compose and stay vectorised
across the full dataset rather than iterating row by row in Python.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .. import config

if TYPE_CHECKING:
    from pyspark.sql import Column


# ---------------------------------------------------------------------------
# Strings
# ---------------------------------------------------------------------------


def clean_string(column: "Column") -> "Column":
    """Trim surrounding whitespace and turn empty strings into nulls.

    The membership export contains values padded with spaces such as
    ``"  jeffrey_lowe@acme-inc.com  "``. Left untrimmed these break every join
    that uses email as a key.
    """
    from pyspark.sql import functions as F

    trimmed = F.trim(column.cast("string"))
    return F.when((trimmed == "") | trimmed.isNull(), None).otherwise(trimmed)


def normalize_email(column: "Column") -> "Column":
    """Lowercase, trim and repair near-miss corporate domains.

    The directory contains four spellings of the corporate domain. Emails are
    the join key between memberships, teams and people, so leaving the variants
    in place silently orphans thousands of memberships.
    """
    from pyspark.sql import functions as F

    email = F.lower(clean_string(column))
    local_part = F.split(email, "@").getItem(0)
    domain = F.split(email, "@").getItem(1)

    repaired_domain = F.when(
        domain.isin(list(config.KNOWN_DOMAIN_TYPOS)),
        F.lit(config.CANONICAL_EMAIL_DOMAIN),
    ).otherwise(domain)

    return F.when(email.isNull() | ~email.contains("@"), None).otherwise(
        F.concat_ws("@", local_part, repaired_domain)
    )


def email_domain(column: "Column") -> "Column":
    """Return the domain part of an email address."""
    from pyspark.sql import functions as F

    return F.split(clean_string(column), "@").getItem(1)


def is_valid_email(column: "Column") -> "Column":
    """True when the value looks like a single well formed address."""
    from pyspark.sql import functions as F

    return clean_string(column).rlike(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------


def parse_date(column: "Column") -> "Column":
    """Parse a date written in any of the formats found across the sources.

    Each candidate pattern is tried in turn and the first that succeeds wins.
    An unparseable value becomes null rather than raising, which is what lets
    validation catch it and route the row to quarantine with a reason.
    """
    from pyspark.sql import functions as F

    value = clean_string(column)
    attempts = [F.to_date(value, fmt) for fmt in config.SPARK_DATE_FORMATS]
    return F.coalesce(*attempts)


def parse_month(column: "Column") -> "Column":
    """Parse a reporting month into the first day of that month."""
    from pyspark.sql import functions as F

    value = clean_string(column)
    attempts = [F.to_date(value, fmt) for fmt in config.SPARK_MONTH_FORMATS]
    return F.coalesce(*attempts)


def month_key(column: "Column") -> "Column":
    """Return a canonical ``YYYY-MM`` string for a parsed month."""
    from pyspark.sql import functions as F

    return F.date_format(parse_month(column), "yyyy-MM")


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


def parse_int(column: "Column") -> "Column":
    """Cast to integer, yielding null when the value is not numeric."""
    from pyspark.sql import functions as F

    return F.when(
        clean_string(column).rlike(r"^-?\d+$"), clean_string(column).cast("int")
    ).otherwise(None)


def parse_double(column: "Column") -> "Column":
    """Cast to double, yielding null when the value is not numeric."""
    from pyspark.sql import functions as F

    return F.when(
        clean_string(column).rlike(r"^-?\d+(\.\d+)?$"), clean_string(column).cast("double")
    ).otherwise(None)


def parse_impact_score(column: "Column") -> "Column":
    """Resolve the mixed-type impact score into a single numeric column.

    The achievements feed stores this field as a float, as null, or as one of
    the labels Low / Medium / High. Labels are mapped onto the midpoint of their
    band so that averages remain meaningful; the original form is preserved
    separately by :func:`impact_source_type` for auditability.
    """
    from pyspark.sql import functions as F

    numeric = parse_double(column)
    label = F.lower(clean_string(column))

    banded = F.lit(None).cast("double")
    for name, score in config.IMPACT_BAND_SCORES.items():
        banded = F.when(label == name, F.lit(score)).otherwise(banded)

    return F.coalesce(numeric, banded)


def impact_source_type(column: "Column") -> "Column":
    """Record whether the impact score arrived numeric, banded or missing."""
    from pyspark.sql import functions as F

    label = F.lower(clean_string(column))
    return (
        F.when(parse_double(column).isNotNull(), F.lit("NUMERIC"))
        .when(label.isin(list(config.IMPACT_BAND_SCORES)), F.lit("BAND"))
        .when(clean_string(column).isNull(), F.lit("MISSING"))
        .otherwise(F.lit("UNPARSEABLE"))
    )


# ---------------------------------------------------------------------------
# Categorical conformance
# ---------------------------------------------------------------------------

#: Six spellings of the same employment type appear in the directory export.
EMPLOYMENT_TYPE_MAP: dict[str, str] = {
    "full-time": "FULL_TIME",
    "full time": "FULL_TIME",
    "fulltime": "FULL_TIME",
    "ft": "FULL_TIME",
    "fte": "FULL_TIME",
    "employee": "FULL_TIME",
    "part-time": "PART_TIME",
    "part time": "PART_TIME",
    "pt": "PART_TIME",
}

#: Seven spellings of contractor engagements appear in the vendor export.
ENGAGEMENT_TYPE_MAP: dict[str, str] = {
    "c2c": "CONTRACTOR",
    "1099": "CONTRACTOR",
    "temp": "CONTRACTOR",
    "vendor": "CONTRACTOR",
    "consultant": "CONTRACTOR",
    "contractor": "CONTRACTOR",
}

#: Membership roles collapse to leader versus member for ratio calculations.
ROLE_MAP: dict[str, str] = {
    "team lead": "TEAM_LEAD",
    "team leader": "TEAM_LEAD",
    "lead": "TEAM_LEAD",
    "member": "MEMBER",
    "contributor": "MEMBER",
    "individual contributor": "MEMBER",
    "sme": "MEMBER",
}

STATUS_MAP: dict[str, str] = {
    "active": "ACTIVE",
    "terminated": "TERMINATED",
    "ended": "ENDED",
    "inactive": "INACTIVE",
}


def map_values(column: "Column", mapping: dict[str, str], default: str = "UNKNOWN") -> "Column":
    """Fold a free-text categorical column onto a canonical vocabulary.

    Values outside the mapping become ``default`` rather than null, so that
    unexpected new values are visible in aggregates instead of disappearing.
    """
    from pyspark.sql import functions as F

    key = F.lower(clean_string(column))
    result = F.lit(default)
    for source_value, canonical in mapping.items():
        result = F.when(key == source_value, F.lit(canonical)).otherwise(result)
    return F.when(key.isNull(), None).otherwise(result)


def normalize_employment_type(column: "Column") -> "Column":
    """Conform the employment type vocabulary."""
    return map_values(column, EMPLOYMENT_TYPE_MAP)


def normalize_engagement_type(column: "Column") -> "Column":
    """Conform the contractor engagement vocabulary."""
    return map_values(column, ENGAGEMENT_TYPE_MAP)


def normalize_role(column: "Column") -> "Column":
    """Conform the membership role vocabulary."""
    return map_values(column, ROLE_MAP)


def normalize_status(column: "Column") -> "Column":
    """Conform the status vocabulary."""
    return map_values(column, STATUS_MAP)


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


def surrogate_key(*columns: "Column") -> "Column":
    """Build a deterministic surrogate key from the supplied columns.

    Determinism matters: the achievements feed has no natural key, and a random
    or monotonic id would produce different values on every run, breaking
    idempotency. Hashing the payload means a rerun reproduces the same keys.
    """
    from pyspark.sql import functions as F

    parts = [F.coalesce(c.cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("||", *parts), 256)

def normalize_team_id(column: "Column") -> "Column":
    """Fold the three spellings of a team identifier onto one canonical form.

    The membership export writes the same identifier three ways: ``TM-001``,
    ``tm-126`` and ``tm685``. Left alone, roughly a fifth of all memberships
    fail a format check and drop out of the roster, which silently removes real
    people from real teams and corrupts every headcount that follows.

    The canonical form matches the team master data: ``TM-`` followed by the
    number, zero padded to at least three digits.
    """
    from pyspark.sql import functions as F

    value = F.upper(clean_string(column))
    digits = F.regexp_extract(value, r"^TM-?0*([0-9]+)$", 1)
    return F.when(digits == "", None).otherwise(
        F.concat(
            F.lit("TM-"),
            # Spark's lpad truncates when the value is already longer than
            # the target width, so pad only the short ids.
            F.when(F.length(digits) >= 3, digits).otherwise(F.lpad(digits, 3, "0")),
        )
    )
