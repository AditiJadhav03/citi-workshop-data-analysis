"""Validation contracts for every silver entity.

Each schema states three things explicitly: which columns must exist, what the
business key is, and which constraints a row must satisfy. Keeping these as data
in one file means the data quality story can be read end to end without opening
a single transformation job, and each rule can be unit tested on a handful of
synthetic rows.

Rules are written against the *normalised* column names produced by the silver
job, not against the raw source names.
"""

from __future__ import annotations

from .validator import EntitySchema, Rule

# ---------------------------------------------------------------------------
# People
# ---------------------------------------------------------------------------

EMPLOYEES = EntitySchema(
    entity="employees",
    description="Corporate directory. Direct staff of ACME Inc.",
    required_columns=("emp_id", "email", "location_code", "employment_type", "status"),
    business_key=("emp_id",),
    rules=(
        Rule("emp_id_required", "emp_id IS NOT NULL", "Primary key must be present", null_passes=False),
        Rule("emp_id_format", "emp_id RLIKE '^EMP-[0-9]+$'", "Employee ids follow EMP-nnnnn"),
        Rule("email_required", "email IS NOT NULL", "Email is the join key to memberships", null_passes=False),
        Rule(
            "email_format",
            "email RLIKE '^[^@ ]+@[^@ ]+[.][A-Za-z]{2,}$'",
            "Email must be well formed after domain repair",
        ),
        Rule("status_known", "status IN ('ACTIVE','TERMINATED')", "Status vocabulary is closed"),
        Rule("employment_type_known", "employment_type <> 'UNKNOWN'", "Employment type must map to the canonical vocabulary"),
        Rule("hire_date_parsed", "hire_date IS NOT NULL", "Hire date must parse in one of the known formats", severity="warning"),
        Rule("hire_date_not_future", "hire_date <= current_date()", "Hire date cannot be in the future"),
        Rule("location_present", "location_code IS NOT NULL", "Location is needed for co-location analysis", severity="warning"),
    ),
)

CONTRACTORS = EntitySchema(
    entity="contractors",
    description="Vendor roster. Non-direct staff engaged through agencies.",
    required_columns=("contractor_id", "email", "engagement_type", "status"),
    business_key=("contractor_id",),
    rules=(
        Rule("contractor_id_required", "contractor_id IS NOT NULL", "Primary key must be present", null_passes=False),
        Rule("contractor_id_format", "contractor_id RLIKE '^CTR-[0-9]+$'", "Contractor ids follow CTR-nnnn"),
        Rule("email_required", "email IS NOT NULL", "Email is the join key to memberships", null_passes=False),
        Rule(
            "email_format",
            "email RLIKE '^[^@ ]+@[^@ ]+[.][A-Za-z]{2,}$'",
            "Email must be well formed",
        ),
        Rule("engagement_type_known", "engagement_type = 'CONTRACTOR'", "All roster entries are contractor engagements"),
        Rule("status_known", "status IN ('ACTIVE','ENDED','TERMINATED','INACTIVE')", "Status vocabulary is closed"),
        Rule(
            "engagement_dates_ordered",
            "end_date IS NULL OR start_date IS NULL OR end_date >= start_date",
            "An engagement cannot end before it starts; flagged, not rejected, since the staff classification is still valid",
            severity="warning",
        ),
        Rule("location_present", "location_code IS NOT NULL", "Roster rows without a location cannot be placed", severity="warning"),
    ),
)

# ---------------------------------------------------------------------------
# Places and organisations
# ---------------------------------------------------------------------------

LOCATIONS = EntitySchema(
    entity="locations",
    description="Facilities reference data. One row per office location code.",
    required_columns=("location_code", "city", "country", "region", "timezone"),
    business_key=("location_code",),
    rules=(
        Rule("location_code_required", "location_code IS NOT NULL", "Primary key must be present", null_passes=False),
        Rule("location_code_format", "location_code RLIKE '^LOC-[0-9]+$'", "Location codes follow LOC-nn"),
        Rule("region_known", "region IN ('AMER','EMEA','APAC')", "Region vocabulary is closed"),
        Rule("city_required", "city IS NOT NULL", "City is needed for reporting", null_passes=False),
        Rule("timezone_required", "timezone IS NOT NULL", "Timezone is needed for co-location analysis", severity="warning"),
    ),
)

ORGANIZATIONS = EntitySchema(
    entity="organizations",
    description="Organisation hierarchy and their leaders.",
    required_columns=("org_id", "org_name", "org_leader_email"),
    business_key=("org_id",),
    rules=(
        Rule("org_id_required", "org_id IS NOT NULL", "Primary key must be present", null_passes=False),
        Rule("org_id_format", "org_id RLIKE '^ORG-[0-9]+$'", "Organisation ids follow ORG-nn"),
        Rule("org_name_required", "org_name IS NOT NULL", "Name is required", null_passes=False),
        Rule(
            "leader_email_format",
            "org_leader_email RLIKE '^[^@ ]+@[^@ ]+[.][A-Za-z]{2,}$'",
            "Org leader email drives question 7",
        ),
    ),
)

# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

TEAMS = EntitySchema(
    entity="teams",
    description="Team master data, flattened from the nested project tracking export.",
    required_columns=("team_id", "team_name", "team_leader_email", "primary_office", "reports_to_type"),
    business_key=("team_id",),
    rules=(
        Rule("team_id_required", "team_id IS NOT NULL", "Primary key must be present", null_passes=False),
        Rule("team_id_format", "team_id RLIKE '^TM-[0-9]+$'", "Team ids follow TM-nnn"),
        Rule("team_name_required", "team_name IS NOT NULL", "Name is required", null_passes=False),
        Rule(
            "leader_email_format",
            "team_leader_email RLIKE '^[^@ ]+@[^@ ]+[.][A-Za-z]{2,}$'",
            "Leader email drives questions 4 and 5",
        ),
        Rule("primary_office_required", "primary_office IS NOT NULL", "Primary office drives question 4", null_passes=False),
        Rule("org_id_present", "org_id IS NOT NULL", "Organisation link drives question 7", severity="warning"),
        Rule("formed_date_parsed", "formed_date IS NOT NULL", "Formation date must parse", severity="warning"),
        Rule("formed_date_not_future", "formed_date <= current_date()", "A team cannot be formed in the future"),
    ),
)

TEAM_MEMBERSHIP = EntitySchema(
    entity="team_membership",
    description="Team roster. One row per person per team assignment.",
    required_columns=("team_id", "employee_email", "role", "allocation_pct"),
    business_key=("team_id", "employee_email", "start_date"),
    rules=(
        Rule("team_id_required", "team_id IS NOT NULL", "Foreign key to teams", null_passes=False),
        Rule("team_id_format", "team_id RLIKE '^TM-[0-9]+$'", "Team ids follow TM-nnn"),
        Rule("email_required", "employee_email IS NOT NULL", "Foreign key to people", null_passes=False),
        Rule(
            "email_format",
            "employee_email RLIKE '^[^@ ]+@[^@ ]+[.][A-Za-z]{2,}$'",
            "Email must be well formed after trimming and domain repair",
        ),
        Rule("role_known", "role IN ('TEAM_LEAD','MEMBER')", "Role vocabulary is closed"),
        # Deliberately a warning, not an error. Roughly a quarter of the export
        # carries an allocation above 100, and rejecting those rows would remove
        # 60k people from their teams, breaking questions 1, 4 and 6. The value
        # is capped and flagged instead; the original is kept in
        # allocation_pct_raw so nothing is silently rewritten.
        Rule(
            "allocation_in_range",
            "allocation_pct_raw BETWEEN 0 AND 100",
            "Allocation is a percentage; values above 100 are capped and flagged",
            severity="warning",
        ),
        Rule("allocation_present", "allocation_pct IS NOT NULL", "Allocation must be numeric", severity="warning"),
        Rule(
            "membership_dates_ordered",
            "end_date IS NULL OR start_date IS NULL OR end_date >= start_date",
            "A membership cannot end before it starts; flagged, not rejected, since the person is still on the team",
            severity="warning",
        ),
        Rule("start_date_parsed", "start_date IS NOT NULL", "Start date must parse", severity="warning"),
    ),
)

# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------

ACHIEVEMENTS = EntitySchema(
    entity="achievements",
    description="Monthly team achievements. Keyed by a deterministic hash of the payload.",
    required_columns=("achievement_sk", "month_key", "team_id", "title", "category"),
    business_key=("achievement_sk",),
    # A third of this feed arrives with no team_id and cannot be attributed,
    # because team_name is not unique (200 distinct names across 25,000 teams).
    # That is a documented source defect, so this entity is allowed a higher
    # quarantine rate than the pipeline default without failing the run.
    max_quarantine_ratio=0.40,
    rules=(
        Rule("surrogate_key_required", "achievement_sk IS NOT NULL", "Deterministic key must be present", null_passes=False),
        Rule(
            "team_id_resolved",
            "team_id IS NOT NULL",
            "Achievements that cannot be attributed to a team are unusable for question 3",
            null_passes=False,
        ),
        Rule("team_id_format", "team_id RLIKE '^TM-[0-9]+$'", "Team ids follow TM-nnn"),
        Rule("month_parsed", "month_key RLIKE '^[0-9]{4}-[0-9]{2}$'", "Month must be a canonical YYYY-MM", null_passes=False),
        Rule("title_required", "title IS NOT NULL", "An achievement needs a description", null_passes=False),
        Rule("category_required", "category IS NOT NULL", "Category is used for breakdowns", severity="warning"),
        Rule(
            "impact_score_in_range",
            "impact_score BETWEEN 0 AND 10",
            "Impact scores are on a nought to ten scale",
        ),
        Rule(
            "impact_score_present",
            "impact_score IS NOT NULL",
            "Missing or unparseable impact scores are excluded from averages",
            severity="warning",
        ),
        Rule(
            "reporter_email_format",
            "reported_by RLIKE '^[^@ ]+@[^@ ]+[.][A-Za-z]{2,}$'",
            "Reporter email is used to resolve unattributed achievements",
            severity="warning",
        ),
    ),
)


#: Registry consumed by the silver job. Keys are silver entity names.
SCHEMAS: dict[str, EntitySchema] = {
    schema.entity: schema
    for schema in (
        EMPLOYEES,
        CONTRACTORS,
        LOCATIONS,
        ORGANIZATIONS,
        TEAMS,
        TEAM_MEMBERSHIP,
        ACHIEVEMENTS,
    )
}


def get_schema(entity: str) -> EntitySchema:
    """Look up the validation contract for a silver entity."""
    try:
        return SCHEMAS[entity]
    except KeyError as exc:
        raise KeyError(
            f"No schema registered for '{entity}'. Known: {sorted(SCHEMAS)}"
        ) from exc


def rule_catalogue() -> list[dict[str, str]]:
    """Return every rule as a flat list, for documentation and test coverage."""
    return [
        {
            "entity": schema.entity,
            "rule": rule.name,
            "severity": rule.severity,
            "expression": rule.expr,
            "description": rule.description,
        }
        for schema in SCHEMAS.values()
        for rule in schema.rules
    ]