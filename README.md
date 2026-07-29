# ACME Workforce Analytics — Backend Data Pipeline

A medallion-architecture ETL pipeline built during a Citi coding workshop (July 2026).

I worked on this project as a **backend / data engineering** participant. The pipeline
ingests seven operational exports from six source systems into a layered data lake and
produces nine gold tables answering organisational-structure and staffing questions.

> **Scope of my work:** backend and data engineering only. See
> [Attribution](#attribution) below for exactly which files are mine and which came
> from the workshop's starter scaffold. The `frontend/` directory is unmodified
> starter code and was not part of my task.

---

## Status

**Workshop complete — infrastructure decommissioned.**

This repository is the preserved source code. The AWS infrastructure it deployed to
was torn down at the end of the workshop, and the workshop environment no longer
exists. Nothing is currently running.

The pipeline still runs locally against Docker Compose (see
[Running locally](#running-locally)) provided source data is supplied.

---

## Architecture

```
raw ──► bronze ──► silver ──► gold
                └──► quarantine   (rejected records, retained per batch)
```

Every run emits a lineage manifest recording source path, target path and row counts
for each hop, written to `data/lineage/batch_id=<timestamp>-<hash>/`. Sample manifests
are committed under `docs/test-evidence/lineage-manifests/`.

### Sources → bronze

Ingested to date-partitioned Parquet (`ingest_date=YYYY-MM-DD`):

| Source system | Files |
| --- | --- |
| Employee directory | `employees.csv` |
| Vendor management | `contractor_roster.csv` |
| Facilities | `locations.csv` |
| Org structure | `organizations.json` |
| Project tracking | `teams.json`, `team_membership.csv` |
| Performance management | `monthly_achievements.json` |

### Bronze → silver

Schema validation, type coercion and deduplication produce eight conformed tables:

`employees` · `contractors` · `locations` · `organizations` · `teams` ·
`team_membership` · `achievements` · `dim_person`

`dim_person` is a unified person dimension merging employees and contractors into a
single identity, so downstream analytics can treat the whole workforce consistently.

Records failing validation are written to `quarantine/silver/<table>/batch_id=<id>/`
rather than dropped, keeping every rejection auditable per run.

### Silver → gold

| Table | Answers |
| --- | --- |
| `team_members` | Who the members of each team are |
| `team_locations` | Where each team is located |
| `monthly_team_achievements` | Key achievements per team, per month |
| `leader_not_colocated` | Teams whose leader is not co-located with their members |
| `leader_non_direct_staff` | Teams whose leader is non-direct staff |
| `staff_ratio_analysis` | Teams exceeding a 20% non-direct-staff to employee ratio |
| `organization_reporting_summary` | Teams reporting to an organization leader |
| `employee_summary` | Headcount and attribute rollup across the workforce |
| `business_answers` | Consolidated answers to the business questions |

Sample output charts are in [`docs/results/`](docs/results/).

---

## Stack

PySpark · Parquet · PostgreSQL · Terraform (S3, RDS, EKS, Lambda, CloudFront) ·
Helm · Docker Compose · pytest

The same `data_pipeline.main` entry point runs locally and on EKS. The cloud launcher
(`data/team-etl/job.py`) pulls the packaged pipeline from S3 and places the zip on
`sys.path`, so cloud execution changes configuration only — never code.

---

## Repository layout

```
backend/data_pipeline/
├── ingestion/          # Source readers → bronze
├── silver/             # bronze_to_silver: validate, conform, deduplicate
├── gold/               # silver_to_gold: joins and aggregations
├── validation/         # Schema definitions and the validator
├── database/           # PostgreSQL loader
├── utils/              # Spark session, transforms, IO, logging
├── notebooks/          # business_questions.ipynb
└── main.py             # Entry point
backend/tests/          # pytest suite for transforms and validation
data/team-etl/          # EKS entry point
infra/                  # Terraform + Helm chart
```

---

## Running locally

```bash
cp .env.local.sample .env.local
docker compose up -d

pip install -r backend/requirements.txt
python -m data_pipeline.main

pytest backend/tests
```

---

## Data

**The workshop dataset is not included in this repository.**

The pipeline reads source files from `RAW_DIR` (defaults to `data/raw/`). Expected
directory structure is documented in [`data/README.md`](data/README.md), and
column-level schemas and validation rules are defined in
`backend/data_pipeline/validation/schemas.py`.

---

## Attribution

The project scaffold comes from Citi's Apache-2.0 licensed coding-workshop starter
repository. To be explicit about what is and isn't my work:

**Written by me (backend / data engineering task):**

- `backend/data_pipeline/` — the full pipeline: ingestion, bronze→silver,
  silver→gold, validation, quarantine handling, lineage manifests, Postgres loader
- `backend/tests/` — pytest coverage for transforms and validation
- `data/team-etl/job.py` — EKS launcher
- `infra/eks.tf` and `infra/helm/` — cluster and job deployment

**From the starter scaffold (not my work):**

- `bin/` — setup, deployment and teardown scripts
- `infra/*.tf` other than `eks.tf` — S3, RDS, Lambda, CloudFront, DocumentDB
- `frontend/` — unmodified React starter; the frontend track was a different role
- `.github/` workflows and instruction files
- `docs/` role guides and `validation.md`

## Licence

Apache-2.0, inherited from the starter repository. See [LICENSE](LICENSE).
