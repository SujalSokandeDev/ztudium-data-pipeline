# Ztudium Data Pipeline

Automated ingestion for Google (GSC + GA4) and Ahrefs exports into Supabase.

## Architecture

- `daily-google-fetch.yml` runs `scripts/fetch_google.py` (API pull -> Supabase upsert).
- `process-ahrefs.yml` runs `scripts/process_ahrefs.py` (Storage files -> parse -> Supabase upsert).
- Frontend reads Supabase directly.
- Refresh buttons dispatch GitHub workflows through a server-side API route in the frontend app.

## Data Model And Migrations

Canonical SQL migrations live in [`database/migrations`](database/migrations):

1. `001_initial_schema.sql`
2. `002_ahrefs_tables.sql`
3. `003_content_gap.sql`
4. `004_indexes_and_constraints.sql`
5. `005_rls_policies.sql`

Helpers:

- `database/run_migrations.py` applies all migrations in order using `SUPABASE_DB_URL`.
- `database/schema.sql` includes all migrations.
- `database/reset_production_schema.sql` drops all pipeline tables and recreates schema.
- `database/seed_sample_data.sql` inserts sample smoke-test data.

### Run migrations

```bash
cd "d:\Ztudium\Data Consolidation\ztudium-data-pipeline"
set SUPABASE_DB_URL=postgresql://postgres:<password>@<host>:5432/postgres
python database/run_migrations.py
```

## Required Environment Variables

Local `.env` and/or GitHub Actions secrets:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `SUPABASE_DB_URL` (required for migration runner only)
- `GOOGLE_CREDENTIALS_JSON` (Actions secret) or `GOOGLE_APPLICATION_CREDENTIALS` (local file path)
- `GSC_PROPERTY_*` for all websites
- `GA4_PROPERTY_*` for all websites

## Workflows

### Daily Google Fetch

- Workflow: `.github/workflows/daily-google-fetch.yml`
- Timeout: 15 minutes
- Writes: `daily_metrics`, `website_keywords`, `website_pages`

### Ahrefs Processing

- Workflow: `.github/workflows/process-ahrefs.yml`
- Timeout: 60 minutes
- Writes: `ahrefs_overview`, `ahrefs_referring_domains`, `ahrefs_broken_backlinks`, `ahrefs_competitors`, plus Ahrefs snapshots into `website_keywords` and `website_pages`

## Reliability Controls

- Upserts use supabase-py compatible signatures (no `default_to_null`).
- Batch upsert has chunk retries and row-level fallback.
- Transient API/network errors use exponential backoff retries.
- Snapshot date is taken from source filenames and preserved for idempotent reruns.
- Ingestion run metadata is recorded in `ingestion_runs` when table is present.

## Security

- Credentials are read from environment variables only.
- `.env` and credential files are ignored in `.gitignore`.
- If a key was ever committed historically, rotate it in Supabase/GitHub immediately.

## Troubleshooting

- `403 User does not have sufficient permissions`: the service account is missing access on that GSC site or GA4 property.
- `Invalid property ID`: corresponding `GA4_PROPERTY_*` secret is empty or non-numeric.
- Ahrefs workflow timeout: verify migration 004 indexes exist; rerun with fewer files if needed.
- Missing latest data in dashboard: verify frontend is querying latest snapshot date (now enabled for Ahrefs detail tabs).
