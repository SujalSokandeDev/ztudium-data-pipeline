# Deployment

## Production Checklist

1. Rotate all sensitive keys if they were ever exposed.
2. Set GitHub Actions secrets:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `GOOGLE_CREDENTIALS_JSON`
- all `GSC_PROPERTY_*` and `GA4_PROPERTY_*`
3. Apply DB migrations (001-005).
4. Validate RLS policies and anon read access for dashboard tables.
5. Run both workflows manually once and confirm writes.

## Database Migration Commands

```bash
cd "d:\Ztudium\Data Consolidation\ztudium-data-pipeline"
set SUPABASE_DB_URL=postgresql://postgres:<password>@<host>:5432/postgres
python database/run_migrations.py
```

## Optional Clean Reset

```bash
psql "$SUPABASE_DB_URL" -f database/reset_production_schema.sql
```

## Rollback Approach

- Revert application code to previous git commit.
- Restore Supabase from a pre-migration backup/snapshot.
- Re-run workflows after restore.

## Monitoring

- GitHub Actions run status and durations.
- `ingestion_runs` table for status, failed websites, and error payload.
- Supabase API logs for rate-limit or schema errors.

## Frontend Refresh Trigger Config

Set these in frontend deployment environment:

- `GITHUB_ACTIONS_PAT`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_REF` (default `main`)
- `GITHUB_GOOGLE_WORKFLOW` (default `daily-google-fetch.yml`)
- `GITHUB_AHREFS_WORKFLOW` (default `process-ahrefs.yml`)

## Security Hardening

- Keep `service_role` key server-side only.
- Use fine-grained GitHub PAT scoped to one repo and Actions write.
- Restrict dashboard read access via RLS policies.
- Periodically rotate Supabase and GitHub tokens.
