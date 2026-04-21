# Deployment Runbook

Scope: V3 Growth Engine + Arvow automation deployment for `ztudium-data-pipeline` and the dashboard frontend.

Audience: developer / operations engineer deploying the daily content automation for `FreedomX` and `FashionABC`.

## Overview

This deployment enables:

- daily content opportunity generation
- automatic Arvow dispatch for `FreedomX` and `FashionABC`
- delayed publication verification
- dashboard monitoring through `/arvow`
- cluster sync after verified publication

Important current reality:

- `FreedomX` has already been verified end-to-end against a real live published article
- `FashionABC` still requires an Arvow-side integration check before production automation can be trusted

Do not skip the pre-deployment checklist.

## 1. Pre-Deployment Checklist

Complete all of these before pushing or enabling the workflow.

### Code And Repo Checklist

- Confirm `ztudium-data-pipeline` contains all V3 files:
  - `scripts/generate_daily_content_opportunities.py`
  - `scripts/auto_publish_arvow.py`
  - `scripts/verify_arvow_publish.py`
  - `.github/workflows/daily-content-opportunities.yml`
  - `.github/workflows/arvow-verification-followup.yml`
  - `database/migrations/011_arvow_automation_v3.sql`
- Confirm the dashboard frontend repo contains the matching UI/API changes:
  - `frontend/app/arvow/page.tsx`
  - `frontend/app/api/arvow/*`
  - `frontend/lib/growth-engine/*`
- Decide how SQL files under `data-consolidation-dashboard/database` will be versioned, because that folder is outside `data-consolidation-dashboard/frontend/.git`
- Confirm the Netlify-deployed frontend matches the code you intend to run

### External Dependency Checklist

- Confirm `ARVOW_INTEGRATION_ID_FREEDOMX` is correct
- Confirm `ARVOW_INTEGRATION_ID_FASHIONABC` is correct
- In Arvow, confirm `FashionABC` is:
  - connected to the correct destination
  - set to `Published`, not `Draft`
  - publishing as `Post` or `Page` as intended
  - not blocked by firewall / WordPress REST restrictions

### Security Checklist

- Apply the live RLS remediation SQL below
- Confirm `SUPABASE_SERVICE_KEY` is set only in server-side environments
- Confirm no local secret files are being committed
- Remove or secure local `backend/google-credentials.json` if that workspace is shared

### Frontend Checklist

- `npm run build` must pass in `data-consolidation-dashboard/frontend`
- Add and commit a real ESLint config if you want `npm run lint` to be a CI gate

## 2. Supabase SQL To Run Before Enabling Automation

Run these in Supabase SQL Editor.

### 2.1 RLS Remediation

This fixes the live security issue found in the audit.

```sql
begin;

drop policy if exists "Service write daily_insights" on daily_insights;
drop policy if exists "Service update daily_insights" on daily_insights;
create policy "Service write daily_insights"
on daily_insights
for insert
to service_role
with check (true);
create policy "Service update daily_insights"
on daily_insights
for update
to service_role
using (true);

drop policy if exists "Service write growth_engine_actions" on growth_engine_actions;
drop policy if exists "Service update growth_engine_actions" on growth_engine_actions;
create policy "Service write growth_engine_actions"
on growth_engine_actions
for insert
to service_role
with check (true);
create policy "Service update growth_engine_actions"
on growth_engine_actions
for update
to service_role
using (true);

commit;
```

### 2.2 V3 Automation Schema

If not already applied, run the V3 upgrade SQL from the dashboard SQL folder:

- `data-consolidation-dashboard/database/upgrade_arvow_automation_v3.sql`

This adds:

- `arvow_batch_id`
- `arvow_job_id`
- `sent_to_arvow_at`
- `published_url`
- `verified_at`
- `verification_notes`
- `arvow_publish_history`

### 2.3 Optional Cleanup SQL

If you want to clean old duplicate-style index overlap after the rollout is stable:

```sql
drop index if exists idx_daily_content_opportunities_site_status;
```

Only do this after confirming the V3 index is active and query performance is normal.

## 3. Environment Variables Setup

Use placeholders exactly like this and replace values before deployment.

### 3.1 Local Development - Pipeline `.env`

| Variable | Example Value |
|---|---|
| `SUPABASE_URL` | `https://your-project-ref.supabase.co` |
| `SUPABASE_SERVICE_KEY` | `your_supabase_service_role_key_here` |
| `SUPABASE_DB_URL` | `postgresql://postgres:your_password@db.your-project-ref.supabase.co:5432/postgres` |
| `OPENAI_API_KEY` | `your_openai_api_key_here` |
| `ARVOW_API_KEY` | `your_arvow_api_key_here` |
| `ARVOW_INTEGRATION_ID_FREEDOMX` | `your_freedomx_integration_id_here` |
| `ARVOW_INTEGRATION_ID_FASHIONABC` | `your_fashionabc_integration_id_here` |
| `GOOGLE_CREDENTIALS_JSON` | `your_google_service_account_json_here` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `./google-credentials.json` |
| `AHREFS_STORAGE_BUCKET` | `ahrefs-exports` |
| `KEYWORD_GAP_STORAGE_BUCKET` | `keyword_gap` |
| `ARVOW_STATUS_URL_TEMPLATE` | `https://api.arvow.com/api/v0.1/batch/{id}` |
| `ARVOW_VERIFY_MIN_DELAY_MINUTES` | `45` |
| `ARVOW_VERIFY_MAX_ATTEMPTS` | `4` |
| `ARVOW_VERIFY_ATTEMPT_WINDOWS` | `45,90,180,360` |
| `ARVOW_VERIFY_LOOKBACK_DAYS` | `7` |

Also set all required Google properties:

| Variable | Example Value |
|---|---|
| `GSC_PROPERTY_CITIESABC` | `sc-domain:citiesabc.com` |
| `GSC_PROPERTY_BUSINESSABC` | `sc-domain:businessabc.net` |
| `GSC_PROPERTY_HEDGETHINK` | `sc-domain:hedgethink.com` |
| `GSC_PROPERTY_FASHIONABC` | `sc-domain:fashionabc.org` |
| `GSC_PROPERTY_TRADERSDNA` | `sc-domain:tradersdna.com` |
| `GSC_PROPERTY_FREEDOMX` | `sc-domain:freedomx.com` |
| `GSC_PROPERTY_WISDOMIA` | `sc-domain:wisdomia.ai` |
| `GSC_PROPERTY_SPORTSDNA` | `sc-domain:sportsdna.ai` |
| `GSC_PROPERTY_INTELLIGENTHQ` | `sc-domain:intelligenthq.com` |
| `GA4_PROPERTY_CITIESABC` | `123456789` |
| `GA4_PROPERTY_BUSINESSABC` | `123456789` |
| `GA4_PROPERTY_HEDGETHINK` | `123456789` |
| `GA4_PROPERTY_FASHIONABC` | `123456789` |
| `GA4_PROPERTY_TRADERSDNA` | `123456789` |
| `GA4_PROPERTY_FREEDOMX` | `123456789` |
| `GA4_PROPERTY_WISDOMIA` | `123456789` |
| `GA4_PROPERTY_SPORTSDNA` | `123456789` |
| `GA4_PROPERTY_INTELLIGENTHQ` | `123456789` |

### 3.2 GitHub Actions Secrets

Add these in the `ztudium-data-pipeline` GitHub repo settings.

| Secret | Example Value |
|---|---|
| `SUPABASE_URL` | `https://your-project-ref.supabase.co` |
| `SUPABASE_SERVICE_KEY` | `your_supabase_service_role_key_here` |
| `OPENAI_API_KEY` | `your_openai_api_key_here` |
| `GOOGLE_CREDENTIALS_JSON` | `your_google_service_account_json_here` |
| `ARVOW_API_KEY` | `your_arvow_api_key_here` |
| `ARVOW_INTEGRATION_ID_FREEDOMX` | `your_freedomx_integration_id_here` |
| `ARVOW_INTEGRATION_ID_FASHIONABC` | `your_fashionabc_integration_id_here` |
| `GSC_PROPERTY_CITIESABC` | `sc-domain:citiesabc.com` |
| `GSC_PROPERTY_BUSINESSABC` | `sc-domain:businessabc.net` |
| `GSC_PROPERTY_HEDGETHINK` | `sc-domain:hedgethink.com` |
| `GSC_PROPERTY_FASHIONABC` | `sc-domain:fashionabc.org` |
| `GSC_PROPERTY_TRADERSDNA` | `sc-domain:tradersdna.com` |
| `GSC_PROPERTY_FREEDOMX` | `sc-domain:freedomx.com` |
| `GSC_PROPERTY_WISDOMIA` | `sc-domain:wisdomia.ai` |
| `GSC_PROPERTY_SPORTSDNA` | `sc-domain:sportsdna.ai` |
| `GSC_PROPERTY_INTELLIGENTHQ` | `sc-domain:intelligenthq.com` |
| `GA4_PROPERTY_CITIESABC` | `123456789` |
| `GA4_PROPERTY_BUSINESSABC` | `123456789` |
| `GA4_PROPERTY_HEDGETHINK` | `123456789` |
| `GA4_PROPERTY_FASHIONABC` | `123456789` |
| `GA4_PROPERTY_TRADERSDNA` | `123456789` |
| `GA4_PROPERTY_FREEDOMX` | `123456789` |
| `GA4_PROPERTY_WISDOMIA` | `123456789` |
| `GA4_PROPERTY_SPORTSDNA` | `123456789` |
| `GA4_PROPERTY_INTELLIGENTHQ` | `123456789` |

### 3.3 Netlify Environment Variables

Add these in the deployed dashboard site.

| Variable | Example Value |
|---|---|
| `NEXT_PUBLIC_SUPABASE_URL` | `https://your-project-ref.supabase.co` |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | `your_supabase_anon_key_here` |
| `SUPABASE_SERVICE_KEY` | `your_supabase_service_role_key_here` |
| `ARVOW_API_KEY` | `your_arvow_api_key_here` |
| `ARVOW_INTEGRATION_ID_FREEDOMX` | `your_freedomx_integration_id_here` |
| `ARVOW_INTEGRATION_ID_FASHIONABC` | `your_fashionabc_integration_id_here` |
| `GITHUB_ACTIONS_PAT` | `your_github_pat_here` |
| `GITHUB_OWNER` | `your_github_owner_here` |
| `GITHUB_REPO` | `ztudium-data-pipeline` |
| `GITHUB_REF` | `main` |
| `GITHUB_GOOGLE_WORKFLOW` | `daily-google-fetch.yml` |
| `GITHUB_AHREFS_WORKFLOW` | `process-ahrefs.yml` |
| `GITHUB_INTERNAL_LINKING_WORKFLOW` | `process-internal-linking.yml` |
| `GITHUB_KEYWORD_GAP_WORKFLOW` | `process-keyword-gap.yml` |

After updating Netlify env vars:

- trigger a redeploy

## 4. Database Migration Instructions

### Recommended Migration Order

Use this order for a clean environment:

1. `ztudium-data-pipeline/database/migrations/001_initial_schema.sql`
2. `ztudium-data-pipeline/database/migrations/002_ahrefs_tables.sql`
3. `ztudium-data-pipeline/database/migrations/003_content_gap.sql`
4. `ztudium-data-pipeline/database/migrations/004_indexes_and_constraints.sql`
5. `ztudium-data-pipeline/database/migrations/005_rls_policies.sql`
6. `ztudium-data-pipeline/database/migrations/006_lost_backlinks_and_internal_linking.sql`
7. `ztudium-data-pipeline/database/migrations/007_cross_platform_and_validation.sql`
8. `ztudium-data-pipeline/database/migrations/008_backlink_snapshot_cleanup_and_status_codes.sql`
9. `ztudium-data-pipeline/database/migrations/009_internal_linking_ai_reasoning.sql`
10. `ztudium-data-pipeline/database/migrations/010_daily_content_opportunities.sql`
11. `ztudium-data-pipeline/database/migrations/011_arvow_automation_v3.sql`

Dashboard-side operational SQL:

12. `data-consolidation-dashboard/database/add_daily_insights.sql`
13. `data-consolidation-dashboard/database/add_growth_engine_actions.sql`
14. `data-consolidation-dashboard/database/upgrade_arvow_automation_v3.sql`

### Migration Verification Queries

Run these after migration:

```sql
select table_name
from information_schema.tables
where table_schema = 'public'
  and table_name in (
    'daily_content_opportunities',
    'content_generation_history',
    'arvow_publish_history',
    'daily_insights',
    'growth_engine_actions'
  )
order by table_name;
```

```sql
select column_name
from information_schema.columns
where table_schema = 'public'
  and table_name = 'daily_content_opportunities'
order by ordinal_position;
```

```sql
select schemaname, tablename, policyname, roles, cmd
from pg_policies
where schemaname = 'public'
  and tablename in ('daily_insights', 'growth_engine_actions', 'daily_content_opportunities', 'arvow_publish_history')
order by tablename, policyname;
```

Expected checks:

- `daily_content_opportunities` exists with V3 columns
- `arvow_publish_history` exists
- write policies for `daily_insights` and `growth_engine_actions` are restricted to `service_role`

## 5. Workflow Deployment Steps

### Step 1: Push Pipeline Repo

Push `ztudium-data-pipeline` only after:

- secrets are set
- SQL is applied
- FashionABC integration is checked

### Step 2: Push Dashboard Frontend Repo

Push `data-consolidation-dashboard/frontend` after:

- Netlify env vars are configured
- the deployed build has been tested locally with `npm run build`

### Step 3: Enable GitHub Actions

In GitHub:

1. open the `ztudium-data-pipeline` repo
2. go to `Actions`
3. confirm these workflows are present:
   - `Daily Google Fetch (GSC + GA4)`
   - `Process Ahrefs CSVs`
   - `Process Keyword Gap CSVs`
   - `Daily Content Opportunities And Arvow Automation`
   - `Arvow Verification Followup`

### Step 4: Trigger The First Manual Run

Recommended first rollout sequence:

1. manually run `Daily Google Fetch (GSC + GA4)`
2. if needed, manually run:
   - `Process Ahrefs CSVs`
   - `Process Keyword Gap CSVs`
3. manually run `Daily Content Opportunities And Arvow Automation`
4. later, manually run `Arvow Verification Followup`

### What To Watch In Logs

#### `Daily Content Opportunities And Arvow Automation`

Expected sequence:

1. secrets validation passes
2. opportunities are generated
3. `FreedomX` top 4 are queued/sent
4. `FashionABC` top 4 are queued/sent
5. workflow sleeps
6. verification starts

Look for:

- `sent '<title>' to Arvow`
- `verified publish for '<title>'`
- `verification pending`
- `verification failed`

#### `Arvow Verification Followup`

Look for:

- pending rows being rechecked
- status changes to `published`
- cluster sync messages if verification succeeds

## 6. Post-Deployment Verification

### 6.1 Check Pipeline Data

In Supabase:

```sql
select site, status, count(*) as rows
from daily_content_opportunities
group by site, status
order by site, status;
```

```sql
select site, status, count(*) as rows
from arvow_publish_history
group by site, status
order by site, status;
```

Confirm:

- `FreedomX` has rows in `sent_to_arvow`, `verification_pending`, or `published`
- `FashionABC` has the same if the integration is working

### 6.2 Check Dashboard UI

Open the deployed dashboard and verify:

- `/arvow` loads successfully
- sites render as accordions
- `FreedomX` and `FashionABC` show automation status instead of manual Generate buttons
- other sites still allow manual Generate
- history modal shows:
  - Arvow batch/job metadata
  - status
  - published URL if available

### 6.3 Check Live Publication

For `FreedomX`:

- verify new posts on:
  - `https://freedomx.com/wp-json/wp/v2/posts?per_page=5&_fields=link,date,title`
- verify sitemap entries on:
  - `https://freedomx.com/sitemap_index.xml`

For `FashionABC`:

- verify new posts on:
  - `https://fashionabc.org/wp-json/wp/v2/posts?per_page=5&_fields=link,date,title`
- verify sitemap entries on:
  - `https://fashionabc.org/sitemap_index.xml`

### 6.4 Check Published History

Run:

```sql
select site, title, status, published_url, verified_at
from daily_content_opportunities
where status = 'published'
order by verified_at desc nulls last;
```

Success criteria:

- published rows exist
- `published_url` is populated
- `/arvow` history reflects those rows correctly

## 7. Rollback Plan

### Option A: Stop Automation Only

Fastest rollback.

1. Disable GitHub Actions workflows:
   - `Daily Content Opportunities And Arvow Automation`
   - `Arvow Verification Followup`
2. Optionally set these integrations aside by removing secrets:
   - `ARVOW_INTEGRATION_ID_FREEDOMX`
   - `ARVOW_INTEGRATION_ID_FASHIONABC`
3. Keep queue generation but stop dispatch/verification

### Option B: Revert Auto-Publish Behavior In Data

If rows are stuck mid-flight:

```sql
update daily_content_opportunities
set status = 'ignored',
    verification_notes = 'Manually disabled during rollback.',
    updated_at = now()
where status in ('queued_for_arvow', 'sent_to_arvow', 'verification_pending');
```

### Option C: Revert Schema Changes

Use only if necessary.

```sql
begin;

drop table if exists arvow_publish_history;

alter table if exists daily_content_opportunities
    drop column if exists arvow_batch_id,
    drop column if exists arvow_job_id,
    drop column if exists sent_to_arvow_at,
    drop column if exists published_url,
    drop column if exists verified_at,
    drop column if exists verification_notes;

commit;
```

Only do this if you are abandoning V3. Otherwise prefer disabling workflows and leaving schema intact.

## 8. Troubleshooting

### Problem: Verification stays pending forever

Likely causes:

- article is still draft-only in Arvow / WordPress
- wrong integration destination
- sitemap updates are delayed
- WordPress REST is blocked
- `ARVOW_STATUS_URL_TEMPLATE` is not set or not correct

What to do:

1. check `arvow_publish_history`
2. inspect the Arvow integration directly
3. test WordPress REST manually
4. run:

```powershell
cd "D:\Ztudium\Data Consolidation\ztudium-data-pipeline"
python scripts/verify_arvow_publish.py --site FashionABC --force-verify --debug
```

### Problem: Arvow accepts payload but no article goes live

Likely causes:

- integration publishes to draft
- integration points to another destination
- destination requires manual approval
- CMS API / WordPress publish permission issue

What to do:

1. confirm integration in Arvow
2. verify publish mode
3. verify destination site and credentials
4. compare `FreedomX` and `FashionABC` integration settings side by side

### Problem: Dashboard can read queue but cannot update actions

Likely causes:

- missing `SUPABASE_SERVICE_KEY` in Netlify
- RLS write policy problem

What to do:

1. confirm Netlify env vars
2. re-run the RLS remediation SQL
3. redeploy Netlify

### Problem: Workflow fails immediately on secrets validation

Likely causes:

- missing GitHub Actions secret
- secret name mismatch

What to do:

1. open GitHub repo settings -> secrets
2. compare against the environment table in this runbook
3. re-run workflow after adding the missing value

### Problem: `npm run lint` is interactive

Likely cause:

- ESLint config is not committed in the frontend repo

What to do:

1. create a real ESLint config
2. commit it
3. rerun `npm run lint`

### Problem: Old rows still have missing Arvow metadata

Likely cause:

- they were created before the full V3 state model was in place

What to do:

- backfill manually if you care about audit consistency
- or archive those rows and focus on new V3-generated data

## 9. Recommended First Live Rollout

If you want tomorrow morning's automation to run safely, this is the safest sequence:

1. apply the RLS SQL fix
2. verify `FashionABC` integration in Arvow
3. ensure GitHub and Netlify secrets are present
4. push `ztudium-data-pipeline`
5. push `data-consolidation-dashboard/frontend`
6. redeploy Netlify
7. manually run the daily content workflow once
8. monitor `FreedomX`
9. monitor `FashionABC`
10. only leave the scheduled run enabled after both sites behave as expected

## 10. Final Operational Note

As of this runbook:

- `FreedomX` is the known-good automation site
- `FashionABC` is the known-risk site until Arvow-side publishing is confirmed

Treat `FreedomX` as the baseline health signal for the system. If `FreedomX` sends and verifies but `FashionABC` does not, the problem is almost certainly outside the repo code.
