# Architecture

## Decision

Option A is implemented: **frontend reads Supabase directly** for dashboard data.

- No Flask data API in this repository.
- Python scripts are ingestion workers only.
- UI refresh controls trigger GitHub Actions workflow dispatch (server-side API in frontend app).

## Components

1. `scripts/fetch_google.py`
- Pulls GSC + GA4 daily data.
- Upserts into `daily_metrics`, `website_keywords`, `website_pages`.
- Logs ingestion metadata into `ingestion_runs` when available.

2. `scripts/process_ahrefs.py`
- Downloads CSV/TXT exports from Supabase Storage.
- Parses overview, keywords, top pages, referring domains, broken backlinks, competitors.
- Upserts snapshot-scoped rows by `(date, website, ...)` keys.
- Uses source-file lineage fields when schema supports them.

3. `scripts/process_keyword_gap.py`
- Downloads keyword-gap CSVs from Supabase Storage bucket `keyword_gap`.
- Parses keyword opportunities and upserts into `content_gap_keywords`.
- Applies snapshot-date inserts from filename and dedupe by `(date, website, keyword)` before upsert.

4. Frontend (`SEO-Data-Consolidation-Dashboard`)
- Direct Supabase queries for all data views.
- Snapshot-scoped detail tabs to prevent cross-date duplicates.
- Date-range controls persist in URL query params.

## Data Integrity Rules

- Snapshot date is derived from file names and retained on insert.
- Upserts are conflict-based and idempotent for reruns.
- Latest-detail dashboard tabs query max snapshot date per website/table.

## Operational Flow

- Daily schedule: run Google fetch workflow.
- Weekly/manual: upload Ahrefs exports -> run Ahrefs process workflow.
- Weekly/manual: upload keyword-gap exports -> run keyword-gap process workflow.
- Monitoring: inspect `ingestion_runs` + workflow logs for partial/failed runs.
