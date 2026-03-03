-- WARNING: destructive reset for production-like environments.
-- Run only when you intentionally want a clean rebuild.

BEGIN;

DROP TABLE IF EXISTS content_gap_keywords CASCADE;
DROP TABLE IF EXISTS ahrefs_competitors CASCADE;
DROP TABLE IF EXISTS ahrefs_broken_backlinks CASCADE;
DROP TABLE IF EXISTS ahrefs_referring_domains CASCADE;
DROP TABLE IF EXISTS ahrefs_overview CASCADE;
DROP TABLE IF EXISTS calculated_metrics CASCADE;
DROP TABLE IF EXISTS website_pages CASCADE;
DROP TABLE IF EXISTS website_keywords CASCADE;
DROP TABLE IF EXISTS daily_metrics CASCADE;
DROP TABLE IF EXISTS ingestion_runs CASCADE;

COMMIT;

\i database/migrations/001_initial_schema.sql
\i database/migrations/002_ahrefs_tables.sql
\i database/migrations/003_content_gap.sql
\i database/migrations/004_indexes_and_constraints.sql
\i database/migrations/005_rls_policies.sql
