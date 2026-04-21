-- Canonical schema bootstrap.
-- Apply with psql from repo root:
--   psql "$SUPABASE_DB_URL" -f database/schema.sql

\i database/migrations/001_initial_schema.sql
\i database/migrations/002_ahrefs_tables.sql
\i database/migrations/003_content_gap.sql
\i database/migrations/004_indexes_and_constraints.sql
\i database/migrations/005_rls_policies.sql
\i database/migrations/006_lost_backlinks.sql
\i database/migrations/007_internal_linking_suggestions.sql
\i database/migrations/008_backlink_validation.sql
\i database/migrations/009_internal_linking_ai_fields.sql
\i database/migrations/010_daily_content_opportunities.sql
\i database/migrations/011_arvow_automation_v3.sql
