-- Canonical schema bootstrap.
-- Apply with psql from repo root:
--   psql "$SUPABASE_DB_URL" -f database/schema.sql

\i database/migrations/001_initial_schema.sql
\i database/migrations/002_ahrefs_tables.sql
\i database/migrations/003_content_gap.sql
\i database/migrations/004_indexes_and_constraints.sql
\i database/migrations/005_rls_policies.sql
