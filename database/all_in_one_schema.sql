-- AUTO-GENERATED: Combined migration script
-- Run in Supabase SQL Editor as one script


-- ===== BEGIN 001_initial_schema.sql =====

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'success', 'partial', 'failed')),
    websites_attempted TEXT[] NOT NULL DEFAULT '{}',
    websites_succeeded TEXT[] NOT NULL DEFAULT '{}',
    websites_failed TEXT[] NOT NULL DEFAULT '{}',
    error_details JSONB,
    duration_seconds INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    gsc_clicks BIGINT,
    gsc_impressions BIGINT,
    gsc_ctr NUMERIC(10, 2),
    gsc_position NUMERIC(10, 2),
    ga_sessions BIGINT,
    ga_users BIGINT,
    ga_organic_sessions BIGINT,
    ga_organic_users BIGINT,
    ga_engagement_time NUMERIC(12, 2),
    ga_bounce_rate NUMERIC(10, 2),
    domain_rating NUMERIC(8, 2),
    ahrefs_traffic BIGINT,
    ahrefs_keywords BIGINT,
    ref_domains_total BIGINT,
    data_source TEXT NOT NULL DEFAULT 'api',
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_date_website UNIQUE (date, website)
);

CREATE TABLE IF NOT EXISTS website_keywords (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    keyword TEXT NOT NULL,
    clicks BIGINT,
    impressions BIGINT,
    ctr NUMERIC(10, 2),
    position NUMERIC(10, 2),
    search_volume BIGINT,
    keyword_difficulty NUMERIC(10, 2),
    traffic_estimate BIGINT,
    source TEXT NOT NULL DEFAULT 'gsc',
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_keyword_snapshot UNIQUE (date, website, keyword, source)
);

CREATE TABLE IF NOT EXISTS website_pages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    url TEXT NOT NULL,
    clicks BIGINT,
    impressions BIGINT,
    ctr NUMERIC(10, 2),
    position NUMERIC(10, 2),
    ga_sessions BIGINT,
    ga_pageviews BIGINT,
    traffic_ahrefs BIGINT,
    keywords_count BIGINT,
    top_keyword TEXT,
    source TEXT NOT NULL DEFAULT 'gsc',
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_page_snapshot UNIQUE (date, website, url, source)
);

CREATE TABLE IF NOT EXISTS calculated_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    day_over_day_pct NUMERIC(10, 2),
    week_over_week_pct NUMERIC(10, 2),
    month_over_month_pct NUMERIC(10, 2),
    seven_day_avg NUMERIC(14, 4),
    is_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    anomaly_description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_calculated_metric UNIQUE (date, website, metric_name)
);

COMMIT;

-- ===== END 001_initial_schema.sql =====


-- ===== BEGIN 002_ahrefs_tables.sql =====

BEGIN;

CREATE TABLE IF NOT EXISTS ahrefs_overview (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    domain TEXT,
    dr NUMERIC(8, 2),
    dr_delta NUMERIC(10, 2),
    ur NUMERIC(8, 2),
    ur_delta NUMERIC(10, 2),
    ahrefs_rank BIGINT,
    backlinks BIGINT,
    ref_domains BIGINT,
    ref_domains_delta BIGINT,
    organic_keywords BIGINT,
    top3_keywords BIGINT,
    organic_traffic BIGINT,
    organic_traffic_delta BIGINT,
    traffic_value BIGINT,
    ai_overview BIGINT,
    ai_chatgpt BIGINT,
    ai_perplexity BIGINT,
    ai_gemini BIGINT,
    ai_copilot BIGINT,
    intent_data JSONB,
    traffic_locations JSONB,
    source_file TEXT,
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_overview_snapshot UNIQUE (date, website)
);

CREATE TABLE IF NOT EXISTS ahrefs_referring_domains (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    domain TEXT NOT NULL,
    dr NUMERIC(8, 2),
    is_spam BOOLEAN NOT NULL DEFAULT FALSE,
    dofollow_links BIGINT,
    links_to_target BIGINT,
    first_seen TEXT,
    source_file TEXT,
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_referring_domain_snapshot UNIQUE (date, website, domain)
);

CREATE TABLE IF NOT EXISTS ahrefs_broken_backlinks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    referring_page TEXT NOT NULL,
    target_url TEXT,
    http_code INTEGER,
    target_http_code INTEGER,
    referring_page_http_code INTEGER,
    anchor_text TEXT,
    ref_domain_dr NUMERIC(8, 2),
    source_file TEXT,
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_broken_backlink_snapshot UNIQUE (date, website, referring_page)
);

CREATE TABLE IF NOT EXISTS ahrefs_competitors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    competitor_domain TEXT NOT NULL,
    keyword_overlap BIGINT,
    share_pct TEXT,
    competitor_keywords BIGINT,
    rank_order INTEGER,
    ingestion_run_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_competitor_snapshot UNIQUE (date, website, competitor_domain)
);

COMMIT;

-- ===== END 002_ahrefs_tables.sql =====


-- ===== BEGIN 003_content_gap.sql =====

BEGIN;

CREATE TABLE IF NOT EXISTS content_gap_keywords (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    website TEXT NOT NULL,
    keyword TEXT NOT NULL,
    volume BIGINT,
    kd NUMERIC(10, 2),
    cpc NUMERIC(10, 2),
    serp_features TEXT,
    intent TEXT,
    is_easy_win BOOLEAN NOT NULL DEFAULT FALSE,
    opportunity_score NUMERIC(10, 2),
    cluster TEXT,
    competitors JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_file TEXT,
    ingestion_run_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_content_gap_snapshot UNIQUE (date, website, keyword)
);

COMMIT;

-- ===== END 003_content_gap.sql =====


-- ===== BEGIN 004_indexes_and_constraints.sql =====

BEGIN;

ALTER TABLE ahrefs_overview ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE ahrefs_referring_domains ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE ahrefs_broken_backlinks ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE website_keywords ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE website_pages ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE ahrefs_overview ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE ahrefs_referring_domains ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE ahrefs_broken_backlinks ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE ahrefs_competitors ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;
ALTER TABLE content_gap_keywords ADD COLUMN IF NOT EXISTS ingestion_run_id UUID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_daily_metrics_ingestion_run'
    ) THEN
        ALTER TABLE daily_metrics
            ADD CONSTRAINT fk_daily_metrics_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_website_keywords_ingestion_run'
    ) THEN
        ALTER TABLE website_keywords
            ADD CONSTRAINT fk_website_keywords_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_website_pages_ingestion_run'
    ) THEN
        ALTER TABLE website_pages
            ADD CONSTRAINT fk_website_pages_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_ahrefs_overview_ingestion_run'
    ) THEN
        ALTER TABLE ahrefs_overview
            ADD CONSTRAINT fk_ahrefs_overview_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_ahrefs_ref_domains_ingestion_run'
    ) THEN
        ALTER TABLE ahrefs_referring_domains
            ADD CONSTRAINT fk_ahrefs_ref_domains_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_ahrefs_broken_backlinks_ingestion_run'
    ) THEN
        ALTER TABLE ahrefs_broken_backlinks
            ADD CONSTRAINT fk_ahrefs_broken_backlinks_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_ahrefs_competitors_ingestion_run'
    ) THEN
        ALTER TABLE ahrefs_competitors
            ADD CONSTRAINT fk_ahrefs_competitors_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_content_gap_keywords_ingestion_run'
    ) THEN
        ALTER TABLE content_gap_keywords
            ADD CONSTRAINT fk_content_gap_keywords_ingestion_run
            FOREIGN KEY (ingestion_run_id) REFERENCES ingestion_runs(id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_daily_metrics_website_date ON daily_metrics (website, date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics (date DESC);
CREATE INDEX IF NOT EXISTS idx_keywords_latest ON website_keywords (website, date DESC, clicks DESC);
CREATE INDEX IF NOT EXISTS idx_pages_latest ON website_pages (website, source, date DESC, clicks DESC);
CREATE INDEX IF NOT EXISTS idx_competitors_latest ON ahrefs_competitors (website, date DESC, keyword_overlap DESC);
CREATE INDEX IF NOT EXISTS idx_ref_domains_latest ON ahrefs_referring_domains (website, date DESC, dr DESC);
CREATE INDEX IF NOT EXISTS idx_broken_backlinks_latest ON ahrefs_broken_backlinks (website, date DESC, ref_domain_dr DESC);
CREATE INDEX IF NOT EXISTS idx_overview_latest ON ahrefs_overview (website, date DESC);
CREATE INDEX IF NOT EXISTS idx_content_gap_latest ON content_gap_keywords (website, date DESC, opportunity_score DESC);

COMMIT;

-- ===== END 004_indexes_and_constraints.sql =====


-- ===== BEGIN 005_rls_policies.sql =====

BEGIN;

ALTER TABLE daily_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE website_keywords ENABLE ROW LEVEL SECURITY;
ALTER TABLE website_pages ENABLE ROW LEVEL SECURITY;
ALTER TABLE calculated_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE ahrefs_overview ENABLE ROW LEVEL SECURITY;
ALTER TABLE ahrefs_referring_domains ENABLE ROW LEVEL SECURITY;
ALTER TABLE ahrefs_broken_backlinks ENABLE ROW LEVEL SECURITY;
ALTER TABLE ahrefs_competitors ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_gap_keywords ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_runs ENABLE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA public TO anon, authenticated;
GRANT SELECT ON TABLE
    daily_metrics,
    website_keywords,
    website_pages,
    calculated_metrics,
    ahrefs_overview,
    ahrefs_referring_domains,
    ahrefs_broken_backlinks,
    ahrefs_competitors,
    content_gap_keywords
TO anon, authenticated;

DROP POLICY IF EXISTS daily_metrics_select_policy ON daily_metrics;
CREATE POLICY daily_metrics_select_policy ON daily_metrics
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS website_keywords_select_policy ON website_keywords;
CREATE POLICY website_keywords_select_policy ON website_keywords
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS website_pages_select_policy ON website_pages;
CREATE POLICY website_pages_select_policy ON website_pages
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS calculated_metrics_select_policy ON calculated_metrics;
CREATE POLICY calculated_metrics_select_policy ON calculated_metrics
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS ahrefs_overview_select_policy ON ahrefs_overview;
CREATE POLICY ahrefs_overview_select_policy ON ahrefs_overview
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS ahrefs_ref_domains_select_policy ON ahrefs_referring_domains;
CREATE POLICY ahrefs_ref_domains_select_policy ON ahrefs_referring_domains
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS ahrefs_broken_backlinks_select_policy ON ahrefs_broken_backlinks;
CREATE POLICY ahrefs_broken_backlinks_select_policy ON ahrefs_broken_backlinks
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS ahrefs_competitors_select_policy ON ahrefs_competitors;
CREATE POLICY ahrefs_competitors_select_policy ON ahrefs_competitors
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS content_gap_keywords_select_policy ON content_gap_keywords;
CREATE POLICY content_gap_keywords_select_policy ON content_gap_keywords
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- Restrict ingestion run metadata to service role only.
DROP POLICY IF EXISTS ingestion_runs_service_role_policy ON ingestion_runs;
CREATE POLICY ingestion_runs_service_role_policy ON ingestion_runs
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

COMMIT;

-- ===== END 005_rls_policies.sql =====

