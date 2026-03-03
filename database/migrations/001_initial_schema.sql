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
