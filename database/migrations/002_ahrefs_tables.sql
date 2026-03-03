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
