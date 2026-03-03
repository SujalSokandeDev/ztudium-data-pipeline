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
