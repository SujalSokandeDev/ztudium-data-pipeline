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
