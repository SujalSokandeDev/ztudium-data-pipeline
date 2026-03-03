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
