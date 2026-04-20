BEGIN;

CREATE TABLE IF NOT EXISTS daily_content_opportunities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site TEXT NOT NULL,
    title TEXT NOT NULL,
    primary_keyword TEXT NOT NULL,
    cluster_id TEXT,
    reasoning TEXT NOT NULL,
    priority_score INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_review', 'approved', 'generated', 'completed', 'ignored')),
    arvow_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_date DATE NOT NULL DEFAULT CURRENT_DATE,
    CONSTRAINT uq_daily_content_opportunities_daily UNIQUE (site, title, primary_keyword, generated_date)
);

CREATE TABLE IF NOT EXISTS content_generation_history (
    id UUID PRIMARY KEY,
    site TEXT NOT NULL,
    title TEXT NOT NULL,
    primary_keyword TEXT NOT NULL,
    cluster_id TEXT,
    reasoning TEXT NOT NULL,
    priority_score INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'completed'
        CHECK (status IN ('pending', 'in_review', 'approved', 'generated', 'completed', 'ignored')),
    arvow_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    generated_date DATE NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action_taken TEXT NOT NULL
        CHECK (action_taken IN ('completed', 'ignored'))
);

CREATE INDEX IF NOT EXISTS idx_daily_content_opportunities_site_status
    ON daily_content_opportunities (site, status, priority_score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_daily_content_opportunities_generated_date
    ON daily_content_opportunities (generated_date DESC, site);

CREATE INDEX IF NOT EXISTS idx_content_generation_history_site_completed
    ON content_generation_history (site, completed_at DESC);

CREATE INDEX IF NOT EXISTS idx_content_generation_history_generated_date
    ON content_generation_history (generated_date DESC, site);

ALTER TABLE daily_content_opportunities ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_generation_history ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS daily_content_opportunities_select_policy ON daily_content_opportunities;
CREATE POLICY daily_content_opportunities_select_policy ON daily_content_opportunities
    FOR SELECT TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS daily_content_opportunities_service_write_policy ON daily_content_opportunities;
CREATE POLICY daily_content_opportunities_service_write_policy ON daily_content_opportunities
    FOR ALL TO service_role
    USING (true)
    WITH CHECK (true);

DROP POLICY IF EXISTS content_generation_history_select_policy ON content_generation_history;
CREATE POLICY content_generation_history_select_policy ON content_generation_history
    FOR SELECT TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS content_generation_history_service_write_policy ON content_generation_history;
CREATE POLICY content_generation_history_service_write_policy ON content_generation_history
    FOR ALL TO service_role
    USING (true)
    WITH CHECK (true);

COMMIT;
