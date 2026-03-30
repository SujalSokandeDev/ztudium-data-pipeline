BEGIN;

CREATE TABLE IF NOT EXISTS ahrefs_lost_backlinks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    website TEXT NOT NULL,
    referring_page_url TEXT NOT NULL,
    domain_rating INTEGER,
    target_url TEXT NOT NULL,
    anchor TEXT,
    first_seen DATE,
    last_seen DATE,
    lost_date DATE NOT NULL,
    drop_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_lost_backlink UNIQUE (website, referring_page_url, target_url, lost_date)
);

CREATE TABLE IF NOT EXISTS internal_linking_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    website TEXT NOT NULL,
    source_page TEXT NOT NULL,
    source_page_traffic INTEGER,
    target_page TEXT NOT NULL,
    target_page_keyword TEXT NOT NULL,
    target_page_position INTEGER,
    target_page_volume INTEGER,
    suggested_anchor TEXT,
    existing_link BOOLEAN NOT NULL DEFAULT FALSE,
    score INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'accepted', 'ignored')),
    CONSTRAINT unique_internal_link_suggestion
        UNIQUE (website, source_page, target_page, target_page_keyword)
);

CREATE INDEX IF NOT EXISTS idx_lost_backlinks_website_lost_date
    ON ahrefs_lost_backlinks (website, lost_date DESC, domain_rating DESC);

CREATE INDEX IF NOT EXISTS idx_internal_linking_suggestions_website_score
    ON internal_linking_suggestions (website, score DESC, status);

ALTER TABLE ahrefs_lost_backlinks ENABLE ROW LEVEL SECURITY;
ALTER TABLE internal_linking_suggestions ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON TABLE
    ahrefs_lost_backlinks,
    internal_linking_suggestions
TO anon, authenticated;

DROP POLICY IF EXISTS ahrefs_lost_backlinks_select_policy ON ahrefs_lost_backlinks;
CREATE POLICY ahrefs_lost_backlinks_select_policy ON ahrefs_lost_backlinks
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS internal_linking_suggestions_select_policy ON internal_linking_suggestions;
CREATE POLICY internal_linking_suggestions_select_policy ON internal_linking_suggestions
    FOR SELECT
    TO anon, authenticated
    USING (true);

COMMIT;
