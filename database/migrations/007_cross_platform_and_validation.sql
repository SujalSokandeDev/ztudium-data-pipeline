BEGIN;

ALTER TABLE internal_linking_suggestions
    ADD COLUMN IF NOT EXISTS suggestion_scope TEXT DEFAULT 'within_site',
    ADD COLUMN IF NOT EXISTS source_website TEXT,
    ADD COLUMN IF NOT EXISTS target_website TEXT;

UPDATE internal_linking_suggestions
SET
    suggestion_scope = COALESCE(suggestion_scope, 'within_site'),
    source_website = COALESCE(source_website, website),
    target_website = COALESCE(target_website, website)
WHERE suggestion_scope IS NULL
   OR source_website IS NULL
   OR target_website IS NULL;

CREATE INDEX IF NOT EXISTS idx_internal_linking_suggestions_scope_score
    ON internal_linking_suggestions (suggestion_scope, website, score DESC, status);

CREATE UNIQUE INDEX IF NOT EXISTS uq_internal_linking_suggestions_scope
    ON internal_linking_suggestions (
        suggestion_scope,
        source_website,
        source_page,
        target_website,
        target_page,
        target_page_keyword
    );

ALTER TABLE ahrefs_broken_backlinks
    ADD COLUMN IF NOT EXISTS validation_status TEXT DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS last_validated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS validation_notes TEXT;

ALTER TABLE ahrefs_lost_backlinks
    ADD COLUMN IF NOT EXISTS validation_status TEXT DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS last_validated_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS validation_notes TEXT;

UPDATE ahrefs_broken_backlinks
SET validation_status = 'pending'
WHERE validation_status IS NULL;

UPDATE ahrefs_lost_backlinks
SET validation_status = 'pending'
WHERE validation_status IS NULL;

CREATE INDEX IF NOT EXISTS idx_broken_backlinks_validation_status
    ON ahrefs_broken_backlinks (validation_status, website, date DESC);

CREATE INDEX IF NOT EXISTS idx_lost_backlinks_validation_status
    ON ahrefs_lost_backlinks (validation_status, website, lost_date DESC);

COMMIT;
