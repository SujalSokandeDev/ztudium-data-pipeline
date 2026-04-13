BEGIN;

ALTER TABLE ahrefs_broken_backlinks
    ADD COLUMN IF NOT EXISTS target_http_code INTEGER,
    ADD COLUMN IF NOT EXISTS referring_page_http_code INTEGER;

UPDATE ahrefs_broken_backlinks
SET target_http_code = COALESCE(target_http_code, http_code)
WHERE target_http_code IS NULL
  AND http_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_broken_backlinks_latest_validation
    ON ahrefs_broken_backlinks (website, date DESC, validation_status, last_validated_at DESC);

CREATE INDEX IF NOT EXISTS idx_lost_backlinks_latest_validation
    ON ahrefs_lost_backlinks (website, lost_date DESC, validation_status, last_validated_at DESC);

COMMIT;
