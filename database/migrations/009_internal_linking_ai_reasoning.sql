BEGIN;

ALTER TABLE internal_linking_suggestions
    ADD COLUMN IF NOT EXISTS reason TEXT,
    ADD COLUMN IF NOT EXISTS ai_confidence INTEGER;

COMMIT;
