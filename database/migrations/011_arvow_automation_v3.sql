BEGIN;

ALTER TABLE IF EXISTS daily_content_opportunities
    ADD COLUMN IF NOT EXISTS arvow_batch_id TEXT,
    ADD COLUMN IF NOT EXISTS arvow_job_id TEXT,
    ADD COLUMN IF NOT EXISTS sent_to_arvow_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS published_url TEXT,
    ADD COLUMN IF NOT EXISTS verified_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS verification_notes TEXT NOT NULL DEFAULT '';

ALTER TABLE IF EXISTS daily_content_opportunities
    ALTER COLUMN arvow_payload SET DEFAULT '{}'::jsonb;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'daily_content_opportunities_status_check'
    ) THEN
        ALTER TABLE daily_content_opportunities
            DROP CONSTRAINT daily_content_opportunities_status_check;
    END IF;
END $$;

UPDATE daily_content_opportunities
SET status = CASE
    WHEN status = 'generated' THEN 'sent_to_arvow'
    WHEN status = 'completed' THEN 'published'
    ELSE status
END
WHERE status IN ('generated', 'completed');

ALTER TABLE IF EXISTS daily_content_opportunities
    ADD CONSTRAINT daily_content_opportunities_status_check
    CHECK (
        status IN (
            'pending',
            'in_review',
            'approved',
            'queued_for_arvow',
            'sent_to_arvow',
            'verification_pending',
            'published',
            'verification_failed',
            'ignored'
        )
    );

CREATE INDEX IF NOT EXISTS idx_daily_content_opportunities_site_status_v3
    ON daily_content_opportunities (site, status, priority_score DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_daily_content_opportunities_sent_at
    ON daily_content_opportunities (site, sent_to_arvow_at DESC);

CREATE TABLE IF NOT EXISTS arvow_publish_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site TEXT NOT NULL,
    opportunity_id UUID REFERENCES daily_content_opportunities (id) ON DELETE SET NULL,
    arvow_batch_id TEXT,
    arvow_response JSONB,
    published_url TEXT,
    status TEXT NOT NULL
        CHECK (
            status IN (
                'queued_for_arvow',
                'sent_to_arvow',
                'verification_pending',
                'published',
                'verification_failed',
                'ignored'
            )
        ),
    error_message TEXT,
    sent_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_arvow_publish_history_site_created
    ON arvow_publish_history (site, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_arvow_publish_history_opportunity
    ON arvow_publish_history (opportunity_id, created_at DESC);

COMMIT;
