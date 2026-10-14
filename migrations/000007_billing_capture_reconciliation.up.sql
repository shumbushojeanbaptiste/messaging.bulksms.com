ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS billing_reservation_id UUID,
    ADD COLUMN IF NOT EXISTS billing_capture_status VARCHAR(20) NOT NULL DEFAULT 'not_required',
    ADD COLUMN IF NOT EXISTS billing_capture_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS billing_capture_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE campaigns
    ADD COLUMN IF NOT EXISTS billing_reservation_id UUID,
    ADD COLUMN IF NOT EXISTS billing_capture_status VARCHAR(20) NOT NULL DEFAULT 'not_required',
    ADD COLUMN IF NOT EXISTS billing_capture_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS billing_capture_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'messages_billing_capture_status_check'
          AND conrelid = 'messages'::regclass
    ) THEN
        ALTER TABLE messages
            ADD CONSTRAINT messages_billing_capture_status_check
            CHECK (billing_capture_status IN (
                'not_required', 'pending', 'captured', 'release_pending', 'released'
            ));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'campaigns_billing_capture_status_check'
          AND conrelid = 'campaigns'::regclass
    ) THEN
        ALTER TABLE campaigns
            ADD CONSTRAINT campaigns_billing_capture_status_check
            CHECK (billing_capture_status IN (
                'not_required', 'pending', 'captured', 'release_pending', 'released'
            ));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_messages_billing_capture_pending
    ON messages (created_at)
    WHERE billing_reservation_id IS NOT NULL
      AND billing_capture_status = 'pending';

CREATE INDEX IF NOT EXISTS idx_campaigns_billing_capture_pending
    ON campaigns (created_at)
    WHERE billing_reservation_id IS NOT NULL
      AND billing_capture_status = 'pending';

CREATE INDEX IF NOT EXISTS idx_messages_billing_capture_due
    ON messages (billing_capture_next_attempt_at, created_at)
    WHERE billing_reservation_id IS NOT NULL
      AND billing_capture_status = 'pending';

CREATE INDEX IF NOT EXISTS idx_campaigns_billing_capture_due
    ON campaigns (billing_capture_next_attempt_at, created_at)
    WHERE billing_reservation_id IS NOT NULL
      AND billing_capture_status = 'pending';
