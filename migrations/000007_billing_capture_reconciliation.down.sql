DROP INDEX IF EXISTS idx_campaigns_billing_capture_due;
DROP INDEX IF EXISTS idx_messages_billing_capture_due;

ALTER TABLE campaigns
    DROP COLUMN IF EXISTS billing_capture_next_attempt_at,
    DROP COLUMN IF EXISTS billing_capture_attempts;

ALTER TABLE messages
    DROP COLUMN IF EXISTS billing_capture_next_attempt_at,
    DROP COLUMN IF EXISTS billing_capture_attempts;
