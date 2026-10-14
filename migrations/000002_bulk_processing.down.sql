
DROP TABLE IF EXISTS outbox_events;

DROP INDEX IF EXISTS idx_messages_scheduled;
DROP INDEX IF EXISTS idx_messages_campaign_status;
DROP INDEX IF EXISTS idx_messages_global_idempotency;

ALTER TABLE messages
    DROP COLUMN IF EXISTS submission_started_at,
    DROP COLUMN IF EXISTS attempt_count,
    DROP COLUMN IF EXISTS scheduled_at,
    DROP COLUMN IF EXISTS request_hash,
    DROP COLUMN IF EXISTS campaign_id;

DROP INDEX IF EXISTS idx_campaigns_created_at;
DROP INDEX IF EXISTS idx_campaigns_global_idempotency;

DROP TABLE IF EXISTS campaigns;
