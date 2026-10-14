
-- Bulk SMS campaigns.
CREATE TABLE campaigns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID,
    idempotency_key VARCHAR(255),
    request_hash VARCHAR(64) NOT NULL,
    sender_id VARCHAR(20) NOT NULL,
    message_body TEXT NOT NULL,
    recipient_count INTEGER NOT NULL CHECK (recipient_count > 0),
    status VARCHAR(30) NOT NULL DEFAULT 'queued',
    scheduled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add a global idempotency index for the current single-account MVP.
-- Tenant-specific indexes and authorization will follow in Phase 3.
CREATE UNIQUE INDEX idx_campaigns_global_idempotency
    ON campaigns (idempotency_key)
    WHERE tenant_id IS NULL
      AND idempotency_key IS NOT NULL;

CREATE INDEX idx_campaigns_created_at
    ON campaigns (created_at DESC);

ALTER TABLE messages
    ADD COLUMN campaign_id UUID REFERENCES campaigns(id),
    ADD COLUMN request_hash VARCHAR(64),
    ADD COLUMN scheduled_at TIMESTAMPTZ,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN submission_started_at TIMESTAMPTZ;

-- Idempotency for the current single-account API.
CREATE UNIQUE INDEX idx_messages_global_idempotency
    ON messages (idempotency_key)
    WHERE tenant_id IS NULL
      AND idempotency_key IS NOT NULL;

CREATE INDEX idx_messages_campaign_status
    ON messages (campaign_id, status)
    WHERE campaign_id IS NOT NULL;

CREATE INDEX idx_messages_scheduled
    ON messages (scheduled_at)
    WHERE status = 'queued';

-- Transactional outbox: committed messages cannot be lost
-- simply because Kafka was temporarily unavailable.
CREATE TABLE outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    aggregate_id UUID NOT NULL REFERENCES messages(id),
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error VARCHAR(200)
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (next_attempt_at, id)
    WHERE published_at IS NULL;
