CREATE TABLE IF NOT EXISTS outbox_events (
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

CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events (next_attempt_at, id)
    WHERE published_at IS NULL;
