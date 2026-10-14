
CREATE TABLE providers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    api_id VARCHAR(255) NOT NULL,
    secret_ref VARCHAR(255) NOT NULL DEFAULT 'environment',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID,
    provider_id BIGINT NOT NULL REFERENCES providers(id),
    idempotency_key VARCHAR(255),
    recipient VARCHAR(30) NOT NULL,
    sender_id VARCHAR(20) NOT NULL,
    message_body TEXT NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'accepted',
    provider_message_id VARCHAR(255),
    external_message_id VARCHAR(255),
    provider_response JSONB,
    error_code VARCHAR(100),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_messages_created_at
    ON messages (created_at DESC);

CREATE INDEX idx_messages_status_created
    ON messages (status, created_at DESC);

CREATE INDEX idx_messages_provider_message_id
    ON messages (provider_id, provider_message_id);

CREATE INDEX idx_messages_tenant_created
    ON messages (tenant_id, created_at DESC);

CREATE UNIQUE INDEX idx_messages_tenant_idempotency
    ON messages (tenant_id, idempotency_key)
    WHERE tenant_id IS NOT NULL
      AND idempotency_key IS NOT NULL;

CREATE TABLE message_status_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id UUID NOT NULL REFERENCES messages(id),
    previous_status VARCHAR(30),
    new_status VARCHAR(30) NOT NULL,
    source VARCHAR(30) NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_message_status_history_message
    ON message_status_history (message_id, created_at DESC);
