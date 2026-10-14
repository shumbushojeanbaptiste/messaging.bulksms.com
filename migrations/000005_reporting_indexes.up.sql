-- Verify your current schema has tenant_id on messages/campaigns before applying.
CREATE INDEX IF NOT EXISTS idx_messages_tenant_created_id
    ON messages (tenant_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_status_created_id
    ON messages (tenant_id, status, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_recipient_created_id
    ON messages (tenant_id, recipient, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_tenant_campaign_created_id
    ON messages (tenant_id, campaign_id, created_at DESC, id DESC)
    WHERE campaign_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_campaigns_tenant_created_id
    ON campaigns (tenant_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_campaigns_tenant_status_created_id
    ON campaigns (tenant_id, status, created_at DESC, id DESC);
