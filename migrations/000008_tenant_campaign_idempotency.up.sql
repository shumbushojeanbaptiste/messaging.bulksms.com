-- Phase 1 already created a tenant-scoped messages idempotency index.
-- Add the corresponding index for campaigns before enabling tenant IDs.
CREATE UNIQUE INDEX IF NOT EXISTS idx_campaigns_tenant_idempotency
    ON campaigns (tenant_id, idempotency_key)
    WHERE tenant_id IS NOT NULL AND idempotency_key IS NOT NULL;
