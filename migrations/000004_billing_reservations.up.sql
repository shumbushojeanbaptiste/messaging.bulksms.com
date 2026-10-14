-- Billing service uses its own database, so billing_reservation_id intentionally
-- has no cross-database foreign key. Reconciliation uses this durable identifier.
ALTER TABLE messages
    ADD COLUMN billing_reservation_id UUID,
    ADD COLUMN billing_capture_status VARCHAR(20) NOT NULL DEFAULT 'not_required'
        CHECK (billing_capture_status IN ('not_required','pending','captured','release_pending','released'));

ALTER TABLE campaigns
    ADD COLUMN billing_reservation_id UUID,
    ADD COLUMN billing_capture_status VARCHAR(20) NOT NULL DEFAULT 'not_required'
        CHECK (billing_capture_status IN ('not_required','pending','captured','release_pending','released'));

CREATE INDEX idx_messages_billing_capture_pending
    ON messages (created_at)
    WHERE billing_reservation_id IS NOT NULL AND billing_capture_status = 'pending';

CREATE INDEX idx_campaigns_billing_capture_pending
    ON campaigns (created_at)
    WHERE billing_reservation_id IS NOT NULL AND billing_capture_status = 'pending';

-- Tenant-scoped idempotency indexes. Existing global indexes only cover NULL
-- tenant_id rows and remain useful for legacy/internal records.
CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_tenant_idempotency
    ON messages (tenant_id, idempotency_key)
    WHERE tenant_id IS NOT NULL AND idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_campaigns_tenant_idempotency
    ON campaigns (tenant_id, idempotency_key)
    WHERE tenant_id IS NOT NULL AND idempotency_key IS NOT NULL;
