DROP INDEX IF EXISTS idx_campaigns_tenant_idempotency;
DROP INDEX IF EXISTS idx_messages_tenant_idempotency;
DROP INDEX IF EXISTS idx_campaigns_billing_capture_pending;
DROP INDEX IF EXISTS idx_messages_billing_capture_pending;
ALTER TABLE campaigns DROP COLUMN IF EXISTS billing_capture_status, DROP COLUMN IF EXISTS billing_reservation_id;
ALTER TABLE messages DROP COLUMN IF EXISTS billing_capture_status, DROP COLUMN IF EXISTS billing_reservation_id;
