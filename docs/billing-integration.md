# Billing reservations

The API service reserves SMS credits through the billing service before it
persists a message or campaign. Configure the internal billing connection in
the API service environment:

```dotenv
BILLING_SERVICE_URL=http://localhost:8082
BILLING_INTERNAL_SERVICE_TOKEN=<shared-private-internal-token>
```

Use HTTPS for non-local billing service URLs. The API constructs the
`billingclient.Client` at startup; the token is sent only in the
`X-Internal-Service-Token` header and is not exposed to API callers.

## Reservation request

The API sends an internal request to
`POST /internal/v1/reservations` with `Content-Type: application/json` and
`X-Internal-Service-Token`. For one message, the body is equivalent to:

```json
{
  "tenant_id": "TENANT_UUID_FROM_IDENTITY_AUTH",
  "idempotency_key": "message:CLIENT_IDEMPOTENCY_KEY",
  "quantity": 1,
  "currency": "RWF",
  "reference_type": "message",
  "reference_id": "INTERNAL_MESSAGE_UUID"
}
```

The billing client adds the internal-service header itself. Customer requests
should continue to use the messaging API and must not send this internal token.
If the caller omits `Idempotency-Key`, the API uses the generated message or
campaign UUID as the unique suffix for the billing operation key.

## Bulk campaigns

Before reserving, the API trims and deduplicates recipients. The campaign
reservation has `quantity` equal to the number of unique recipients,
`reference_type` equal to `campaign`, and `reference_id` equal to the campaign
UUID. Its idempotency key is `campaign:` followed by the caller's
`Idempotency-Key`.

The API writes the campaign's `billing_reservation_id` and
`billing_capture_status='pending'` in the same database transaction as the
campaign, recipient messages, and outbox events. It reserves once for the
campaign, not once per recipient. A single-message reservation is likewise
stored on its message row with status `pending`.

Capture is a reservation-level operation: a campaign reservation must be
captured at most once for the campaign, never once per recipient. Keep the
capture status pending until the reservation's delivery outcome is eligible
for capture; release a reservation if persistence fails before its outcome is
durably recorded.

## Capture reconciliation

The worker runs a background capture reconciler for pending message and
campaign reservations. It locks a bounded batch of due rows in the messaging
database, calls `billingclient.Capture` once per reservation, and marks a row
`captured` only after billing confirms success. Billing capture is idempotent,
so a crash between the capture and database update can safely retry. Campaign
capture is driven only by the campaign row, not its individual message rows.

Failed calls increment `billing_capture_attempts` and are retried with
exponential backoff starting at five seconds and capped at fifteen minutes.
The current oldest pending reservation age is exported at the worker's
`/debug/vars` endpoint as
`billing_capture_pending_oldest_seconds`. Configure its listener with
`WORKER_METRICS_ADDR` (defaults to `127.0.0.1:9091`); expose it only to
trusted monitoring systems.

API requests continue returning the accepted message or campaign while
capture is pending: reservation has already held the funds in the wallet.
Deploy the worker-side capture reconciler before production traffic is enabled.
