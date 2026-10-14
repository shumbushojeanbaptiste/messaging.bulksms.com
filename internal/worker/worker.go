package worker

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"log"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	// "github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/billingclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/providers/hdev"
)

var oldestPendingCaptureSeconds = expvar.NewInt(
	"billing_capture_pending_oldest_seconds",
)

type Worker struct {
	DB              *pgxpool.Pool
	Provider        *hdev.Client
	Billing         *billingclient.Client
	Writer          *kafka.Writer
	Reader          *kafka.Reader
	OutboxBatchSize int
}

type DispatchEvent struct {
	EventID   string `json:"event_id"`
	MessageID string `json:"message_id"`
}

type outboxRecord struct {
	ID          int64
	EventID     string
	AggregateID string
}

func New(
	db *pgxpool.Pool,
	provider *hdev.Client,
	writer *kafka.Writer,
	reader *kafka.Reader,
	batchSize int,
) *Worker {
	if batchSize < 1 {
		batchSize = 100
	}

	return &Worker{
		DB:              db,
		Provider:        provider,
		Writer:          writer,
		Reader:          reader,
		OutboxBatchSize: batchSize,
	}
}

// Run starts independent publisher, consumer, and reconciliation loops.
func (w *Worker) Run(ctx context.Context) {
	go w.runOutboxPublisher(ctx)
	go w.runConsumer(ctx)
	go w.runReconciler(ctx)
	go w.runBillingCaptureReconciler(ctx)

	<-ctx.Done()
}

type billingCapture struct {
	ID            string
	ReservationID string
	Attempts      int
}

func (w *Worker) runBillingCaptureReconciler(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		if err := w.reconcileBillingCaptures(ctx); err != nil &&
			ctx.Err() == nil {
			log.Printf("Billing capture reconciliation encountered an error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) reconcileBillingCaptures(ctx context.Context) error {
	if w.Billing == nil {
		return errors.New("billing client is unavailable")
	}

	if err := w.updateOldestPendingCaptureMetric(ctx); err != nil {
		return err
	}
	if err := w.reconcileCaptureTable(ctx, "messages"); err != nil {
		return err
	}
	return w.reconcileCaptureTable(ctx, "campaigns")
}

func (w *Worker) updateOldestPendingCaptureMetric(ctx context.Context) error {
	dbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var oldestSeconds int64
	err := w.DB.QueryRow(dbCtx, `
		SELECT GREATEST(
			COALESCE((
				SELECT EXTRACT(EPOCH FROM NOW() - MIN(created_at))::bigint
				FROM messages
				WHERE billing_reservation_id IS NOT NULL
				  AND billing_capture_status = 'pending'
			), 0),
			COALESCE((
				SELECT EXTRACT(EPOCH FROM NOW() - MIN(created_at))::bigint
				FROM campaigns
				WHERE billing_reservation_id IS NOT NULL
				  AND billing_capture_status = 'pending'
			), 0)
		)
	`).Scan(&oldestSeconds)
	if err != nil {
		return err
	}

	oldestPendingCaptureSeconds.Set(oldestSeconds)
	if oldestSeconds > int64(5*time.Minute/time.Second) {
		log.Printf(
			"Billing capture pending age metric: billing_capture_pending_oldest_seconds=%d",
			oldestSeconds,
		)
	}
	return nil
}

func (w *Worker) reconcileCaptureTable(
	ctx context.Context,
	table string,
) error {
	if table != "messages" && table != "campaigns" {
		return errors.New("invalid billing capture table")
	}

	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := w.DB.Begin(dbCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback(dbCtx)

	rows, err := tx.Query(dbCtx, `
		SELECT id::text, billing_reservation_id::text, billing_capture_attempts
		FROM `+table+`
		WHERE billing_reservation_id IS NOT NULL
		  AND billing_capture_status = 'pending'
		  AND billing_capture_next_attempt_at <= NOW()
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, w.OutboxBatchSize)
	if err != nil {
		return err
	}

	captures := make([]billingCapture, 0, w.OutboxBatchSize)
	for rows.Next() {
		var capture billingCapture
		if err := rows.Scan(
			&capture.ID, &capture.ReservationID, &capture.Attempts,
		); err != nil {
			rows.Close()
			return err
		}
		captures = append(captures, capture)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return rowsErr
	}

	for i := range captures {
		captures[i].Attempts++
		_, err = tx.Exec(dbCtx, `
			UPDATE `+table+`
			SET billing_capture_attempts = $2,
			    billing_capture_next_attempt_at = NOW() + INTERVAL '30 seconds'
			WHERE id = $1
		`, captures[i].ID, captures[i].Attempts)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(dbCtx); err != nil {
		return err
	}

	for _, capture := range captures {
		callCtx, callCancel := context.WithTimeout(ctx, 8*time.Second)
		_, captureErr := w.Billing.Capture(callCtx, capture.ReservationID)
		callCancel()
		if captureErr != nil {
			retryAfter := billingCaptureRetryDelay(capture.Attempts)
			updateCtx, updateCancel := context.WithTimeout(ctx, 5*time.Second)
			_, updateErr := w.DB.Exec(updateCtx, `
				UPDATE `+table+`
				SET billing_capture_next_attempt_at = NOW() + $3::interval
				WHERE id = $1
				  AND billing_reservation_id = $2
				  AND billing_capture_status = 'pending'
			`, capture.ID, capture.ReservationID, retryAfter.String())
			updateCancel()
			if updateErr != nil {
				return updateErr
			}
			log.Printf(
				"Billing capture failed for %s %s; retry in %s: %v",
				table, capture.ID, retryAfter, captureErr,
			)
			continue
		}

		updateCtx, updateCancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = w.DB.Exec(updateCtx, `
			UPDATE `+table+`
			SET billing_capture_status = 'captured',
			    billing_capture_next_attempt_at = NOW(),
			    updated_at = NOW()
			WHERE id = $1
			  AND billing_reservation_id = $2
			  AND billing_capture_status = 'pending'
		`, capture.ID, capture.ReservationID)
		updateCancel()
		if err != nil {
			return err
		}
	}

	return nil
}

func billingCaptureRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	const (
		initialDelay = 5 * time.Second
		maximumDelay = 15 * time.Minute
	)
	exponent := math.Min(float64(attempt-1), 8)
	delay := time.Duration(float64(initialDelay) * math.Pow(2, exponent))
	if delay > maximumDelay {
		return maximumDelay
	}
	return delay
}

func (w *Worker) runOutboxPublisher(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		// Drain currently eligible records before waiting again.
		for {
			count, err := w.publishOutboxBatch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("outbox publisher encountered an error")
				break
			}
			if count == 0 {
				break
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) publishOutboxBatch(ctx context.Context) (int, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT
			o.id,
			o.event_id::text,
			o.aggregate_id::text
		FROM outbox_events o
		INNER JOIN messages m ON m.id = o.aggregate_id
		WHERE o.published_at IS NULL
		  AND o.next_attempt_at <= NOW()
		  AND (m.scheduled_at IS NULL OR m.scheduled_at <= NOW())

		ORDER BY o.id
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED
	`, w.OutboxBatchSize)
	if err != nil {
		return 0, err
	}

	records := make([]outboxRecord, 0, w.OutboxBatchSize)
	for rows.Next() {
		var rec outboxRecord
		if err := rows.Scan(
			&rec.ID, &rec.EventID, &rec.AggregateID,
		); err != nil {
			rows.Close()
			return 0, err
		}
		records = append(records, rec)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return 0, rowsErr
	}

	if len(records) == 0 {
		return 0, tx.Commit(ctx)
	}

	messages := make([]kafka.Message, 0, len(records))
	ids := make([]int64, 0, len(records))

	for _, rec := range records {
		body, err := json.Marshal(DispatchEvent{
			EventID:   rec.EventID,
			MessageID: rec.AggregateID,
		})
		if err != nil {
			return 0, err
		}

		messages = append(messages, kafka.Message{
			Key:   []byte(rec.AggregateID),
			Value: body,
		})
		ids = append(ids, rec.ID)
	}

	// The transaction remains open while publishing. If publishing
	// succeeds but the subsequent database commit fails, an event
	// may be published again. The consumer's database claim prevents
	// a second provider submission for an already claimed message.
	if err := w.Writer.WriteMessages(ctx, messages...); err != nil {
		_, updateErr := tx.Exec(ctx, `
			UPDATE outbox_events
			SET publish_attempts = publish_attempts + 1,
			    last_error = 'kafka_publish_failed',
			    next_attempt_at = NOW() + INTERVAL '5 seconds'
			WHERE id = ANY($1::bigint[])
		`, ids)
		if updateErr != nil {
			return 0, updateErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return 0, commitErr
		}
		return 0, err
	}

	_, err = tx.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = NOW(),
		    last_error = NULL
		WHERE id = ANY($1::bigint[])
	`, ids)
	if err != nil {
		return 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}

	return len(records), nil
}

func (w *Worker) runConsumer(ctx context.Context) {
	for {
		message, err := w.Reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Kafka consumer fetch failed")
			if !sleepContext(ctx, time.Second) {
				return
			}
			continue
		}

		var event DispatchEvent
		if err := json.Unmarshal(message.Value, &event); err != nil ||
			event.MessageID == "" || event.EventID == "" {
			// This topic is written by our own outbox publisher.
			// Malformed internal events are logged and skipped to
			// avoid blocking the consumer group indefinitely.
			log.Printf("Kafka received a malformed dispatch event")
			if err := w.Reader.CommitMessages(ctx, message); err != nil &&
				ctx.Err() == nil {
				log.Printf("Kafka offset commit failed")
			}
			continue
		}

		for {
			err := w.dispatchOne(ctx, event.MessageID)
			if err == nil {
				break
			}

			if ctx.Err() != nil {
				return
			}

			// Retry the database operation/event processing, not a
			// provider send that already has an ambiguous outcome.
			log.Printf("Dispatch processing failed; retrying event")
			if !sleepContext(ctx, time.Second) {
				return
			}
		}

		if err := w.Reader.CommitMessages(ctx, message); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("Kafka offset commit failed")
		}
	}
}

type claimedMessage struct {
	ID         uuid.UUID
	Recipient  string
	SenderID   string
	Body       string
	CampaignID string
}

func (w *Worker) claimMessage(
	ctx context.Context,
	id uuid.UUID,
) (*claimedMessage, bool, error) {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	var msg claimedMessage
	msg.ID = id

	err = tx.QueryRow(ctx, `
		UPDATE messages
		SET status = 'submitting',
		    attempt_count = attempt_count + 1,
		    submission_started_at = NOW(),
		    updated_at = NOW()
		WHERE id = $1
		  AND status = 'queued'
		  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
		RETURNING
			recipient,
			sender_id,
			message_body,
			COALESCE(campaign_id::text, '')
	`, id).Scan(
		&msg.Recipient,
		&msg.SenderID,
		&msg.Body,
		&msg.CampaignID,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, tx.Rollback(ctx)
	}
	if err != nil {
		return nil, false, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, previous_status, new_status, source
		) VALUES ($1, 'queued', 'submitting', 'worker')
	`, id)
	if err != nil {
		return nil, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}

	return &msg, true, nil
}

func (w *Worker) dispatchOne(ctx context.Context, rawID string) error {
	id, err := uuid.Parse(rawID)
	if err != nil {
		// Invalid message IDs are not created by our application.
		// The consumer should not keep retrying a malformed event.
		return nil
	}

	msg, claimed, err := w.claimMessage(ctx, id)
	if err != nil {
		return err
	}
	if !claimed {
		// Already claimed, completed, scheduled, or otherwise no
		// longer eligible. A duplicate Kafka event must not resend it.
		return nil
	}

	result, sendErr := w.Provider.Send(
		ctx, msg.SenderID, msg.Recipient, msg.Body,
	)

	nextStatus := "unknown"
	errorCode := "provider_outcome_unknown"
	providerSID := ""
	var response *hdev.Response

	if sendErr == nil && result != nil {
		response = result

		switch strings.ToLower(strings.TrimSpace(result.Status)) {
		case "success":
			providerSID = result.SIDString()
			if providerSID != "" {
				nextStatus = "sent"
				errorCode = ""
			} else {
				nextStatus = "unknown"
				errorCode = "provider_missing_message_id"
			}
		case "error":
			// A documented provider rejection is a definite failure.
			nextStatus = "failed"
			errorCode = classifyProviderError(result.Message)
		default:
			// An unexpected response must be reconciled, not resent.
			nextStatus = "unknown"
			errorCode = "provider_unexpected_response"
		}
	}

	// Provider transport errors and timeouts have an ambiguous
	// outcome: HDEV might have accepted the SMS before the failure.
	// Never blindly retry that submission.
	if err := w.finishMessage(
		id, nextStatus, providerSID, errorCode, response,
	); err != nil {
		return err
	}

	if msg.CampaignID != "" {
		if err := w.refreshCampaignStatus(msg.CampaignID); err != nil {
			return err
		}
	}

	return nil
}

func classifyProviderError(message string) string {
	msg := strings.ToLower(message)

	switch {
	case strings.Contains(msg, "insufficient balance"):
		return "provider_insufficient_balance"
	case strings.Contains(msg, "sender id not valid"):
		return "provider_invalid_sender_id"
	case strings.Contains(msg, "api user does not exist"):
		return "provider_authentication_failed"
	case strings.Contains(msg, "message not found"):
		return "provider_message_not_found"
	default:
		return "provider_rejected_request"
	}
}

func (w *Worker) finishMessage(
	id uuid.UUID,
	nextStatus string,
	providerSID string,
	errorCode string,
	response *hdev.Response,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var previous string
	err = tx.QueryRow(ctx, `
		SELECT status
		FROM messages
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&previous)
	if err != nil {
		return err
	}

	// The stale-submission reconciler may already have resolved the
	// state. Never overwrite a state that is no longer submitting.
	if previous != "submitting" {
		return tx.Commit(ctx)
	}

	raw := hdev.SafeRawJSON(response)

	errorMessage := ""
	switch {
	case nextStatus == "failed":
		errorMessage = "Provider rejected the request"
	case nextStatus == "unknown":
		errorMessage = "Provider outcome could not be confirmed"
	}

	_, err = tx.Exec(ctx, `
		UPDATE messages
		SET status = $2,
		    provider_message_id = NULLIF($3, ''),
		    provider_response = $4::jsonb,
		    error_code = NULLIF($5, ''),
		    error_message = NULLIF($6, ''),
		    updated_at = NOW()
		WHERE id = $1
		  AND status = 'submitting'
	`, id, nextStatus, providerSID, string(raw), errorCode, errorMessage)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, previous_status, new_status, source, details
		) VALUES ($1, $2, $3, 'worker', $4::jsonb)
	`, id, previous, nextStatus, string(raw))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (w *Worker) refreshCampaignStatus(rawID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id, err := uuid.Parse(rawID)
	if err != nil {
		return err
	}

	_, err = w.DB.Exec(ctx, `
		UPDATE campaigns c
		SET status = CASE
			WHEN c.scheduled_at IS NOT NULL
			     AND c.scheduled_at > NOW()
				THEN 'scheduled'

			WHEN EXISTS (
				SELECT 1 FROM messages m
				WHERE m.campaign_id = c.id
				  AND m.status IN ('queued', 'submitting')
			) THEN 'processing'

			WHEN EXISTS (
				SELECT 1 FROM messages m
				WHERE m.campaign_id = c.id
				  AND m.status = 'unknown'
			) THEN 'needs_review'

			WHEN (
				SELECT COUNT(*) FROM messages m
				WHERE m.campaign_id = c.id
				  AND m.status = 'failed'
			) = c.recipient_count THEN 'failed'

			WHEN EXISTS (
				SELECT 1 FROM messages m
				WHERE m.campaign_id = c.id
				  AND m.status = 'failed'
			) THEN 'partial'

			ELSE 'submitted'
		END,
		updated_at = NOW()
		WHERE c.id = $1
	`, id)

	return err
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) runReconciler(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		if err := w.reconcileStaleSubmissions(ctx); err != nil &&
			ctx.Err() == nil {
			log.Printf("Submission reconciliation encountered an error")
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) reconcileStaleSubmissions(ctx context.Context) error {
	dbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := w.DB.Begin(dbCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback(dbCtx)

	rows, err := tx.Query(dbCtx, `
		UPDATE messages
		SET status = 'unknown',
		    error_code = 'stale_submission_reconciliation',
		    error_message = 'Submission outcome requires reconciliation',
		    updated_at = NOW()
		WHERE status = 'submitting'
		  AND submission_started_at < NOW() - INTERVAL '2 minutes'
		RETURNING id, COALESCE(campaign_id::text, '')
	`)
	if err != nil {
		return err
	}

	type changed struct {
		ID         uuid.UUID
		CampaignID string
	}
	updates := make([]changed, 0)

	for rows.Next() {
		var item changed
		if err := rows.Scan(&item.ID, &item.CampaignID); err != nil {
			rows.Close()
			return err
		}
		updates = append(updates, item)
	}

	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return rowsErr
	}

	for _, item := range updates {
		_, err := tx.Exec(dbCtx, `
			INSERT INTO message_status_history (
				message_id, previous_status, new_status, source, details
			) VALUES (
				$1, 'submitting', 'unknown', 'reconciler',
				jsonb_build_object('reason', 'stale_submission')
			)
		`, item.ID)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(dbCtx); err != nil {
		return err
	}

	for _, item := range updates {
		if item.CampaignID != "" {
			if err := w.refreshCampaignStatus(item.CampaignID); err != nil {
				return err
			}
		}
	}

	return nil
}

// Ensure a nullable UUID can still be scanned when required in future
// database operations; it is also useful for nullable campaign metadata.
// var _ pgtype.UUID
