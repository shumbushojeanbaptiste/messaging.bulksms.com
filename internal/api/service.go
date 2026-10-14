package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/billingclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/identityclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/providers/hdev"
)

type Server struct {
	DB                    *pgxpool.Pool
	Provider              *hdev.Client
	SenderID              string
	ProviderID            int64
	MaxCampaignRecipients int
	Identity              *identityclient.Client
	Billing               *billingclient.Client
	AdminInternalToken    string
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)

	mux.HandleFunc("POST /api/v1/messages", s.sendMessage)
	mux.HandleFunc("POST /api/v1/messages/{id}/status", s.checkStatus)

	mux.HandleFunc("POST /api/v1/campaigns", s.createCampaign)
	mux.HandleFunc("GET /api/v1/campaigns/{id}", s.getCampaign)

	// intergration to view report
	mux.HandleFunc("GET /api/v1/messages", s.listMessages)
	mux.HandleFunc("GET /api/v1/messages/{id}", s.getMessage)
	mux.HandleFunc(
		"GET /api/v1/messages/{id}/history",
		s.getMessageHistory,
	)

	mux.HandleFunc("GET /api/v1/campaigns", s.listCampaigns)
	mux.HandleFunc(
		"GET /api/v1/campaigns/{id}/messages",
		s.listCampaignMessages,
	)
	mux.HandleFunc(
		"GET /api/v1/dashboard/summary",
		s.dashboardSummary,
	)

	mux.HandleFunc("GET /api/v1/wallet", s.customerWallet)
	mux.HandleFunc(
		"GET /api/v1/wallet/transactions",
		s.customerWalletTransactions,
	)

	// admin report
	mux.HandleFunc(
		"GET /internal/v1/admin/messages",
		s.adminListMessages,
	)
	mux.HandleFunc(
		"GET /internal/v1/admin/campaigns",
		s.adminListCampaigns,
	)
	mux.HandleFunc(
		"GET /internal/v1/admin/dashboard/summary",
		s.adminMessagingSummary,
	)
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func requestHash(parts ...string) string {
	h := sha256.New()

	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil))
}

func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if err := s.DB.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not_ready",
			"error":  "database_unavailable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "ready",
		"database": "connected",
	})
}

type sendRequest struct {
	Recipient string `json:"recipient"`
	SenderID  string `json:"sender_id"`
	Message   string `json:"message"`
}

var phonePattern = regexp.MustCompile(`^\+?[0-9]{7,15}$`)

func (s *Server) sendMessageWithoutKafka(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	var req sendRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	// Reject additional JSON values after the first object.
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	req.Recipient = strings.TrimSpace(req.Recipient)
	req.SenderID = strings.TrimSpace(req.SenderID)
	req.Message = strings.TrimSpace(req.Message)

	if !phonePattern.MatchString(req.Recipient) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_recipient",
		})
		return
	}

	if req.SenderID == "" {
		req.SenderID = s.SenderID
	}

	if req.SenderID != s.SenderID {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "sender_id_not_allowed",
		})
		return
	}

	if req.Message == "" || len(req.Message) > 5000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_message",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	messageID := uuid.New()
	var storedID string

	err := s.DB.QueryRow(ctx, `
		INSERT INTO messages (
			id, provider_id, recipient, sender_id,
			message_body, status
		)
		VALUES ($1, $2, $3, $4, $5, 'submitting')
		RETURNING id::text
	`, messageID, s.ProviderID, req.Recipient,
		req.SenderID, req.Message).Scan(&storedID)

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "message_persistence_failed",
		})
		return
	}

	_, _ = s.DB.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, new_status, source
		) VALUES ($1, 'submitting', 'api')
	`, messageID)

	result, providerErr := s.Provider.Send(
		ctx, req.SenderID, req.Recipient, req.Message,
	)

	if providerErr != nil {
		log.Printf("SMS provider send failed for message %s: %v", storedID, providerErr)

		// A timeout may mean the provider accepted the SMS but
		// its response was lost. Do not automatically retry it.
		_, _ = s.DB.Exec(context.Background(), `
			UPDATE messages
			SET status = 'unknown',
			    error_code = 'provider_outcome_unknown',
			    error_message = 'Provider outcome could not be confirmed',
			    updated_at = NOW()
			WHERE id = $1
		`, messageID)

		_, _ = s.DB.Exec(context.Background(), `
			INSERT INTO message_status_history (
				message_id, previous_status, new_status, source
			) VALUES ($1, 'submitting', 'unknown', 'provider')
		`, messageID)

		writeJSON(w, http.StatusBadGateway, map[string]string{
			"id":     storedID,
			"status": "unknown",
			"error":  "provider_outcome_unknown",
		})
		return
	}

	if strings.EqualFold(result.Status, "success") {
		providerSID := result.SIDString()
		if providerSID == "" {
			// The provider reported success without a usable ID.
			// Do not resend; reconcile the outcome.
			s.updateResult(messageID, "unknown", "", result,
				"provider_missing_message_id")

			writeJSON(w, http.StatusBadGateway, map[string]string{
				"id":     storedID,
				"status": "unknown",
				"error":  "provider_missing_message_id",
			})
			return
		}

		if err := s.updateResult(messageID, "sent", providerSID,
			result, ""); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"id":     storedID,
				"status": "unknown",
				"error":  "message_persistence_failed",
			})
			return
		}

		writeJSON(w, http.StatusCreated, map[string]any{
			"id":                  storedID,
			"status":              "sent",
			"provider_message_id": providerSID,
		})
		return
	}

	errorCode := classifyProviderError(result.Message)
	s.updateResult(messageID, "failed", "", result, errorCode)

	writeJSON(w, http.StatusBadGateway, map[string]any{
		"id":     storedID,
		"status": "failed",
		"error":  errorCode,
	})
}

func (s *Server) sendMessage(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	var req sendRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	req.Recipient = strings.TrimSpace(req.Recipient)
	req.SenderID = strings.TrimSpace(req.SenderID)
	req.Message = strings.TrimSpace(req.Message)

	if !phonePattern.MatchString(req.Recipient) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_recipient",
		})
		return
	}

	if req.SenderID == "" {
		req.SenderID = s.SenderID
	}

	if req.SenderID != s.SenderID {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "sender_id_not_allowed",
		})
		return
	}

	if req.Message == "" || len(req.Message) > 5000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_message",
		})
		return
	}

	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > 255 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "idempotency_key_too_long",
		})
		return
	}

	hash := requestHash(req.Recipient, req.SenderID, req.Message)
	authorization, ok := s.authorizeTenant(
		w, r, "sms:send", req.SenderID, key, hash, 1, "message",
	)
	if !ok {
		return
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			if err := s.releaseQuotaBestEffort(authorization.ReservationID); err != nil {
				log.Printf("identity quota release failed: %v", err)
			}
		}
	}()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	messageID := uuid.New()
	tenantID, err := uuid.Parse(authorization.TenantID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "identity_service_invalid_response",
		})
		return
	}
	billingKey := "message:" + key
	if key == "" {
		billingKey += messageID.String()
	}
	billingReservation, err := s.reserveBilling(
		ctx,
		billingclient.ReservationRequest{
			TenantID:       tenantID.String(),
			IdempotencyKey: billingKey,
			Quantity:       1,
			Currency:       "RWF",
			ReferenceType:  "message",
			ReferenceID:    messageID.String(),
		},
	)
	if err != nil {
		writeBillingError(w, err)
		return
	}
	if _, err := uuid.Parse(billingReservation.ID); err != nil {
		log.Printf("billing service returned invalid reservation ID for message %s", messageID)
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "billing_service_invalid_response",
		})
		return
	}
	releaseBillingReservation := true
	defer func() {
		if releaseBillingReservation {
			if err := s.releaseBillingBestEffort(billingReservation.ID); err != nil {
				log.Printf("billing reservation release failed for message %s: %v", messageID, err)
			}
		}
	}()

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "database_error",
		})
		return
	}
	defer tx.Rollback(ctx)

	var storedID string

	var keyValue any
	if key != "" {
		keyValue = key
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO messages (
			id, tenant_id, provider_id, idempotency_key, request_hash,
			recipient, sender_id, message_body, status,
			billing_reservation_id, billing_capture_status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', $9, 'pending')
		ON CONFLICT (tenant_id, idempotency_key)
			WHERE tenant_id IS NOT NULL AND idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id::text
	`, messageID, tenantID, s.ProviderID, keyValue, hash,
		req.Recipient, req.SenderID, req.Message,
		billingReservation.ID).Scan(&storedID)

	if errors.Is(err, pgx.ErrNoRows) && key != "" {
		var existingID, existingHash, existingStatus string

		err = tx.QueryRow(ctx, `
			SELECT id::text, request_hash, status
			FROM messages
			WHERE tenant_id = $1
			  AND idempotency_key = $2
		`, tenantID, key).Scan(&existingID, &existingHash, &existingStatus)

		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "idempotency_lookup_failed",
			})
			return
		}

		_ = tx.Rollback(ctx)
		releaseReservation = false
		releaseBillingReservation = false

		if existingHash != hash {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "idempotency_key_reused_with_different_request",
			})
			return
		}

		if err := s.commitQuotaBestEffort(authorization.ReservationID); err != nil {
			log.Printf("identity quota commit failed for message %s: %v", existingID, err)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id":                existingID,
			"status":            existingStatus,
			"idempotent_replay": true,
		})
		return
	}

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "message_persistence_failed",
		})
		return
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, new_status, source
		) VALUES ($1, 'queued', 'api')
	`, messageID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "status_history_persistence_failed",
		})
		return
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, event_type, payload)
		VALUES ($1, 'sms.dispatch', jsonb_build_object('message_id', $2::text))
	`, messageID, messageID.String())
	if err != nil {
		log.Printf("outbox event insert failed for message %s: %v", storedID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "outbox_persistence_failed",
		})
		return
	}

	if err := tx.Commit(ctx); err != nil {
		// The commit outcome may be ambiguous; do not release quota
		// for a message that may have been persisted.
		releaseReservation = false
		releaseBillingReservation = false
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "message_commit_failed",
		})
		return
	}
	releaseReservation = false
	releaseBillingReservation = false
	if err := s.commitQuotaBestEffort(authorization.ReservationID); err != nil {
		log.Printf("identity quota commit failed for message %s: %v", storedID, err)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"id":     storedID,
		"status": "queued",
	})
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

func (s *Server) updateResult(
	id uuid.UUID,
	status string,
	providerSID string,
	result *hdev.Response,
	errorCode string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	raw := hdev.SafeRawJSON(result)

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var previous string
	err = tx.QueryRow(ctx, `
		SELECT status FROM messages WHERE id = $1 FOR UPDATE
	`, id).Scan(&previous)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE messages
		SET status = $2,
		    provider_message_id = NULLIF($3, ''),
		    provider_response = $4::jsonb,
		    error_code = NULLIF($5, ''),
		    error_message = CASE
		        WHEN $5 = '' THEN NULL
		        ELSE 'Provider rejected the request'
		    END,
		    updated_at = NOW()
		WHERE id = $1
	`, id, status, providerSID, string(raw), errorCode)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, previous_status, new_status, source, details
		) VALUES ($1, $2, $3, 'provider', $4::jsonb)
	`, id, previous, status, string(raw))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Server) checkStatus(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_message_id",
		})
		return
	}

	authorization, ok := s.authorizeTenant(
		w, r, "sms:status:read", "", "", "", 0, "",
	)
	if !ok {
		return
	}
	tenantID, err := uuid.Parse(authorization.TenantID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "identity_service_invalid_response",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	var status string
	var providerSID *string

	err = s.DB.QueryRow(ctx, `
		SELECT status, provider_message_id
		FROM messages
		WHERE id = $1
		  AND tenant_id = $2
		  AND provider_id = $3
	`, id, tenantID, s.ProviderID).Scan(&status, &providerSID)

	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "message_not_found",
		})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "database_error",
		})
		return
	}

	if providerSID == nil || *providerSID == "" {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error":  "provider_message_id_unavailable",
			"status": status,
		})
		return
	}

	result, err := s.Provider.CheckStatus(ctx, *providerSID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "provider_status_unavailable",
		})
		return
	}

	if !strings.EqualFold(result.Status, "success") {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": classifyProviderError(result.Message),
		})
		return
	}

	deliveryStatus := strings.ToLower(strings.TrimSpace(result.DeliveryStatus))
	switch deliveryStatus {
	case "sent", "pending", "delivered", "failed":
		// Recognized provider statuses.
	default:
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "unknown_provider_delivery_status",
		})
		return
	}

	if deliveryStatus != status {
		if err := s.updateStatusFromPoll(ctx, id, status, deliveryStatus, result); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "status_persistence_failed",
			})
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":                  id.String(),
		"status":              deliveryStatus,
		"provider_message_id": *providerSID,
		"external_message_id": result.ExternalMessageID,
	})
}

func (s *Server) updateStatusFromPoll(
	ctx context.Context,
	id uuid.UUID,
	previous string,
	next string,
	result *hdev.Response,
) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	raw := hdev.SafeRawJSON(result)

	_, err = tx.Exec(ctx, `
		UPDATE messages
		SET status = $2,
		    external_message_id = $3,
		    provider_response = $4::jsonb,
		    updated_at = NOW()
		WHERE id = $1
	`, id, next, result.ExternalMessageID, string(raw))
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, previous_status, new_status, source, details
		) VALUES ($1, $2, $3, 'status_poll', $4::jsonb)
	`, id, previous, next, string(raw))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// added when we add kafka

type campaignRequest struct {
	Recipients  []string   `json:"recipients"`
	SenderID    string     `json:"sender_id"`
	Message     string     `json:"message"`
	ScheduledAt *time.Time `json:"scheduled_at,omitempty"`
}

func uniqueRecipients(input []string) []string {
	seen := make(map[string]struct{}, len(input))
	result := make([]string, 0, len(input))

	for _, recipient := range input {
		recipient = strings.TrimSpace(recipient)
		if _, exists := seen[recipient]; exists {
			continue
		}
		seen[recipient] = struct{}{}
		result = append(result, recipient)
	}

	return result
}

func (s *Server) createCampaign(w http.ResponseWriter, r *http.Request) {
	maxRecipients := s.MaxCampaignRecipients
	if maxRecipients <= 0 {
		maxRecipients = 10000
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2*1024*1024)

	var req campaignRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_json",
		})
		return
	}

	if len(req.Recipients) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "recipients_required",
		})
		return
	}

	if len(req.Recipients) > maxRecipients {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":          "recipient_limit_exceeded",
			"max_recipients": maxRecipients,
		})
		return
	}

	req.SenderID = strings.TrimSpace(req.SenderID)
	req.Message = strings.TrimSpace(req.Message)

	if req.SenderID == "" {
		req.SenderID = s.SenderID
	}

	if req.SenderID != s.SenderID {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "sender_id_not_allowed",
		})
		return
	}

	if req.Message == "" || len(req.Message) > 5000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_message",
		})
		return
	}

	for _, recipient := range req.Recipients {
		recipient = strings.TrimSpace(recipient)
		if !phonePattern.MatchString(recipient) {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "invalid_recipient",
			})
			return
		}
	}

	inputRecipientCount := len(req.Recipients)
	req.Recipients = uniqueRecipients(req.Recipients)
	if len(req.Recipients) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "recipients_required",
		})
		return
	}

	if req.ScheduledAt != nil {
		if req.ScheduledAt.Before(time.Now().Add(-30 * time.Second)) {
			writeJSON(w, http.StatusBadRequest, map[string]string{
				"error": "scheduled_at_must_not_be_in_the_past",
			})
			return
		}
		t := req.ScheduledAt.UTC()
		req.ScheduledAt = &t
	}

	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > 255 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "idempotency_key_too_long",
		})
		return
	}

	canonical, err := json.Marshal(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "request_encoding_failed",
		})
		return
	}
	hash := requestHash(string(canonical))
	authorization, ok := s.authorizeTenant(
		w, r, "campaigns:create", req.SenderID, key, hash,
		len(req.Recipients), "campaign",
	)
	if !ok {
		return
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			if err := s.releaseQuotaBestEffort(authorization.ReservationID); err != nil {
				log.Printf("identity quota release failed: %v", err)
			}
		}
	}()

	if authorization.MaxCampaignRecipients > 0 &&
		len(req.Recipients) > authorization.MaxCampaignRecipients {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":          "recipient_limit_exceeded",
			"max_recipients": authorization.MaxCampaignRecipients,
		})
		return
	}
	tenantID, err := uuid.Parse(authorization.TenantID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "identity_service_invalid_response",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	campaignID := uuid.New()
	billingKey := "campaign:" + key
	if key == "" {
		billingKey += campaignID.String()
	}
	billingReservation, err := s.reserveBilling(
		ctx,
		billingclient.ReservationRequest{
			TenantID:       tenantID.String(),
			IdempotencyKey: billingKey,
			Quantity:       int64(len(req.Recipients)),
			Currency:       "RWF",
			ReferenceType:  "campaign",
			ReferenceID:    campaignID.String(),
		},
	)
	if err != nil {
		writeBillingError(w, err)
		return
	}
	if _, err := uuid.Parse(billingReservation.ID); err != nil {
		log.Printf("billing service returned invalid reservation ID for campaign %s", campaignID)
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "billing_service_invalid_response",
		})
		return
	}
	releaseBillingReservation := true
	defer func() {
		if releaseBillingReservation {
			if err := s.releaseBillingBestEffort(billingReservation.ID); err != nil {
				log.Printf("billing reservation release failed for campaign %s: %v", campaignID, err)
			}
		}
	}()

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "database_error",
		})
		return
	}
	defer tx.Rollback(ctx)

	var keyValue any
	if key != "" {
		keyValue = key
	}

	initialStatus := "queued"
	if req.ScheduledAt != nil && req.ScheduledAt.After(time.Now()) {
		initialStatus = "scheduled"
	}

	var storedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO campaigns (
			id, tenant_id, idempotency_key, request_hash, sender_id,
			message_body, recipient_count, status, scheduled_at,
			billing_reservation_id, billing_capture_status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'pending')
		ON CONFLICT (tenant_id, idempotency_key)
			WHERE tenant_id IS NOT NULL AND idempotency_key IS NOT NULL
		DO NOTHING
		RETURNING id::text
	`, campaignID, tenantID, keyValue, hash, req.SenderID,
		req.Message, len(req.Recipients), initialStatus,
		req.ScheduledAt, billingReservation.ID).Scan(&storedID)

	if errors.Is(err, pgx.ErrNoRows) && key != "" {
		var existingHash, existingStatus string
		var existingID string
		var existingCount int

		err = tx.QueryRow(ctx, `
			SELECT id::text, request_hash, status, recipient_count
			FROM campaigns
			WHERE tenant_id = $1
			  AND idempotency_key = $2
		`, tenantID, key).Scan(
			&existingID, &existingHash, &existingStatus, &existingCount,
		)

		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": "idempotency_lookup_failed",
			})
			return
		}

		_ = tx.Rollback(ctx)
		releaseReservation = false
		releaseBillingReservation = false

		if existingHash != hash {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "idempotency_key_reused_with_different_request",
			})
			return
		}

		if err := s.commitQuotaBestEffort(authorization.ReservationID); err != nil {
			log.Printf("identity quota commit failed for campaign %s: %v", existingID, err)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"campaign_id":       existingID,
			"status":            existingStatus,
			"recipient_count":   existingCount,
			"idempotent_replay": true,
		})
		return
	}
	if err != nil {
		log.Printf("campaign insert failed for tenant %s: %v", tenantID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "campaign_persistence_failed",
		})
		return
	}

	// Insert all messages and matching outbox events in the same
	// transaction. The provider is not called by this HTTP handler.
	_, err = tx.Exec(ctx, `
		WITH inserted AS (
			INSERT INTO messages (
				id, tenant_id, campaign_id, provider_id, recipient,
				sender_id, message_body, status, scheduled_at
			)
			SELECT
				gen_random_uuid(),
				$1,
				$2,
				$3,
				item.recipient,
				$4,
				$5,
				'queued',
				$6
			FROM unnest($7::text[]) AS item(recipient)
			RETURNING id
		)
		INSERT INTO outbox_events (
			aggregate_id, event_type, payload
		)
		SELECT
			id,
			'sms.dispatch',
			jsonb_build_object('message_id', id::text)
		FROM inserted
	`, tenantID, campaignID, s.ProviderID, req.SenderID, req.Message,
		req.ScheduledAt, req.Recipients)

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "campaign_messages_persistence_failed",
		})
		return
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO message_status_history (
			message_id, new_status, source, details
		)
		SELECT
			id,
			'queued',
			'api',
			jsonb_build_object('campaign_id', campaign_id::text)
		FROM messages
		WHERE campaign_id = $1
		  AND tenant_id = $2
	`, campaignID, tenantID)

	if err != nil {
		log.Printf("campaign history insert failed for campaign %s: %v", storedID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "campaign_history_persistence_failed",
		})
		return
	}

	if err := tx.Commit(ctx); err != nil {
		// The commit outcome may be ambiguous; do not release quota
		// for a campaign that may have been persisted.
		releaseReservation = false
		releaseBillingReservation = false
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "campaign_commit_failed",
		})
		return
	}
	releaseReservation = false
	releaseBillingReservation = false
	if err := s.commitQuotaBestEffort(authorization.ReservationID); err != nil {
		log.Printf("identity quota commit failed for campaign %s: %v", storedID, err)
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"campaign_id":                  storedID,
		"status":                       initialStatus,
		"recipient_count":              len(req.Recipients),
		"duplicate_recipients_removed": inputRecipientCount - len(req.Recipients),
	})
}

func (s *Server) getCampaign(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid_campaign_id",
		})
		return
	}

	authorization, ok := s.authorizeTenant(
		w, r, "campaigns:read", "", "", "", 0, "",
	)
	if !ok {
		return
	}
	tenantID, err := uuid.Parse(authorization.TenantID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "identity_service_invalid_response",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var (
		campaignID  string
		status      string
		senderID    string
		count       int
		createdAt   time.Time
		scheduledAt *time.Time
		queued      int64
		submitting  int64
		sent        int64
		pending     int64
		delivered   int64
		failed      int64
		unknown     int64
	)

	err = s.DB.QueryRow(ctx, `
		SELECT
			c.id::text,
			c.status,
			c.sender_id,
			c.recipient_count,
			c.created_at,
			c.scheduled_at,
			COUNT(m.id) FILTER (WHERE m.status = 'queued'),
			COUNT(m.id) FILTER (WHERE m.status = 'submitting'),
			COUNT(m.id) FILTER (WHERE m.status = 'sent'),
			COUNT(m.id) FILTER (WHERE m.status = 'pending'),
			COUNT(m.id) FILTER (WHERE m.status = 'delivered'),
			COUNT(m.id) FILTER (WHERE m.status = 'failed'),
			COUNT(m.id) FILTER (WHERE m.status = 'unknown')
		FROM campaigns c
		LEFT JOIN messages m
		  ON m.campaign_id = c.id
		 AND m.tenant_id = c.tenant_id
		WHERE c.id = $1
		  AND c.tenant_id = $2
		GROUP BY c.id
	`, id, tenantID).Scan(
		&campaignID, &status, &senderID, &count,
		&createdAt, &scheduledAt,
		&queued, &submitting, &sent, &pending,
		&delivered, &failed, &unknown,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "campaign_not_found",
		})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "campaign_lookup_failed",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"campaign_id":     campaignID,
		"status":          status,
		"sender_id":       senderID,
		"recipient_count": count,
		"created_at":      createdAt,
		"scheduled_at":    scheduledAt,
		"messages": map[string]int64{
			"queued":     queued,
			"submitting": submitting,
			"sent":       sent,
			"pending":    pending,
			"delivered":  delivered,
			"failed":     failed,
			"unknown":    unknown,
		},
	})
}
