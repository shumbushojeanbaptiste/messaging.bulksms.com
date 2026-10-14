// Phase 5 reporting handlers for central-sms-platform.
// Copy into package api after confirming imports and names in your repo.
// Requires the existing Server, DB, authorizeTenant, writeJSON, and current schema.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type reportCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func encodeReportCursor(c reportCursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeReportCursor(raw string) (*reportCursor, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var c reportCursor
	if err := json.Unmarshal(data, &c); err != nil || c.CreatedAt.IsZero() {
		return nil, errors.New("invalid cursor")
	}
	if _, err := uuid.Parse(c.ID); err != nil {
		return nil, errors.New("invalid cursor")
	}
	return &c, nil
}

func parseReportLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 50, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 100 {
		return 0, errors.New("limit must be between 1 and 100")
	}
	return n, nil
}

func parseReportTime(raw string) (any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return nil, errors.New("date must be RFC3339 / ISO 8601")
	}
	return t.UTC(), nil
}

func allowedMessageStatus(v string) bool {
	switch v {
	case "queued", "submitting", "sent", "pending", "delivered", "failed", "unknown", "accepted":
		return true
	default:
		return false
	}
}

func (s *Server) listMessages(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "messages:read", "", "", "", 0, "")
	if !ok {
		return
	}

	limit, err := parseReportLimit(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	if status != "" && !allowedMessageStatus(status) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_status_filter"})
		return
	}
	recipient := strings.TrimSpace(r.URL.Query().Get("recipient"))
	if len(recipient) > 30 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "recipient_filter_too_long"})
		return
	}
	from, err := parseReportTime(r.URL.Query().Get("from"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_from_date"})
		return
	}
	to, err := parseReportTime(r.URL.Query().Get("to"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_to_date"})
		return
	}
	if from != nil && to != nil && to.(time.Time).Before(from.(time.Time)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_date_range"})
		return
	}
	cursor, err := decodeReportCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_cursor"})
		return
	}
	var cursorTime, cursorID any
	if cursor != nil {
		cursorTime, cursorID = cursor.CreatedAt, cursor.ID
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.DB.Query(ctx, `
        SELECT id::text, COALESCE(campaign_id::text, ''), recipient, sender_id,
               status, COALESCE(provider_message_id, ''),
               COALESCE(external_message_id, ''), COALESCE(error_code, ''),
               created_at, updated_at
        FROM messages
        WHERE tenant_id = $1
          AND ($2::text = '' OR status = $2)
          AND ($3::text = '' OR recipient LIKE '%' || $3 || '%')
          AND ($4::timestamptz IS NULL OR created_at >= $4)
          AND ($5::timestamptz IS NULL OR created_at < $5)
          AND ($6::timestamptz IS NULL OR (created_at, id) < ($6::timestamptz, $7::uuid))
        ORDER BY created_at DESC, id DESC
        LIMIT $8
    `, auth.TenantID, status, recipient, from, to, cursorTime, cursorID, limit+1)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_list_failed"})
		return
	}
	defer rows.Close()

	type item struct {
		ID                string    `json:"id"`
		CampaignID        string    `json:"campaign_id,omitempty"`
		Recipient         string    `json:"recipient"`
		SenderID          string    `json:"sender_id"`
		Status            string    `json:"status"`
		ProviderMessageID string    `json:"provider_message_id,omitempty"`
		ExternalMessageID string    `json:"external_message_id,omitempty"`
		ErrorCode         string    `json:"error_code,omitempty"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}
	items := make([]item, 0, limit+1)
	for rows.Next() {
		var v item
		if err := rows.Scan(&v.ID, &v.CampaignID, &v.Recipient, &v.SenderID,
			&v.Status, &v.ProviderMessageID, &v.ExternalMessageID, &v.ErrorCode,
			&v.CreatedAt, &v.UpdatedAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_list_failed"})
			return
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_list_failed"})
		return
	}
	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = encodeReportCursor(reportCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "next_cursor": next})
}

func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "sms:status:read", "", "", "", 0, "")
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_message_id"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var v struct {
		ID                string    `json:"id"`
		CampaignID        string    `json:"campaign_id,omitempty"`
		Recipient         string    `json:"recipient"`
		SenderID          string    `json:"sender_id"`
		Message           string    `json:"message"`
		Status            string    `json:"status"`
		ProviderMessageID string    `json:"provider_message_id,omitempty"`
		ExternalMessageID string    `json:"external_message_id,omitempty"`
		ErrorCode         string    `json:"error_code,omitempty"`
		ErrorMessage      string    `json:"error_message,omitempty"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}
	err = s.DB.QueryRow(ctx, `
        SELECT id::text, COALESCE(campaign_id::text, ''), recipient, sender_id,
               message_body, status, COALESCE(provider_message_id, ''),
               COALESCE(external_message_id, ''), COALESCE(error_code, ''),
               COALESCE(error_message, ''), created_at, updated_at
        FROM messages WHERE id = $1 AND tenant_id = $2
    `, id, auth.TenantID).Scan(&v.ID, &v.CampaignID, &v.Recipient, &v.SenderID,
		&v.Message, &v.Status, &v.ProviderMessageID, &v.ExternalMessageID,
		&v.ErrorCode, &v.ErrorMessage, &v.CreatedAt, &v.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "message_not_found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_lookup_failed"})
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) getMessageHistory(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "sms:status:read", "", "", "", 0, "")
	if !ok {
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_message_id"})
		return
	}
	limit, err := parseReportLimit(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.DB.Query(ctx, `
        SELECT h.id, COALESCE(h.previous_status, ''), h.new_status,
               h.source, COALESCE(h.details, '{}'::jsonb)::text, h.created_at
        FROM message_status_history h
        JOIN messages m ON m.id = h.message_id
        WHERE m.id = $1 AND m.tenant_id = $2
        ORDER BY h.created_at DESC, h.id DESC
        LIMIT $3
    `, id, auth.TenantID, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_history_failed"})
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, limit)
	for rows.Next() {
		var historyID int64
		var previous, next, source, details string
		var created time.Time
		if err := rows.Scan(&historyID, &previous, &next, &source, &details, &created); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_history_failed"})
			return
		}
		var raw json.RawMessage = []byte(details)
		items = append(items, map[string]any{
			"id": historyID, "previous_status": previous, "new_status": next,
			"source": source, "details": raw, "created_at": created,
		})
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "message_history_failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) listCampaigns(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "campaigns:read", "", "", "", 0, "")
	if !ok {
		return
	}
	limit, err := parseReportLimit(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
		return
	}
	status := strings.TrimSpace(r.URL.Query().Get("status"))
	cursor, err := decodeReportCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_cursor"})
		return
	}
	var cursorTime, cursorID any
	if cursor != nil {
		cursorTime, cursorID = cursor.CreatedAt, cursor.ID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.DB.Query(ctx, `
        SELECT id::text, sender_id, recipient_count, status,
               COALESCE(scheduled_at::text, ''), created_at, updated_at
        FROM campaigns
        WHERE tenant_id = $1
          AND ($2::text = '' OR status = $2)
          AND ($3::timestamptz IS NULL OR (created_at, id) < ($3::timestamptz, $4::uuid))
        ORDER BY created_at DESC, id DESC
        LIMIT $5
    `, auth.TenantID, status, cursorTime, cursorID, limit+1)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "campaign_list_failed"})
		return
	}
	defer rows.Close()
	type item struct {
		ID             string    `json:"id"`
		SenderID       string    `json:"sender_id"`
		RecipientCount int       `json:"recipient_count"`
		Status         string    `json:"status"`
		ScheduledAt    string    `json:"scheduled_at,omitempty"`
		CreatedAt      time.Time `json:"created_at"`
		UpdatedAt      time.Time `json:"updated_at"`
	}
	items := make([]item, 0, limit+1)
	for rows.Next() {
		var v item
		if err := rows.Scan(&v.ID, &v.SenderID, &v.RecipientCount, &v.Status,
			&v.ScheduledAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "campaign_list_failed"})
			return
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "campaign_list_failed"})
		return
	}
	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = encodeReportCursor(reportCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "next_cursor": next})
}

func (s *Server) listCampaignMessages(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "campaigns:recipients:read", "", "", "", 0, "")
	if !ok {
		return
	}
	campaignID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_campaign_id"})
		return
	}
	limit, err := parseReportLimit(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
		return
	}
	cursor, err := decodeReportCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_cursor"})
		return
	}
	var cursorTime, cursorID any
	if cursor != nil {
		cursorTime, cursorID = cursor.CreatedAt, cursor.ID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.DB.Query(ctx, `
        SELECT m.id::text, m.recipient, m.status,
               COALESCE(m.provider_message_id, ''), COALESCE(m.external_message_id, ''),
               COALESCE(m.error_code, ''), m.created_at, m.updated_at
        FROM messages m
        JOIN campaigns c ON c.id = m.campaign_id
        WHERE c.id = $1 AND c.tenant_id = $2 AND m.tenant_id = $2
          AND ($3::timestamptz IS NULL OR (m.created_at, m.id) < ($3::timestamptz, $4::uuid))
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT $5
    `, campaignID, auth.TenantID, cursorTime, cursorID, limit+1)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "campaign_messages_failed"})
		return
	}
	defer rows.Close()
	type item struct {
		ID                string    `json:"id"`
		Recipient         string    `json:"recipient"`
		Status            string    `json:"status"`
		ProviderMessageID string    `json:"provider_message_id,omitempty"`
		ExternalMessageID string    `json:"external_message_id,omitempty"`
		ErrorCode         string    `json:"error_code,omitempty"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}
	items := make([]item, 0, limit+1)
	for rows.Next() {
		var v item
		if err := rows.Scan(&v.ID, &v.Recipient, &v.Status, &v.ProviderMessageID,
			&v.ExternalMessageID, &v.ErrorCode, &v.CreatedAt, &v.UpdatedAt); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "campaign_messages_failed"})
			return
		}
		items = append(items, v)
	}
	if err := rows.Err(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "campaign_messages_failed"})
		return
	}
	next := ""
	if len(items) > limit {
		last := items[limit-1]
		next = encodeReportCursor(reportCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		items = items[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "next_cursor": next})
}

func (s *Server) dashboardSummary(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "dashboard:read", "", "", "", 0, "")
	if !ok {
		return
	}
	from, err := parseReportTime(r.URL.Query().Get("from"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_from_date"})
		return
	}
	to, err := parseReportTime(r.URL.Query().Get("to"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_to_date"})
		return
	}
	now := time.Now().UTC()
	if from == nil {
		from = now.AddDate(0, 0, -7)
	}
	if to == nil {
		to = now
	}
	if to.(time.Time).Sub(from.(time.Time)) > 90*24*time.Hour || !to.(time.Time).After(from.(time.Time)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_date_range"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var total, queued, submitting, sent, pending, delivered, failed, unknown int64
	err = s.DB.QueryRow(ctx, `
        SELECT COUNT(*),
          COUNT(*) FILTER (WHERE status = 'queued'),
          COUNT(*) FILTER (WHERE status = 'submitting'),
          COUNT(*) FILTER (WHERE status = 'sent'),
          COUNT(*) FILTER (WHERE status = 'pending'),
          COUNT(*) FILTER (WHERE status = 'delivered'),
          COUNT(*) FILTER (WHERE status = 'failed'),
          COUNT(*) FILTER (WHERE status = 'unknown')
        FROM messages WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
    `, auth.TenantID, from, to).Scan(&total, &queued, &submitting, &sent,
		&pending, &delivered, &failed, &unknown)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "dashboard_summary_failed"})
		return
	}
	var campaignTotal, processing, submitted, partial, campaignFailed int64
	err = s.DB.QueryRow(ctx, `
        SELECT COUNT(*),
          COUNT(*) FILTER (WHERE status IN ('queued', 'scheduled', 'processing')),
          COUNT(*) FILTER (WHERE status = 'submitted'),
          COUNT(*) FILTER (WHERE status = 'partial'),
          COUNT(*) FILTER (WHERE status = 'failed')
        FROM campaigns WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
    `, auth.TenantID, from, to).Scan(&campaignTotal, &processing, &submitted, &partial, &campaignFailed)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "dashboard_summary_failed"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"period": map[string]any{"from": from, "to": to},
		"messages": map[string]int64{
			"total": total, "queued": queued, "submitting": submitting,
			"sent": sent, "pending": pending, "delivered": delivered,
			"failed": failed, "unknown": unknown,
		},
		"campaigns": map[string]int64{
			"total": campaignTotal, "processing": processing,
			"submitted": submitted, "partial": partial, "failed": campaignFailed,
		},
	})
}
