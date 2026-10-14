package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/billingclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/identityclient"
)

func (s *Server) authorizeTenant(
	w http.ResponseWriter,
	r *http.Request,
	permission, senderID, requestKey, requestHash string,
	messageCount int,
	purpose string,
) (*identityclient.Authorization, bool) {
	if s.Identity == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "identity_service_unavailable",
		})
		return nil, false
	}

	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if apiKey == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{
			"error": "api_key_required",
		})
		return nil, false
	}
	requestKey = strings.TrimSpace(requestKey)
	if purpose != "" && requestKey == "" {
		requestKey = uuid.NewString()
	}

	result, err := s.Identity.Authorize(r.Context(), identityclient.AuthorizeRequest{
		APIKey:       apiKey,
		Permission:   permission,
		SenderID:     senderID,
		RequestKey:   requestKey,
		RequestHash:  requestHash,
		MessageCount: messageCount,
		Purpose:      purpose,
	})
	if err != nil {
		var apiErr *identityclient.APIError
		if errors.As(err, &apiErr) {
			status := apiErr.StatusCode
			code := apiErr.Code
			if status < 400 || status > 599 {
				status = http.StatusBadGateway
			}
			if code == "" {
				code = "identity_authorization_failed"
			}
			writeJSON(w, status, map[string]string{"error": code})
			return nil, false
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "identity_service_unavailable",
		})
		return nil, false
	}

	if _, err := uuid.Parse(result.TenantID); err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{
			"error": "identity_service_invalid_response",
		})
		return nil, false
	}
	return result, true
}

func (s *Server) commitQuotaBestEffort(reservationID string) error {
	if s.Identity == nil || reservationID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Identity.Commit(ctx, reservationID)
}

func (s *Server) releaseQuotaBestEffort(reservationID string) error {
	if s.Identity == nil || reservationID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Identity.Release(ctx, reservationID)
}

func (s *Server) reserveBilling(
	ctx context.Context,
	request billingclient.ReservationRequest,
) (billingclient.Reservation, error) {
	if s.Billing == nil {
		return billingclient.Reservation{}, errors.New("billing client is unavailable")
	}
	return s.Billing.Reserve(ctx, request)
}

func (s *Server) releaseBillingBestEffort(reservationID string) error {
	if s.Billing == nil || reservationID == "" {
		return errors.New("billing client or reservation ID is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := s.Billing.Release(ctx, reservationID)
	return err
}

func writeBillingError(w http.ResponseWriter, err error) {
	var apiErr *billingclient.APIError
	if errors.As(err, &apiErr) {
		status := apiErr.StatusCode
		code := apiErr.Code
		if status < 400 || status > 599 {
			status = http.StatusBadGateway
		}
		if code == "" {
			code = "billing_reservation_failed"
		}
		writeJSON(w, status, map[string]string{"error": code})
		return
	}

	writeJSON(w, http.StatusServiceUnavailable, map[string]string{
		"error": "billing_service_unavailable",
	})
}
