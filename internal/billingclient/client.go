// Package billingclient is copied into central-sms-platform. It deliberately
// exposes only internal billing operations; customer authentication remains
// the responsibility of central-sms-identity-service and the messaging API.
package billingclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

type ReservationRequest struct {
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Quantity       int64  `json:"quantity"`
	Currency       string `json:"currency"`
	ReferenceType  string `json:"reference_type"`
	ReferenceID    string `json:"reference_id"`
}

type Reservation struct {
	ID               string `json:"reservation_id"`
	TenantID         string `json:"tenant_id"`
	Quantity         int64  `json:"quantity"`
	UnitCode         string `json:"unit_code"`
	UnitPriceMinor   int64  `json:"unit_price_minor"`
	AmountMinor      int64  `json:"amount_minor"`
	Currency         string `json:"currency"`
	PricingVersion   int64  `json:"pricing_version"`
	Status           string `json:"status"`
	IdempotentReplay bool   `json:"idempotent_replay,omitempty"`
}

type APIError struct {
	StatusCode int
	Code       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("billing service returned HTTP %d (%s)", e.StatusCode, e.Code)
}

func New(baseURL, token string) (*Client, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !strings.HasPrefix(baseURL, "https://") && !strings.HasPrefix(baseURL, "http://localhost") && !strings.HasPrefix(baseURL, "http://127.0.0.1") {
		return nil, errors.New("billing service URL must use HTTPS except for local development")
	}
	if token == "" {
		return nil, errors.New("billing internal token is required")
	}
	return &Client{baseURL: baseURL, token: token, http: &http.Client{Timeout: 6 * time.Second}}, nil
}

func (c *Client) Reserve(ctx context.Context, req ReservationRequest) (Reservation, error) {
	var out Reservation
	err := c.do(ctx, http.MethodPost, "/internal/v1/reservations", req, &out)
	return out, err
}
func (c *Client) Capture(ctx context.Context, id string) (Reservation, error) {
	var out Reservation
	err := c.do(ctx, http.MethodPost, "/internal/v1/reservations/"+id+"/capture", map[string]any{}, &out)
	return out, err
}
func (c *Client) Release(ctx context.Context, id string) (Reservation, error) {
	var out Reservation
	err := c.do(ctx, http.MethodPost, "/internal/v1/reservations/"+id+"/release", map[string]any{}, &out)
	return out, err
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return errors.New("could not encode billing request")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(b))
	if err != nil {
		return errors.New("could not create billing request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Internal-Service-Token", c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("billing service unavailable")
	}
	defer resp.Body.Close()
	var payload map[string]json.RawMessage
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		code := "billing_request_failed"
		if raw, ok := payload["error"]; ok {
			_ = json.Unmarshal(raw, &code)
		}
		return &APIError{StatusCode: resp.StatusCode, Code: code}
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.New("invalid billing service response")
	}
	return nil
}
