// Package identityclient calls the independently deployed Central SMS Identity Service.
package identityclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

type AuthorizeRequest struct {
	APIKey       string `json:"api_key"`
	Permission   string `json:"permission"`
	SenderID     string `json:"sender_id,omitempty"`
	RequestKey   string `json:"request_key,omitempty"`
	RequestHash  string `json:"request_hash,omitempty"`
	MessageCount int    `json:"message_count,omitempty"`
	Purpose      string `json:"purpose,omitempty"`
}

type Authorization struct {
	TenantID              string `json:"tenant_id"`
	APIKeyID              string `json:"api_key_id"`
	Role                  string `json:"role"`
	Permission            string `json:"permission"`
	DailySMSLimit         int64  `json:"daily_sms_limit"`
	DailySMSUsed          int64  `json:"daily_sms_used"`
	MaxCampaignRecipients int    `json:"max_campaign_recipients"`
	ReservationID         string `json:"reservation_id,omitempty"`
	IdempotentReplay      bool   `json:"idempotent_replay"`
}

type APIError struct {
	StatusCode int
	Code       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("identity service request rejected: HTTP %d (%s)", e.StatusCode, e.Code)
}

func New(baseURL, internalToken string) (*Client, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" || internalToken == "" {
		return nil, errors.New("identity service URL and internal token are required")
	}
	return &Client{
		baseURL: baseURL,
		token:   internalToken,
		http:    &http.Client{Timeout: 4 * time.Second},
	}, nil
}

func (c *Client) Authorize(ctx context.Context, reqBody AuthorizeRequest) (*Authorization, error) {
	var result Authorization
	if err := c.post(ctx, "/internal/v1/authorize", reqBody, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) Commit(ctx context.Context, reservationID string) error {
	return c.post(ctx, "/internal/v1/quota/commit", map[string]string{
		"reservation_id": reservationID,
	}, nil)
}

func (c *Client) Release(ctx context.Context, reservationID string) error {
	return c.post(ctx, "/internal/v1/quota/release", map[string]string{
		"reservation_id": reservationID,
	}, nil)
}

func (c *Client) post(ctx context.Context, path string, body any, output any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return errors.New("could not encode identity service request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return errors.New("could not create identity service request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Internal-Service-Token", c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		// Never include request headers or credential values in errors.
		return errors.New("identity service unavailable")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return errors.New("could not read identity service response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &apiErr)
		return &APIError{StatusCode: resp.StatusCode, Code: apiErr.Error}
	}
	if output != nil {
		if err := json.Unmarshal(data, output); err != nil {
			return errors.New("identity service returned invalid JSON")
		}
	}
	return nil
}
