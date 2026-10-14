package hdev

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	BaseURL string
	APIID   string
	APIKey  string
	Timeout time.Duration
}

type Client struct {
	baseURL string
	apiID   string
	apiKey  string
	http    *http.Client
}

type Response struct {
	Status            string          `json:"status"`
	Message           string          `json:"message"`
	Link              string          `json:"link"`
	SID               json.RawMessage `json:"s_id"`
	Tel               string          `json:"tel"`
	ExternalMessageID *string         `json:"external_message_id"`
	DeliveryStatus    string          `json:"delivery_status"`
}

func New(cfg Config) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")

	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" ||
		(u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid SMS provider base URL")
	}

	if u.Scheme != "https" {
		return nil, errors.New("SMS provider must use HTTPS")
	}

	if strings.TrimSpace(cfg.APIID) == "" ||
		strings.TrimSpace(cfg.APIKey) == "" {
		return nil, errors.New("SMS provider credentials are required")
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}

	return &Client{
		baseURL: baseURL,
		apiID:   cfg.APIID,
		apiKey:  cfg.APIKey,
		http: &http.Client{
			Timeout: cfg.Timeout,
		},
	}, nil
}

func (c *Client) Send(
	ctx context.Context,
	senderID string,
	tel string,
	message string,
) (*Response, error) {
	return c.post(ctx, url.Values{
		"ref":       {"sms"},
		"sender_id": {senderID},
		"tel":       {tel},
		"message":   {message},
	})
}

func (c *Client) CheckStatus(
	ctx context.Context,
	providerMessageID string,
) (*Response, error) {
	return c.post(ctx, url.Values{
		"ref":  {"sms-status"},
		"s_id": {providerMessageID},
	})
}

func (c *Client) post(
	ctx context.Context,
	form url.Values,
) (*Response, error) {
	endpoint := fmt.Sprintf(
		"%s/api/%s/%s",
		c.baseURL,
		url.PathEscape(c.apiID),
		url.PathEscape(c.apiKey),
	)

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return nil, errors.New("could not create provider request")
	}

	req.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded",
	)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		// Do not expose the underlying error: it may contain
		// the URL, including credentials in its path.
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, errors.New("SMS provider request timed out")
		case errors.Is(err, context.Canceled):
			return nil, errors.New("SMS provider request was canceled")
		default:
			return nil, errors.New("SMS provider transport failed before a response")
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, errors.New("could not read provider response")
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf(
			"SMS provider returned HTTP status %d",
			resp.StatusCode,
		)
	}

	var result Response
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, errors.New("SMS provider returned invalid JSON")
	}

	return &result, nil
}

// SIDString handles providers that return s_id as either
// a JSON number or a JSON string.
func (r *Response) SIDString() string {
	if len(r.SID) == 0 ||
		string(r.SID) == "null" {
		return ""
	}

	var value string
	if err := json.Unmarshal(r.SID, &value); err == nil {
		return value
	}

	var number json.Number
	if err := json.Unmarshal(r.SID, &number); err == nil {
		return number.String()
	}

	return strings.Trim(string(r.SID), `"`)
}

// SafeRawJSON converts a provider response into JSONB-compatible
// data for persistence.
// func SafeRawJSON(value *Response) []byte {
// 	if value == nil {
// 		return []byte(`{}`)
// 	}

// 	raw, err := json.Marshal(value)
// 	if err != nil || !bytes.Contains(raw, []byte(`"api_key"`)) {
// 		if err == nil {
// 			return raw
// 		}
// 	}
// 	return []byte(`{}`)
// }

func SafeRawJSON(value *Response) []byte {
	if value == nil {
		return []byte(`{}`)
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return []byte(`{}`)
	}

	return raw
}
