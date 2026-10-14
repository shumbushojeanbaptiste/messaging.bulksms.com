// Add this as read_methods.go in the existing package billingclient.
// It reuses the unexported Client fields (baseURL, token, http) from client.go.
package billingclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type WalletView struct {
	TenantID              string `json:"tenant_id"`
	Currency              string `json:"currency"`
	AvailableBalanceMinor int64  `json:"available_balance_minor"`
	ReservedBalanceMinor  int64  `json:"reserved_balance_minor"`
	TotalBalanceMinor     int64  `json:"total_balance_minor"`
	Status                string `json:"status"`
}

type WalletTransactionView struct {
	ID                         string    `json:"id"`
	Type                       string    `json:"transaction_type"`
	AmountMinor                int64     `json:"amount_minor"`
	Currency                   string    `json:"currency"`
	AvailableDeltaMinor        int64     `json:"available_delta_minor"`
	ReservedDeltaMinor         int64     `json:"reserved_delta_minor"`
	AvailableBalanceAfterMinor int64     `json:"available_balance_after_minor"`
	ReservedBalanceAfterMinor  int64     `json:"reserved_balance_after_minor"`
	ReferenceType              string    `json:"reference_type"`
	ReferenceID                string    `json:"reference_id"`
	Description                string    `json:"description"`
	CreatedAt                  time.Time `json:"created_at"`
}

type WalletTransactionsView struct {
	TenantID     string                  `json:"tenant_id"`
	Transactions []WalletTransactionView `json:"transactions"`
}

func (c *Client) GetWallet(ctx context.Context, tenantID string) (WalletView, error) {
	var out WalletView
	path := "/internal/v1/wallets/" + url.PathEscape(tenantID)
	err := c.getJSON(ctx, path, &out)
	return out, err
}

func (c *Client) GetWalletTransactions(ctx context.Context, tenantID string, limit int) (WalletTransactionsView, error) {
	var out WalletTransactionsView
	if limit < 1 || limit > 100 {
		return out, errors.New("transaction limit must be between 1 and 100")
	}
	path := "/internal/v1/wallets/" + url.PathEscape(tenantID) + "/transactions?limit=" + strconv.Itoa(limit)
	err := c.getJSON(ctx, path, &out)
	return out, err
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return errors.New("could not create billing request")
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Internal-Service-Token", c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("billing service unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&apiErr)
		if apiErr.Error == "" {
			apiErr.Error = "billing_request_failed"
		}
		return fmt.Errorf("billing request rejected: HTTP %d (%s)", resp.StatusCode, apiErr.Error)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.New("invalid billing service response")
	}
	return nil
}
