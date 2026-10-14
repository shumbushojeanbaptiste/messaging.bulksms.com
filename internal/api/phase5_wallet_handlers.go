// Add as a new .go file in package api. Assumes Phase 4 Server has Billing
// *billingclient.Client and Phase 3 has authorizeTenant.
package api

import (
	"context"
	"net/http"
	"strconv"
	"time"
)

// Add Billing *billingclient.Client to api.Server if not already present.

func (s *Server) customerWallet(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "wallet:read", "", "", "", 0, "")
	if !ok {
		return
	}
	if s.Billing == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing_service_unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	wallet, err := s.Billing.GetWallet(ctx, auth.TenantID)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "wallet_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, wallet)
}

func (s *Server) customerWalletTransactions(w http.ResponseWriter, r *http.Request) {
	auth, ok := s.authorizeTenant(w, r, "wallet:transactions:read", "", "", "", 0, "")
	if !ok {
		return
	}
	if s.Billing == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "billing_service_unavailable"})
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_limit"})
			return
		}
		limit = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.Billing.GetWalletTransactions(ctx, auth.TenantID, limit)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "wallet_transactions_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, result)
}
