package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/api"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/billingclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/config"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/identityclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/providers/hdev"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("configuration error: ", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatal("could not initialize database connection")
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatal("database connection failed")
	}

	// Store provider metadata only. The provider API key stays
	// in environment-based configuration.
	var providerID int64
	err = pool.QueryRow(ctx, `
		INSERT INTO providers (name, base_url, api_id, secret_ref)
		VALUES ($1, $2, $3, 'environment')
		ON CONFLICT (name) DO UPDATE
		SET base_url = EXCLUDED.base_url,
		    api_id = EXCLUDED.api_id,
		    updated_at = NOW()
		RETURNING id
	`, cfg.ProviderName, cfg.ProviderBaseURL,
		cfg.ProviderAPIID).Scan(&providerID)
	if err != nil {
		log.Fatal("could not initialize provider record")
	}

	provider, err := hdev.New(hdev.Config{
		BaseURL: cfg.ProviderBaseURL,
		APIID:   cfg.ProviderAPIID,
		APIKey:  cfg.ProviderAPIKey,
		Timeout: cfg.ProviderTimeout,
	})
	if err != nil {
		log.Fatal("provider configuration error: ", err)
	}

	identityClient, err := identityclient.New(
		cfg.IdentityServiceURL,
		cfg.IdentityInternalServiceToken,
	)
	if err != nil {
		log.Fatal("identity client configuration error: ", err)
	}

	billingClient, err := billingclient.New(
		cfg.BillingServiceURL,
		cfg.BillingInternalServiceToken,
	)
	if err != nil {
		log.Fatal("billing client configuration error: ", err)
	}

	server := &api.Server{
		DB:                 pool,
		Provider:           provider,
		SenderID:           cfg.ProviderSender,
		ProviderID:         providerID,
		Identity:           identityClient,
		Billing:            billingClient,
		AdminInternalToken: cfg.AdminInternalToken,
	}

	mux := http.NewServeMux()
	server.Register(mux)

	httpServer := &http.Server{
		Addr:              cfg.Address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("SMS API listening on %s", cfg.Address)
		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server failed: %v", err)
			os.Exit(1)
		}
	}()

	stop, stopCancel := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM,
	)
	defer stopCancel()
	<-stop.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
