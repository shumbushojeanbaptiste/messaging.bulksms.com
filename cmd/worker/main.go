package main

import (
	"context"
	"expvar"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/billingclient"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/config"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/providers/hdev"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/queue"
	"github.com/shumbushojeanbaptiste/central-sms-platform/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("configuration error: ", err)
	}

	startCtx, startCancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer startCancel()

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatal("invalid database configuration")
	}

	pool, err := pgxpool.NewWithConfig(startCtx, poolConfig)
	if err != nil {
		log.Fatal("could not initialize database connection")
	}
	defer pool.Close()

	if err := pool.Ping(startCtx); err != nil {
		log.Fatal("database connection failed")
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

	billingClient, err := billingclient.New(
		cfg.BillingServiceURL,
		cfg.BillingInternalServiceToken,
	)
	if err != nil {
		log.Fatal("billing client configuration error: ", err)
	}

	writer := queue.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Kafka writer shutdown encountered an error")
		}
	}()

	reader := queue.NewReader(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.KafkaGroupID,
	)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Kafka reader shutdown encountered an error")
		}
	}()

	appCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	app := worker.New(
		pool,
		provider,
		writer,
		reader,
		cfg.OutboxBatchSize,
	)
	app.Billing = billingClient

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/debug/vars", expvar.Handler())
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddress,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("Worker metrics listening on %s/debug/vars", cfg.MetricsAddress)
		if err := metricsServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			log.Printf("Worker metrics server failed: %v", err)
			stop()
		}
	}()

	log.Printf(
		"SMS worker started; Kafka topic=%s",
		cfg.KafkaTopic,
	)

	app.Run(appCtx)
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("Worker metrics shutdown failed: %v", err)
	}
	log.Println("SMS worker stopped")
}
