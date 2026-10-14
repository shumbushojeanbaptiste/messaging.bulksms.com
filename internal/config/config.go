package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Environment string
	Address     string
	DatabaseURL string

	ProviderName    string
	ProviderBaseURL string
	ProviderAPIID   string
	ProviderAPIKey  string
	ProviderSender  string
	ProviderTimeout time.Duration

	IdentityServiceURL           string
	IdentityInternalServiceToken string
	BillingServiceURL            string
	BillingInternalServiceToken  string
	AdminInternalToken           string

	KafkaBrokers          []string
	KafkaTopic            string
	KafkaGroupID          string
	OutboxBatchSize       int
	MaxCampaignRecipients int
	MetricsAddress        string
}

func Load() (Config, error) {
	// Environment variables set by Windows take precedence over .env.
	_ = godotenv.Load(".env")

	cfg := Config{
		Environment:     getEnv("APP_ENV", "development"),
		Address:         getEnv("APP_ADDR", ":8080"),
		DatabaseURL:     strings.TrimSpace(os.Getenv("DATABASE_URL")),
		ProviderName:    getEnv("SMS_PROVIDER_NAME", "hdev"),
		ProviderBaseURL: strings.TrimSpace(os.Getenv("SMS_PROVIDER_BASE_URL")),
		ProviderAPIID:   strings.TrimSpace(os.Getenv("SMS_PROVIDER_API_ID")),
		ProviderAPIKey:  strings.TrimSpace(os.Getenv("SMS_PROVIDER_API_KEY")),
		ProviderSender:  getEnv("SMS_PROVIDER_SENDER_ID", "N-SMS"),
		IdentityServiceURL: strings.TrimSpace(
			os.Getenv("IDENTITY_SERVICE_URL"),
		),
		IdentityInternalServiceToken: strings.TrimSpace(
			os.Getenv("IDENTITY_INTERNAL_SERVICE_TOKEN"),
		),
		BillingServiceURL: strings.TrimSpace(
			os.Getenv("BILLING_SERVICE_URL"),
		),
		BillingInternalServiceToken: strings.TrimSpace(
			os.Getenv("BILLING_INTERNAL_SERVICE_TOKEN"),
		),
		AdminInternalToken: strings.TrimSpace(
			os.Getenv("CENTRAL_SMS_ADMIN_INTERNAL_TOKEN"),
		),

		KafkaTopic:   getEnv("KAFKA_TOPIC", "sms.dispatch.v1"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "central-sms-dispatchers"),
		MetricsAddress: getEnv(
			"WORKER_METRICS_ADDR", "127.0.0.1:9091",
		),
	}

	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	for _, broker := range strings.Split(brokers, ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS is required")
	}

	timeout, err := getInt("SMS_PROVIDER_TIMEOUT_SECONDS", 15, 1, 120)
	if err != nil {
		return Config{}, err
	}
	cfg.ProviderTimeout = time.Duration(timeout) * time.Second

	cfg.OutboxBatchSize, err = getInt("OUTBOX_BATCH_SIZE", 100, 1, 1000)
	if err != nil {
		return Config{}, err
	}

	cfg.MaxCampaignRecipients, err = getInt(
		"MAX_CAMPAIGN_RECIPIENTS", 10000, 1, 10000,
	)
	if err != nil {
		return Config{}, err
	}

	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}

	if cfg.ProviderBaseURL == "" ||
		cfg.ProviderAPIID == "" ||
		cfg.ProviderAPIKey == "" {
		return Config{}, errors.New(
			"SMS provider base URL, API ID, and API key are required",
		)
	}

	if cfg.ProviderName == "" ||
		cfg.ProviderSender == "" ||
		cfg.KafkaTopic == "" ||
		cfg.KafkaGroupID == "" {
		return Config{}, errors.New(
			"provider and Kafka names are required",
		)
	}

	if cfg.MetricsAddress == "" {
		return Config{}, errors.New("WORKER_METRICS_ADDR is required")
	}

	if cfg.IdentityServiceURL == "" ||
		cfg.IdentityInternalServiceToken == "" {
		return Config{}, errors.New(
			"identity service URL and internal service token are required",
		)
	}

	if cfg.BillingServiceURL == "" ||
		cfg.BillingInternalServiceToken == "" {
		return Config{}, errors.New(
			"billing service URL and internal service token are required",
		)
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getInt(key string, fallback, min, max int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	n, err := strconv.Atoi(value)
	if err != nil || n < min || n > max {
		return 0, errors.New(
			key + " must be an integer between " +
				strconv.Itoa(min) + " and " + strconv.Itoa(max),
		)
	}

	return n, nil
}
