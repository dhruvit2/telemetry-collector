package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for telemetry collector
type Config struct {
	// Service configuration
	ServiceName string
	ServiceID   string
	HealthPort  int
	LogLevel    string

	// Collector configuration
	ReadFrequencyMs int // How often to collect metrics (milliseconds)
	MaxRetries      int // Max retry attempts for collection

	// Message Broker configuration
	BrokerAddresses []string
	Topic           string

	// Collection configuration
	BatchSize      int
	BatchTimeoutMs int
	MaxPollRecords int

	// TSDB configuration
	TSDBEnabled bool
	TSDBURL     string
	TSDBToken   string
	TSDBOrg     string
	TSDBBucket  string

	// Resilience configuration
	CircuitBreakerThreshold int
	HealthCheckInterval     time.Duration

	// Shutdown configuration
	GracefulShutdownTimeout time.Duration
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	cfg := &Config{
		ServiceName: getEnv("SERVICE_NAME", "telemetry-collector"),
		ServiceID:   getEnv("SERVICE_ID", "1"),
		HealthPort:  getEnvInt("HEALTH_PORT", 8081),
		LogLevel:    getEnv("LOG_LEVEL", "info"),

		ReadFrequencyMs: getEnvInt("READ_FREQUENCY_MS", 1000), // Collect every 1 second
		MaxRetries:      getEnvInt("MAX_RETRIES", 5),

		BrokerAddresses: parseAddresses(getEnv("BROKER_ADDRESSES", "localhost:9092")),
		Topic:           getEnv("TOPIC", "telemetry-data"),

		BatchSize:      getEnvInt("BATCH_SIZE", 50),
		BatchTimeoutMs: getEnvInt("BATCH_TIMEOUT_MS", 1000),
		MaxPollRecords: getEnvInt("MAX_POLL_RECORDS", 100),

		// TSDB configuration
		TSDBEnabled: getEnvBool("TSDB_ENABLED", true),
		TSDBURL:     getEnv("TSDB_URL", "http://localhost:8086"),
		TSDBToken:   getEnv("TSDB_TOKEN", ""),
		TSDBOrg:     getEnv("TSDB_ORG", "telemetry"),
		TSDBBucket:  getEnv("TSDB_BUCKET", "gpu_metrics_raw"),

		CircuitBreakerThreshold: getEnvInt("CIRCUIT_BREAKER_THRESHOLD", 10),
		HealthCheckInterval:     time.Duration(getEnvInt("HEALTH_CHECK_INTERVAL", 30)) * time.Second,

		GracefulShutdownTimeout: time.Duration(getEnvInt("GRACEFUL_SHUTDOWN_TIMEOUT", 30)) * time.Second,
	}

	return cfg
}

// Helper functions
func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	valStr := getEnv(key, "")
	if valStr == "" {
		return defaultVal
	}
	return strings.ToLower(valStr) == "true" || valStr == "1" || strings.ToLower(valStr) == "yes"
}

// parseAddresses parses broker addresses from comma-separated string
func parseAddresses(addressStr string) []string {
	addresses := []string{}
	if addressStr == "" {
		addresses = append(addresses, "localhost:9092")
		return addresses
	}

	parts := strings.Split(addressStr, ",")
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			addresses = append(addresses, trimmed)
		}
	}

	if len(addresses) == 0 {
		addresses = append(addresses, "localhost:9092")
	}

	return addresses
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.ReadFrequencyMs <= 0 {
		return fmt.Errorf("READ_FREQUENCY_MS must be > 0")
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MAX_RETRIES must be >= 0")
	}
	if len(c.BrokerAddresses) == 0 {
		return fmt.Errorf("BROKER_ADDRESSES cannot be empty")
	}
	return nil
}
