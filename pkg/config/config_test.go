package config
package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		expected *Config
	}{
		{
			name: "default configuration",
			env:  map[string]string{},
			expected: &Config{
				ServiceName:             "telemetry-collector",
				ServiceID:               "1",
				HealthPort:              8080,
				LogLevel:                "info",
				ReadFrequencyMs:         1000,
				MaxRetries:              5,
				BrokerAddresses:         []string{"localhost:9092"},
				Topic:                   "telemetry-data",
				BatchSize:               50,
				BatchTimeoutMs:          1000,
				MaxPollRecords:          100,
				CircuitBreakerThreshold: 10,
				HealthCheckInterval:     30 * time.Second,
				GracefulShutdownTimeout: 30 * time.Second,
			},
		},
		{
			name: "custom configuration",
			env: map[string]string{
				"SERVICE_NAME":                "my-collector",
				"SERVICE_ID":                  "service-001",
				"HEALTH_PORT":                 "9000",
				"LOG_LEVEL":                   "debug",
				"READ_FREQUENCY_MS":           "2000",
				"MAX_RETRIES":                 "10",
				"BROKER_ADDRESSES":            "broker1:9092,broker2:9092",
				"TOPIC":                       "metrics",
				"BATCH_SIZE":                  "100",
				"BATCH_TIMEOUT_MS":            "5000",
				"MAX_POLL_RECORDS":            "200",
				"CIRCUIT_BREAKER_THRESHOLD":   "20",
				"HEALTH_CHECK_INTERVAL":       "60",
				"GRACEFUL_SHUTDOWN_TIMEOUT":   "60",
			},
			expected: &Config{
				ServiceName:             "my-collector",
				ServiceID:               "service-001",
				HealthPort:              9000,
				LogLevel:                "debug",
				ReadFrequencyMs:         2000,
				MaxRetries:              10,
				BrokerAddresses:         []string{"broker1:9092", "broker2:9092"},
				Topic:                   "metrics",
				BatchSize:               100,
				BatchTimeoutMs:          5000,
				MaxPollRecords:          200,
				CircuitBreakerThreshold: 20,
				HealthCheckInterval:     60 * time.Second,
				GracefulShutdownTimeout: 60 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			for _, key := range []string{
				"SERVICE_NAME", "SERVICE_ID", "HEALTH_PORT", "LOG_LEVEL",
				"READ_FREQUENCY_MS", "MAX_RETRIES", "BROKER_ADDRESSES", "TOPIC",
				"BATCH_SIZE", "BATCH_TIMEOUT_MS", "MAX_POLL_RECORDS",
				"CIRCUIT_BREAKER_THRESHOLD", "HEALTH_CHECK_INTERVAL",
				"GRACEFUL_SHUTDOWN_TIMEOUT",
			} {
				os.Unsetenv(key)
			}

			// Set test environment
			for key, value := range tt.env {
				os.Setenv(key, value)
			}
			defer func() {
				for key := range tt.env {
					os.Unsetenv(key)
				}
			}()

			cfg := LoadConfig()

			if cfg.ServiceName != tt.expected.ServiceName {
				t.Errorf("ServiceName = %v, want %v", cfg.ServiceName, tt.expected.ServiceName)
			}
			if cfg.ServiceID != tt.expected.ServiceID {
				t.Errorf("ServiceID = %v, want %v", cfg.ServiceID, tt.expected.ServiceID)
			}
			if cfg.HealthPort != tt.expected.HealthPort {
				t.Errorf("HealthPort = %v, want %v", cfg.HealthPort, tt.expected.HealthPort)
			}
			if cfg.LogLevel != tt.expected.LogLevel {
				t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, tt.expected.LogLevel)
			}
			if cfg.ReadFrequencyMs != tt.expected.ReadFrequencyMs {
				t.Errorf("ReadFrequencyMs = %v, want %v", cfg.ReadFrequencyMs, tt.expected.ReadFrequencyMs)
			}
			if cfg.MaxRetries != tt.expected.MaxRetries {
				t.Errorf("MaxRetries = %v, want %v", cfg.MaxRetries, tt.expected.MaxRetries)
			}
			if cfg.Topic != tt.expected.Topic {
				t.Errorf("Topic = %v, want %v", cfg.Topic, tt.expected.Topic)
			}
			if cfg.BatchSize != tt.expected.BatchSize {
				t.Errorf("BatchSize = %v, want %v", cfg.BatchSize, tt.expected.BatchSize)
			}

			// Compare broker addresses
			if len(cfg.BrokerAddresses) != len(tt.expected.BrokerAddresses) {
				t.Errorf("BrokerAddresses length = %v, want %v", len(cfg.BrokerAddresses), len(tt.expected.BrokerAddresses))
			}
			for i, addr := range cfg.BrokerAddresses {
				if i < len(tt.expected.BrokerAddresses) && addr != tt.expected.BrokerAddresses[i] {
					t.Errorf("BrokerAddresses[%d] = %v, want %v", i, addr, tt.expected.BrokerAddresses[i])
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid configuration",
			config: &Config{
				ReadFrequencyMs: 1000,
				MaxRetries:      5,
				BrokerAddresses: []string{"localhost:9092"},
			},
			wantErr: false,
		},
		{
			name: "invalid read frequency",
			config: &Config{
				ReadFrequencyMs: 0,
				MaxRetries:      5,
				BrokerAddresses: []string{"localhost:9092"},
			},
			wantErr: true,
			errMsg:  "read frequency",
		},
		{
			name: "negative read frequency",
			config: &Config{
				ReadFrequencyMs: -1,
				MaxRetries:      5,
				BrokerAddresses: []string{"localhost:9092"},
			},
			wantErr: true,
			errMsg:  "read frequency",
		},
		{
			name: "negative max retries",
			config: &Config{
				ReadFrequencyMs: 1000,
				MaxRetries:      -1,
				BrokerAddresses: []string{"localhost:9092"},
			},
			wantErr: true,
			errMsg:  "retries",
		},
		{
			name: "empty broker addresses",
			config: &Config{
				ReadFrequencyMs: 1000,
				MaxRetries:      5,
				BrokerAddresses: []string{},
			},
			wantErr: true,
			errMsg:  "broker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && tt.errMsg != "" {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("Validate() error message = %v, should contain %v", err, tt.errMsg)
				}
			}
		})
	}
}

func TestParseAddresses(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "single address",
			input:    "localhost:9092",
			expected: []string{"localhost:9092"},
		},
		{
			name:     "multiple addresses",
			input:    "broker1:9092,broker2:9092,broker3:9092",
			expected: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:     "addresses with spaces",
			input:    "broker1:9092, broker2:9092 , broker3:9092",
			expected: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{"localhost:9092"},
		},
		{
			name:     "only spaces",
			input:    "   ,  ,  ",
			expected: []string{"localhost:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAddresses(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("parseAddresses() length = %v, want %v", len(result), len(tt.expected))
				return
			}
			for i, addr := range result {
				if addr != tt.expected[i] {
					t.Errorf("parseAddresses()[%d] = %v, want %v", i, addr, tt.expected[i])
				}
			}
		})
	}
}

func TestGetEnv(t *testing.T) {
	key := "TEST_CONFIG_ENV_VAR"
	defaultVal := "default"

	// Test with unset variable
	os.Unsetenv(key)
	result := getEnv(key, defaultVal)
	if result != defaultVal {
		t.Errorf("getEnv() with unset var = %v, want %v", result, defaultVal)
	}

	// Test with set variable
	os.Setenv(key, "custom")
	defer os.Unsetenv(key)
	result = getEnv(key, defaultVal)
	if result != "custom" {
		t.Errorf("getEnv() with set var = %v, want custom", result)
	}
}

func TestGetEnvInt(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		value      string
		defaultVal int
		expected   int
	}{
		{
			name:       "valid integer",
			key:        "TEST_INT_1",
			value:      "42",
			defaultVal: 0,
			expected:   42,
		},
		{
			name:       "invalid integer uses default",
			key:        "TEST_INT_2",
			value:      "not_a_number",
			defaultVal: 99,
			expected:   99,
		},
		{
			name:       "unset uses default",
			key:        "TEST_INT_UNSET",
			value:      "",
			defaultVal: 55,
			expected:   55,
		},
		{
			name:       "negative integer",
			key:        "TEST_INT_3",
			value:      "-10",
			defaultVal: 0,
			expected:   -10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != "" {
				os.Setenv(tt.key, tt.value)
			} else {
				os.Unsetenv(tt.key)
			}
			defer os.Unsetenv(tt.key)

			result := getEnvInt(tt.key, tt.defaultVal)
			if result != tt.expected {
				t.Errorf("getEnvInt() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func contains(str, substr string) bool {
	for i := 0; i+len(substr) <= len(str); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
