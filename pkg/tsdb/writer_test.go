package tsdb

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestConvertToLineProtocol(t *testing.T) {
	logger, _ := zap.NewProduction()
	config := &TSDBConfig{
		URL:        "http://localhost:8086",
		Token:      "test-token",
		Org:        "telemetry",
		Bucket:     "gpu_metrics_raw",
		MaxRetries: 3,
	}

	writer := &TSDBWriter{
		config: config,
		logger: logger,
	}

	record := map[string]interface{}{
		"metric_name": "utilization",
		"gpu_id":      "0",
		"device_id":   "dev001",
		"uuid":        "abc-123-def",
		"model_name":  "A100",
		"host_name":   "server1",
		"container":   "pod-1",
		"value":       85.5,
		"timestamp":   time.Now().Format(time.RFC3339Nano),
	}

	line, err := writer.convertToLineProtocol(record)
	if err != nil {
		t.Fatalf("convertToLineProtocol failed: %v", err)
	}

	// Verify format contains expected elements
	if line == "" {
		t.Error("line protocol is empty")
	}
	if !containsString(line, "gpu_metrics") {
		t.Error("line protocol missing measurement name")
	}
	if !containsString(line, "metric_name=utilization") {
		t.Error("line protocol missing metric_name tag")
	}
	if !containsString(line, "value=") {
		t.Error("line protocol missing value field")
	}
}

func TestGetStringField(t *testing.T) {
	logger, _ := zap.NewProduction()
	writer := &TSDBWriter{logger: logger}

	tests := []struct {
		name       string
		record     map[string]interface{}
		key        string
		defaultVal string
		expected   string
	}{
		{
			name:       "string value",
			record:     map[string]interface{}{"key1": "value1"},
			key:        "key1",
			defaultVal: "default",
			expected:   "value1",
		},
		{
			name:       "non-string value",
			record:     map[string]interface{}{"key1": 123},
			key:        "key1",
			defaultVal: "default",
			expected:   "123",
		},
		{
			name:       "missing key",
			record:     map[string]interface{}{},
			key:        "key1",
			defaultVal: "default",
			expected:   "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writer.getStringField(tt.record, tt.key, tt.defaultVal)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetFloatField(t *testing.T) {
	logger, _ := zap.NewProduction()
	writer := &TSDBWriter{logger: logger}

	tests := []struct {
		name       string
		record     map[string]interface{}
		key        string
		defaultVal float64
		expected   float64
	}{
		{
			name:       "float64 value",
			record:     map[string]interface{}{"value": 85.5},
			key:        "value",
			defaultVal: 0.0,
			expected:   85.5,
		},
		{
			name:       "int value",
			record:     map[string]interface{}{"value": 100},
			key:        "value",
			defaultVal: 0.0,
			expected:   100.0,
		},
		{
			name:       "string float value",
			record:     map[string]interface{}{"value": "42.5"},
			key:        "value",
			defaultVal: 0.0,
			expected:   42.5,
		},
		{
			name:       "missing key",
			record:     map[string]interface{}{},
			key:        "value",
			defaultVal: 99.0,
			expected:   99.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writer.getFloatField(tt.record, tt.key, tt.defaultVal)
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExtractTimestamp(t *testing.T) {
	logger, _ := zap.NewProduction()
	writer := &TSDBWriter{logger: logger}

	now := time.Now()
	nowNano := now.UnixNano()

	tests := []struct {
		name              string
		record            map[string]interface{}
		expectNonZero     bool
		expectWithinRange bool
	}{
		{
			name:          "RFC3339Nano timestamp",
			record:        map[string]interface{}{"timestamp": now.Format(time.RFC3339Nano)},
			expectNonZero: true,
		},
		{
			name:          "unix nanosecond timestamp",
			record:        map[string]interface{}{"timestamp": nowNano},
			expectNonZero: true,
		},
		{
			name:          "missing timestamp",
			record:        map[string]interface{}{},
			expectNonZero: true, // Should return current time
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writer.extractTimestamp(tt.record)
			if tt.expectNonZero && result == 0 {
				t.Error("expected non-zero timestamp")
			}
		})
	}
}

func TestMetrics(t *testing.T) {
	logger, _ := zap.NewProduction()
	config := &TSDBConfig{
		URL:        "http://localhost:8086",
		Token:      "test",
		Org:        "test",
		Bucket:     "test",
		MaxRetries: 1,
	}
	writer := &TSDBWriter{
		config: config,
		logger: logger,
	}

	// Simulate some metrics
	writer.totalWritten = 10
	writer.totalFailed = 2

	metrics := writer.GetMetrics()
	if metrics["total_written"] != 10 {
		t.Errorf("expected total_written=10, got %d", metrics["total_written"])
	}
	if metrics["total_failed"] != 2 {
		t.Errorf("expected total_failed=2, got %d", metrics["total_failed"])
	}
}

// Helper function
func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
