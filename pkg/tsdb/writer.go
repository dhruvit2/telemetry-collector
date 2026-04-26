package tsdb

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"go.uber.org/zap"
)

// TSDBConfig holds TSDB configuration
type TSDBConfig struct {
	URL        string
	Token      string
	Org        string
	Bucket     string
	MaxRetries int
}

// TSDBWriter writes telemetry data to InfluxDB TSDB
type TSDBWriter struct {
	config   *TSDBConfig
	logger   *zap.Logger
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking

	// Metrics
	totalWritten int64
	totalFailed  int64

	done chan struct{}
	wg   sync.WaitGroup
}

// NewTSDBWriter creates new TSDB writer
func NewTSDBWriter(config *TSDBConfig, logger *zap.Logger) (*TSDBWriter, error) {
	if config == nil {
		return nil, fmt.Errorf("tsdb config cannot be nil")
	}
	if config.URL == "" {
		return nil, fmt.Errorf("tsdb url cannot be empty")
	}
	if config.Token == "" {
		return nil, fmt.Errorf("tsdb token cannot be empty")
	}
	if config.Org == "" {
		return nil, fmt.Errorf("tsdb org cannot be empty")
	}
	if config.Bucket == "" {
		return nil, fmt.Errorf("tsdb bucket cannot be empty")
	}

	// Create client
	client := influxdb2.NewClient(config.URL, config.Token)

	// Verify connection
	ready, err := client.Ready(context.Background())
	if err != nil || ready == nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to TSDB: %w", err)
	}

	writer := &TSDBWriter{
		config:   config,
		logger:   logger,
		client:   client,
		writeAPI: client.WriteAPIBlocking(config.Org, config.Bucket),
		done:     make(chan struct{}),
	}

	logger.Info("TSDB writer initialized",
		zap.String("url", config.URL),
		zap.String("org", config.Org),
		zap.String("bucket", config.Bucket))

	return writer, nil
}

// WriteMetric writes a metric to TSDB with retry logic
func (w *TSDBWriter) WriteMetric(ctx context.Context, record map[string]interface{}) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	// Convert to line protocol
	line, err := w.convertToLineProtocol(record)
	if err != nil {
		atomic.AddInt64(&w.totalFailed, 1)
		return err
	}

	// Write with retries
	return w.writeWithRetries(ctx, line)
}

// writeWithRetries implements exponential backoff retry logic
func (w *TSDBWriter) writeWithRetries(ctx context.Context, line string) error {
	var lastErr error

	for attempt := 0; attempt < w.config.MaxRetries; attempt++ {
		// Check context before attempting
		if ctx.Err() != nil {
			atomic.AddInt64(&w.totalFailed, 1)
			return ctx.Err()
		}

		// Attempt write
		err := w.writeAPI.WriteRecord(ctx, line)
		if err == nil {
			atomic.AddInt64(&w.totalWritten, 1)
			return nil
		}

		lastErr = err

		// Exponential backoff
		if attempt < w.config.MaxRetries-1 {
			backoffDuration := time.Duration(100*int(1<<uint(attempt))) * time.Millisecond
			select {
			case <-ctx.Done():
				atomic.AddInt64(&w.totalFailed, 1)
				return ctx.Err()
			case <-time.After(backoffDuration):
			}
		}
	}

	atomic.AddInt64(&w.totalFailed, 1)
	w.logger.Warn("failed to write metric after retries",
		zap.Error(lastErr),
		zap.Int("attempts", w.config.MaxRetries))

	return lastErr
}

// convertToLineProtocol converts record map to InfluxDB line protocol format
// Handles both snake_case and camelCase field names from different CSV sources
func (w *TSDBWriter) convertToLineProtocol(record map[string]interface{}) (string, error) {
	// Extract required fields with flexible field name support
	// Try both snake_case and camelCase variants
	metricName := w.getStringFieldVariants(record, []string{"metric_name", "metricName"}, "unknown_metric")
	gpuID := w.getStringFieldVariants(record, []string{"gpu_id", "gpuId"}, "unknown")
	deviceID := w.getStringFieldVariants(record, []string{"device_id", "device", "deviceId"}, "unknown")
	uuid := w.getStringFieldVariants(record, []string{"uuid", "UUID"}, "unknown")
	modelName := w.getStringFieldVariants(record, []string{"model_name", "modelName"}, "unknown")
	hostName := w.getStringFieldVariants(record, []string{"host_name", "hostname", "Hostname", "hostName"}, "unknown")
	container := w.getStringFieldVariants(record, []string{"container", "container_name"}, "unknown")
	value := w.getFloatField(record, "value", 0.0)

	// Validate that no required fields are empty (prevent invalid InfluxDB line protocol)
	if metricName == "" || gpuID == "" || deviceID == "" || uuid == "" || modelName == "" || hostName == "" || container == "" {
		w.logger.Warn("detected empty required field after default replacement",
			zap.String("metric_name", metricName),
			zap.String("gpu_id", gpuID),
			zap.String("device_id", deviceID),
			zap.String("uuid", uuid),
			zap.String("model_name", modelName),
			zap.String("host_name", hostName),
			zap.String("container", container),
			zap.Any("record", record))
		// Replace any remaining empty fields with "unknown"
		if metricName == "" {
			metricName = "unknown_metric"
		}
		if gpuID == "" {
			gpuID = "unknown"
		}
		if deviceID == "" {
			deviceID = "unknown"
		}
		if uuid == "" {
			uuid = "unknown"
		}
		if modelName == "" {
			modelName = "unknown"
		}
		if hostName == "" {
			hostName = "unknown"
		}
		if container == "" {
			container = "unknown"
		}
	}

	// Extract timestamp
	timestamp := w.extractTimestamp(record)

	// Escape tag values to handle spaces and special characters in InfluxDB line protocol
	// InfluxDB requires escaping spaces, commas, and equals signs in tag values
	metricNameEscaped := w.escapeTagValue(metricName)
	gpuIDEscaped := w.escapeTagValue(gpuID)
	deviceIDEscaped := w.escapeTagValue(deviceID)
	uuidEscaped := w.escapeTagValue(uuid)
	modelNameEscaped := w.escapeTagValue(modelName)
	hostNameEscaped := w.escapeTagValue(hostName)
	containerEscaped := w.escapeTagValue(container)

	// Build line protocol: measurement,tag1=v1,tag2=v2 field=value timestamp
	// After escaping, tag values should be safe from special characters
	tags := fmt.Sprintf("metric_name=%s,gpu_id=%s,device_id=%s,uuid=%s,model_name=%s,host_name=%s,container=%s",
		metricNameEscaped, gpuIDEscaped, deviceIDEscaped, uuidEscaped, modelNameEscaped, hostNameEscaped, containerEscaped)

	line := fmt.Sprintf("gpu_metrics,%s value=%f %d", tags, value, timestamp)

	return line, nil
}

// extractTimestamp extracts and converts timestamp from record
func (w *TSDBWriter) extractTimestamp(record map[string]interface{}) int64 {
	// Try different timestamp field names
	for _, field := range []string{"timestamp", "_timestamp", "ts", "time"} {
		if val, exists := record[field]; exists {
			switch v := val.(type) {
			case int64:
				// Already in nanoseconds
				if v > 1e15 {
					return v
				}
				// Convert from milliseconds or seconds
				if v > 1e12 {
					return v * 1e6
				}
				return v * 1e9
			case float64:
				return int64(v)
			case string:
				// Try parsing RFC3339Nano
				if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
					return t.UnixNano()
				}
				// Try parsing RFC3339
				if t, err := time.Parse(time.RFC3339, v); err == nil {
					return t.UnixNano()
				}
				// Try parsing Unix timestamp
				if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
					if ts > 1e12 {
						return ts * 1e6
					}
					return ts * 1e9
				}
			}
		}
	}

	// Default to current time if no timestamp found
	return time.Now().UnixNano()
}

// getStringField safely extracts string field from record
// Returns defaultVal if field is missing or is an empty string (to prevent invalid InfluxDB tags)
func (w *TSDBWriter) getStringField(record map[string]interface{}, key string, defaultVal string) string {
	if val, exists := record[key]; exists {
		if str, ok := val.(string); ok {
			// Return default if string is empty to prevent invalid InfluxDB tag values
			if str == "" {
				return defaultVal
			}
			return str
		}
		strVal := fmt.Sprintf("%v", val)
		// Also check formatted string for empty values
		if strVal == "" {
			return defaultVal
		}
		return strVal
	}
	return defaultVal
}

// escapeTagValue escapes special characters in InfluxDB tag values
// Per InfluxDB line protocol spec, spaces, commas, and equals signs must be escaped with backslash
func (w *TSDBWriter) escapeTagValue(value string) string {
	// Escape spaces, commas, and equals signs which are special in tag values
	value = strings.ReplaceAll(value, " ", "\\ ")
	value = strings.ReplaceAll(value, ",", "\\,")
	value = strings.ReplaceAll(value, "=", "\\=")
	return value
}

// getStringFieldVariants tries multiple field name variants (case-insensitive, camelCase vs snake_case)
// Returns the first non-empty value found, or defaultVal if none exist or all are empty
func (w *TSDBWriter) getStringFieldVariants(record map[string]interface{}, fieldNames []string, defaultVal string) string {
	for _, fieldName := range fieldNames {
		if val, exists := record[fieldName]; exists {
			if str, ok := val.(string); ok {
				if str != "" {
					return str
				}
			} else {
				strVal := fmt.Sprintf("%v", val)
				if strVal != "" && strVal != "<nil>" {
					return strVal
				}
			}
		}
	}
	return defaultVal
}

// getFloatField safely extracts float field from record
func (w *TSDBWriter) getFloatField(record map[string]interface{}, key string, defaultVal float64) float64 {
	if val, exists := record[key]; exists {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	}
	return defaultVal
}

// Health checks TSDB health
func (w *TSDBWriter) Health(ctx context.Context) (bool, error) {
	ready, err := w.client.Ready(ctx)
	if err != nil || ready == nil {
		return false, err
	}
	return true, nil
}

// GetMetrics returns metrics map
func (w *TSDBWriter) GetMetrics() map[string]int64 {
	return map[string]int64{
		"total_written": atomic.LoadInt64(&w.totalWritten),
		"total_failed":  atomic.LoadInt64(&w.totalFailed),
	}
}

// Close closes the TSDB writer
func (w *TSDBWriter) Close() error {
	close(w.done)
	w.wg.Wait()
	w.client.Close()
	w.logger.Info("TSDB writer closed",
		zap.Int64("total_written", atomic.LoadInt64(&w.totalWritten)),
		zap.Int64("total_failed", atomic.LoadInt64(&w.totalFailed)))
	return nil
}
