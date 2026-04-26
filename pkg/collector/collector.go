package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"telemetry-collector/pkg/metrics"
	"telemetry-collector/pkg/tsdb"

	"go.uber.org/zap"
)

// SourceCollectorFunc is a function that collects telemetry from a source
type SourceCollectorFunc func(ctx context.Context) (map[string]interface{}, error)

// TelemetryCollector collects telemetry data and sends to message broker
type TelemetryCollector struct {
	brokerAddresses []string
	topic           string
	readFrequency   time.Duration
	batchSize       int
	batchTimeout    time.Duration
	maxRetries      int
	logger          *zap.Logger
	metrics         *metrics.CollectorMetrics
	tsdbWriter      *tsdb.TSDBWriter // TSDB writer for metrics storage

	// Collection sources
	sources   map[string]SourceCollectorFunc
	sourcesMu sync.RWMutex

	// State
	batch          [][]byte
	batchMu        sync.Mutex
	batchTimer     *time.Timer
	done           chan struct{}
	wg             sync.WaitGroup
	circuitBreaker *CircuitBreaker

	// Mock for testing
	sentCount int64
}

// CircuitBreaker implements circuit breaker pattern
type CircuitBreaker struct {
	failureThreshold int32
	failures         int32
	successCount     int32
	lastFailureTime  time.Time
	state            string
	mu               sync.RWMutex
	resetTimeout     time.Duration
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(failureThreshold int32, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		state:            "closed",
		resetTimeout:     resetTimeout,
	}
}

// IsOpen returns true if circuit is open
func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.state == "open" {
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.mu.RUnlock()
			cb.mu.Lock()
			cb.state = "half-open"
			atomic.StoreInt32(&cb.successCount, 0)
			cb.mu.Unlock()
			cb.mu.RLock()
			return false
		}
		return true
	}
	return false
}

// RecordFailure records a failure
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastFailureTime = time.Now()
	newFailures := atomic.AddInt32(&cb.failures, 1)

	if newFailures >= cb.failureThreshold && cb.state == "closed" {
		cb.state = "open"
	}
}

// RecordSuccess records a success
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == "half-open" {
		atomic.StoreInt32(&cb.successCount, atomic.LoadInt32(&cb.successCount)+1)
		if atomic.LoadInt32(&cb.successCount) >= 5 {
			cb.state = "closed"
			atomic.StoreInt32(&cb.failures, 0)
		}
	}
}

// NewTelemetryCollector creates a new telemetry collector
func NewTelemetryCollector(
	brokerAddresses []string,
	topic string,
	readFrequencyMs int,
	batchSize int,
	batchTimeoutMs int,
	maxRetries int,
	logger *zap.Logger,
) (*TelemetryCollector, error) {
	if len(brokerAddresses) == 0 {
		return nil, fmt.Errorf("broker addresses cannot be empty")
	}
	if topic == "" {
		return nil, fmt.Errorf("topic cannot be empty")
	}
	if readFrequencyMs <= 0 {
		return nil, fmt.Errorf("read frequency must be > 0")
	}

	return &TelemetryCollector{
		brokerAddresses: brokerAddresses,
		topic:           topic,
		readFrequency:   time.Duration(readFrequencyMs) * time.Millisecond,
		batchSize:       batchSize,
		batchTimeout:    time.Duration(batchTimeoutMs) * time.Millisecond,
		maxRetries:      maxRetries,
		logger:          logger,
		metrics:         &metrics.CollectorMetrics{},
		sources:         make(map[string]SourceCollectorFunc),
		batch:           make([][]byte, 0, batchSize),
		done:            make(chan struct{}),
		circuitBreaker:  NewCircuitBreaker(10, 30*time.Second),
	}, nil
}

// SetTSDBWriter sets the TSDB writer for metrics storage
func (tc *TelemetryCollector) SetTSDBWriter(writer *tsdb.TSDBWriter) {
	tc.tsdbWriter = writer
	if writer != nil {
		tc.logger.Info("TSDB writer attached to collector")
	}
}

// RegisterSource registers a telemetry source
func (tc *TelemetryCollector) RegisterSource(name string, collector SourceCollectorFunc) error {
	if name == "" {
		return fmt.Errorf("source name cannot be empty")
	}
	if collector == nil {
		return fmt.Errorf("collector function cannot be nil")
	}

	tc.sourcesMu.Lock()
	defer tc.sourcesMu.Unlock()

	tc.sources[name] = collector
	tc.logger.Info("source registered", zap.String("source", name))
	return nil
}

// Start starts the collector
func (tc *TelemetryCollector) Start(ctx context.Context) {
	tc.wg.Add(1)
	go tc.collectLoop(ctx)
}

// collectLoop main collection loop
func (tc *TelemetryCollector) collectLoop(ctx context.Context) {
	defer tc.wg.Done()

	ticker := time.NewTicker(tc.readFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			tc.logger.Info("collection loop stopped")
			return
		case <-tc.done:
			return
		case <-ticker.C:
			tc.collectAndSend(ctx)
		}
	}
}

// collectAndSend collects from all sources and sends to broker
func (tc *TelemetryCollector) collectAndSend(ctx context.Context) {
	if tc.circuitBreaker.IsOpen() {
		tc.logger.Warn("circuit breaker open, skipping collection")
		tc.metrics.RecordCircuitBreakerTrip()
		return
	}

	tc.sourcesMu.RLock()
	sources := make(map[string]SourceCollectorFunc)
	for name, fn := range tc.sources {
		sources[name] = fn
	}
	tc.sourcesMu.RUnlock()

	for name, fn := range sources {
		data, err := fn(ctx)
		if err != nil {
			tc.logger.Error("collection failed",
				zap.String("source", name),
				zap.Error(err))
			tc.metrics.RecordCollectionError()
			tc.circuitBreaker.RecordFailure()
			continue
		}

		// Marshal and queue
		jsonData, err := json.Marshal(data)
		if err != nil {
			tc.logger.Error("failed to marshal data",
				zap.String("source", name),
				zap.Error(err))
			tc.metrics.RecordCollectionError()
			continue
		}

		tc.batchMu.Lock()
		tc.batch = append(tc.batch, jsonData)
		tc.metrics.RecordMessagesCollected(1)

		// Write to TSDB if enabled
		if tc.tsdbWriter != nil {
			// Convert JSON back to map for TSDB
			var dataMap map[string]interface{}
			if err := json.Unmarshal(jsonData, &dataMap); err == nil {
				if err := tc.tsdbWriter.WriteMetric(ctx, dataMap); err != nil {
					tc.logger.Warn("failed to write metric to TSDB",
						zap.String("source", name),
						zap.Error(err))
				}
			}
		}

		shouldFlush := len(tc.batch) >= tc.batchSize
		tc.batchMu.Unlock()

		if shouldFlush {
			_ = tc.flushBatch()
		}
	}

	tc.circuitBreaker.RecordSuccess()
}

// flushBatch sends current batch to broker
func (tc *TelemetryCollector) flushBatch() error {
	tc.batchMu.Lock()
	if len(tc.batch) == 0 {
		tc.batchMu.Unlock()
		return nil
	}

	batchToSend := tc.batch
	batchSize := len(batchToSend)
	tc.batch = make([][]byte, 0, tc.batchSize)
	tc.batchMu.Unlock()

	// Send with retries
	var lastErr error
	backoff := 100 * time.Millisecond

	for attempt := 0; attempt <= tc.maxRetries; attempt++ {
		err := tc.sendBatch(batchToSend)
		if err == nil {
			tc.logger.Debug("batch sent successfully",
				zap.Int("size", batchSize))
			atomic.AddInt64(&tc.sentCount, int64(batchSize))
			tc.metrics.RecordBatchSent(int64(batchSize * 100)) // Approximate byte size
			tc.circuitBreaker.RecordSuccess()
			return nil
		}

		lastErr = err
		tc.metrics.RecordRetryAttempt()

		if attempt < tc.maxRetries {
			tc.logger.Warn("batch send failed, retrying",
				zap.Int("attempt", attempt+1),
				zap.Duration("backoff", backoff))
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	tc.logger.Error("batch send failed after retries",
		zap.Error(lastErr))
	tc.circuitBreaker.RecordFailure()
	tc.metrics.RecordMessageFailed()
	return lastErr
}

// sendBatch sends batch to broker (mock implementation)
func (tc *TelemetryCollector) sendBatch(batch [][]byte) error {
	if tc.circuitBreaker.IsOpen() {
		return fmt.Errorf("circuit breaker open")
	}

	// In production, this would send via messagebroker package
	// For now, mock implementation
	tc.logger.Debug("sending batch to broker",
		zap.Int("count", len(batch)),
		zap.String("topic", tc.topic))

	return nil
}

// Close closes the collector
func (tc *TelemetryCollector) Close() error {
	close(tc.done)

	// Final flush
	tc.batchMu.Lock()
	if len(tc.batch) > 0 {
		_ = tc.flushBatch()
	}
	tc.batchMu.Unlock()

	tc.wg.Wait()

	metrics := tc.metrics.GetSnapshot()
	tc.logger.Info("collector closed",
		zap.Int64("messages_collected", metrics.MessagesCollected),
		zap.Int64("messages_sent", metrics.MessagesSent),
		zap.Int64("batches_sent", metrics.BatchesSent))

	return nil
}

// GetMetrics returns current metrics
func (tc *TelemetryCollector) GetMetrics() metrics.Snapshot {
	return tc.metrics.GetSnapshot()
}
