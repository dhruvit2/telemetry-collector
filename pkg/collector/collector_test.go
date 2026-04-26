package collector
package collector

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"telemetry-collector/pkg/metrics"
)

var testLogger, _ = zap.NewDevelopment()

// TestCircuitBreakerInitial tests initial state
func TestCircuitBreakerInitial(t *testing.T) {
	cb := NewCircuitBreaker(5, time.Second)

	if cb.IsOpen() {
		t.Error("circuit breaker should be closed initially")
	}
}

// TestCircuitBreakerTransitions tests state transitions
func TestCircuitBreakerTransitions(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Second)

	// Record failures until threshold
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()

	// Should trip open
	if !cb.IsOpen() {
		t.Error("circuit breaker should be open after 3 failures")
	}

	// Wait for reset timeout
	time.Sleep(1100 * time.Millisecond)

	// Should be half-open now
	if cb.IsOpen() {
		t.Error("circuit breaker should be half-open after reset timeout")
	}

	// Record success should close it
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.IsOpen() {
		t.Error("circuit breaker should be closed after 5 successes in half-open state")
	}
}

// TestCircuitBreakerHalfOpenToOpen tests transitioning back to open from half-open
func TestCircuitBreakerHalfOpenToOpen(t *testing.T) {
	cb := NewCircuitBreaker(2, 500*time.Millisecond)

	// Trip open
	cb.RecordFailure()
	cb.RecordFailure()

	if !cb.IsOpen() {
		t.Error("circuit breaker should be open")
	}

	// Wait for reset
	time.Sleep(600 * time.Millisecond)

	// Should be half-open, but not open
	if cb.IsOpen() {
		t.Error("circuit breaker should be half-open")
	}

	// Record failure in half-open should go back to open
	cb.RecordFailure()

	if !cb.IsOpen() {
		t.Error("circuit breaker should be open after failure in half-open state")
	}
}

// TestNewTelemetryCollectorValidation tests constructor validation
func TestNewTelemetryCollectorValidation(t *testing.T) {
	tests := []struct {
		name             string
		brokerAddresses  []string
		topic            string
		readFrequencyMs  int
		shouldErr        bool
	}{
		{
			name:            "valid configuration",
			brokerAddresses: []string{"localhost:9092"},
			topic:           "test-topic",
			readFrequencyMs: 1000,
			shouldErr:       false,
		},
		{
			name:            "empty broker addresses",
			brokerAddresses: []string{},
			topic:           "test-topic",
			readFrequencyMs: 1000,
			shouldErr:       true,
		},
		{
			name:            "empty topic",
			brokerAddresses: []string{"localhost:9092"},
			topic:           "",
			readFrequencyMs: 1000,
			shouldErr:       true,
		},
		{
			name:            "invalid read frequency",
			brokerAddresses: []string{"localhost:9092"},
			topic:           "test-topic",
			readFrequencyMs: 0,
			shouldErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTelemetryCollector(
				tt.brokerAddresses,
				tt.topic,
				tt.readFrequencyMs,
				50,
				1000,
				5,
				testLogger,
			)

			if (err != nil) != tt.shouldErr {
				t.Errorf("NewTelemetryCollector error = %v, shouldErr = %v", err, tt.shouldErr)
			}
		})
	}
}

// TestRegisterSource tests source registration
func TestRegisterSource(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		1000,
		10,
		1000,
		5,
		testLogger,
	)

	// Register valid source
	err := tc.RegisterSource("source1", func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"test": "data"}, nil
	})
	if err != nil {
		t.Errorf("RegisterSource failed: %v", err)
	}

	// Try to register with empty name
	err = tc.RegisterSource("", func(ctx context.Context) (map[string]interface{}, error) {
		return nil, nil
	})
	if err == nil {
		t.Error("RegisterSource should fail with empty name")
	}

	// Try to register with nil function
	err = tc.RegisterSource("source2", nil)
	if err == nil {
		t.Error("RegisterSource should fail with nil function")
	}
}

// TestCollectorMetrics tests metrics collection
func TestCollectorMetrics(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		1000,
		10,
		1000,
		5,
		testLogger,
	)

	// Register a source
	tc.RegisterSource("test", func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"value": 42}, nil
	})

	// Start collector
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc.Start(ctx)

	// Let it collect once
	time.Sleep(1100 * time.Millisecond)

	// Stop and get metrics
	_ = tc.Close()

	metrics := tc.GetMetrics()
	if metrics.MessagesCollected == 0 {
		t.Error("should have collected messages")
	}
}

// TestCollectorBatching tests batch accumulation and flushing
func TestCollectorBatching(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		100, // Fast collection
		5,   // Small batch size
		500,
		5,
		testLogger,
	)

	callCount := 0
	tc.RegisterSource("test1", func(ctx context.Context) (map[string]interface{}, error) {
		callCount++
		return map[string]interface{}{"id": callCount}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc.Start(ctx)

	// Wait for a few collection cycles
	time.Sleep(600 * time.Millisecond)

	_ = tc.Close()

	metrics := tc.GetMetrics()
	if metrics.MessagesCollected == 0 {
		t.Error("should have collected messages")
	}
}

// TestCollectorErrorHandling tests error handling in collection
func TestCollectorErrorHandling(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		100,
		10,
		500,
		5,
		testLogger,
	)

	// Register a source that fails
	tc.RegisterSource("failing", func(ctx context.Context) (map[string]interface{}, error) {
		return nil, fmt.Errorf("collection error")
	})

	// Register a source that succeeds
	tc.RegisterSource("working", func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"status": "ok"}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc.Start(ctx)

	// Let it collect
	time.Sleep(600 * time.Millisecond)

	_ = tc.Close()

	metrics := tc.GetMetrics()
	if metrics.CollectionErrors == 0 {
		t.Error("should have recorded collection errors")
	}
	if metrics.MessagesCollected == 0 {
		t.Error("should have collected messages from working source")
	}
}

// TestCollectorConcurrency tests concurrent operations
func TestCollectorConcurrency(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		100,
		10,
		500,
		5,
		testLogger,
	)

	// Register multiple sources
	for i := 0; i < 5; i++ {
		sourceID := i
		tc.RegisterSource(fmt.Sprintf("source%d", sourceID), func(ctx context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"source": sourceID, "timestamp": time.Now()}, nil
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc.Start(ctx)

	// Concurrent reads of metrics
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = tc.GetMetrics()
			}
		}()
	}

	time.Sleep(500 * time.Millisecond)

	_ = tc.Close()
	wg.Wait()

	// If we reach here without panics, concurrency test passes
	metrics := tc.GetMetrics()
	if metrics.MessagesCollected == 0 {
		t.Error("should have collected messages")
	}
}

// TestCollectorClose tests graceful shutdown
func TestCollectorClose(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		100,
		10,
		500,
		5,
		testLogger,
	)

	tc.RegisterSource("test", func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"data": "test"}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	tc.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	// Close should not panic
	err := tc.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	cancel()

	// Verify we can close again without issue
	err = tc.Close()
	if err != nil {
		t.Errorf("Close again returned error: %v", err)
	}
}

// TestCircuitBreakerThreadSafety tests thread safety of circuit breaker
func TestCircuitBreakerThreadSafety(t *testing.T) {
	cb := NewCircuitBreaker(100, time.Second)

	var wg sync.WaitGroup
	done := make(chan bool)

	// Concurrent failure recording
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				cb.RecordFailure()
			}
		}()
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = cb.IsOpen()
			}
		}()
	}

	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success - no deadlock or panic
	case <-time.After(5 * time.Second):
		t.Error("TestCircuitBreakerThreadSafety timeout - possible deadlock")
	}
}

// TestCollectorMultipleClose tests closing multiple times
func TestCollectorMultipleClose(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		1000,
		10,
		1000,
		5,
		testLogger,
	)

	ctx, cancel := context.WithCancel(context.Background())

	tc.Start(ctx)

	// Multiple closes should not panic
	_ = tc.Close()
	_ = tc.Close()
	_ = tc.Close()

	cancel()
}

// TestCollectorSourceRetrieval tests retrieving registered sources
func TestCollectorSourceRetrieval(t *testing.T) {
	tc, _ := NewTelemetryCollector(
		[]string{"localhost:9092"},
		"test",
		1000,
		10,
		1000,
		5,
		testLogger,
	)

	sourceName := "test-source"
	expectedData := map[string]interface{}{"key": "value"}

	tc.RegisterSource(sourceName, func(ctx context.Context) (map[string]interface{}, error) {
		return expectedData, nil
	})

	// We can't directly access sources, but we can verify through collection
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	_ = tc.Close()

	metrics := tc.GetMetrics()
	if metrics.MessagesCollected != 1 {
		t.Errorf("should have collected 1 message from source, got %d", metrics.MessagesCollected)
	}
}

// BenchmarkCircuitBreakerFailureRecording benchmarks circuit breaker failure recording
func BenchmarkCircuitBreakerFailureRecording(b *testing.B) {
	cb := NewCircuitBreaker(int32(b.N+1), time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.RecordFailure()
	}
}

// BenchmarkCircuitBreakerIsOpen benchmarks circuit breaker IsOpen check
func BenchmarkCircuitBreakerIsOpen(b *testing.B) {
	cb := NewCircuitBreaker(5, time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.IsOpen()
	}
}

// BenchmarkMetricsRecording benchmarks metrics recording
func BenchmarkMetricsRecording(b *testing.B) {
	m := &metrics.CollectorMetrics{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.RecordMessagesCollected(1)
		m.RecordMessageSent()
		m.RecordBatchSent(100)
	}
}
