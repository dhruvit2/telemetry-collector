package metrics
package metrics

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRecordMessagesCollected(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordMessagesCollected(1)
	snapshot := m.GetSnapshot()
	if snapshot.MessagesCollected != 1 {
		t.Errorf("RecordMessagesCollected(1) = %d, want 1", snapshot.MessagesCollected)
	}

	m.RecordMessagesCollected(5)
	snapshot = m.GetSnapshot()
	if snapshot.MessagesCollected != 6 {
		t.Errorf("RecordMessagesCollected(5) total = %d, want 6", snapshot.MessagesCollected)
	}
}

func TestRecordMessageSent(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordMessageSent()
	snapshot := m.GetSnapshot()
	if snapshot.MessagesSent != 1 {
		t.Errorf("RecordMessageSent() = %d, want 1", snapshot.MessagesSent)
	}

	m.RecordMessageSent()
	m.RecordMessageSent()
	snapshot = m.GetSnapshot()
	if snapshot.MessagesSent != 3 {
		t.Errorf("RecordMessageSent() total = %d, want 3", snapshot.MessagesSent)
	}
}

func TestRecordMessageFailed(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordMessageFailed()
	snapshot := m.GetSnapshot()
	if snapshot.MessagesFailed != 1 {
		t.Errorf("RecordMessageFailed() = %d, want 1", snapshot.MessagesFailed)
	}

	m.RecordMessageFailed()
	snapshot = m.GetSnapshot()
	if snapshot.MessagesFailed != 2 {
		t.Errorf("RecordMessageFailed() total = %d, want 2", snapshot.MessagesFailed)
	}
}

func TestRecordBatchSent(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordBatchSent(100)
	snapshot := m.GetSnapshot()
	if snapshot.BatchesSent != 1 {
		t.Errorf("RecordBatchSent() batches = %d, want 1", snapshot.BatchesSent)
	}
	if snapshot.BytesCollected != 100 {
		t.Errorf("RecordBatchSent() bytes = %d, want 100", snapshot.BytesCollected)
	}

	m.RecordBatchSent(50)
	snapshot = m.GetSnapshot()
	if snapshot.BatchesSent != 2 {
		t.Errorf("RecordBatchSent() batches = %d, want 2", snapshot.BatchesSent)
	}
	if snapshot.BytesCollected != 150 {
		t.Errorf("RecordBatchSent() bytes = %d, want 150", snapshot.BytesCollected)
	}
}

func TestRecordCollectionError(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordCollectionError()
	snapshot := m.GetSnapshot()
	if snapshot.CollectionErrors != 1 {
		t.Errorf("RecordCollectionError() = %d, want 1", snapshot.CollectionErrors)
	}

	for i := 0; i < 5; i++ {
		m.RecordCollectionError()
	}
	snapshot = m.GetSnapshot()
	if snapshot.CollectionErrors != 6 {
		t.Errorf("RecordCollectionError() total = %d, want 6", snapshot.CollectionErrors)
	}
}

func TestRecordRetryAttempt(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordRetryAttempt()
	snapshot := m.GetSnapshot()
	if snapshot.RetryAttempts != 1 {
		t.Errorf("RecordRetryAttempt() = %d, want 1", snapshot.RetryAttempts)
	}

	for i := 0; i < 9; i++ {
		m.RecordRetryAttempt()
	}
	snapshot = m.GetSnapshot()
	if snapshot.RetryAttempts != 10 {
		t.Errorf("RecordRetryAttempt() total = %d, want 10", snapshot.RetryAttempts)
	}
}

func TestRecordCircuitBreakerTrip(t *testing.T) {
	m := &CollectorMetrics{}

	m.RecordCircuitBreakerTrip()
	snapshot := m.GetSnapshot()
	if snapshot.CircuitBreakerTrips != 1 {
		t.Errorf("RecordCircuitBreakerTrip() = %d, want 1", snapshot.CircuitBreakerTrips)
	}

	m.RecordCircuitBreakerTrip()
	m.RecordCircuitBreakerTrip()
	snapshot = m.GetSnapshot()
	if snapshot.CircuitBreakerTrips != 3 {
		t.Errorf("RecordCircuitBreakerTrip() total = %d, want 3", snapshot.CircuitBreakerTrips)
	}
}

func TestGetSnapshot(t *testing.T) {
	m := &CollectorMetrics{}

	// Record various metrics
	m.RecordMessagesCollected(10)
	m.RecordMessageSent()
	m.RecordBatchSent(512)
	m.RecordCollectionError()
	m.RecordRetryAttempt()

	snapshot := m.GetSnapshot()

	if snapshot.MessagesCollected != 10 {
		t.Errorf("snapshot.MessagesCollected = %d, want 10", snapshot.MessagesCollected)
	}
	if snapshot.MessagesSent != 1 {
		t.Errorf("snapshot.MessagesSent = %d, want 1", snapshot.MessagesSent)
	}
	if snapshot.BatchesSent != 1 {
		t.Errorf("snapshot.BatchesSent = %d, want 1", snapshot.BatchesSent)
	}
	if snapshot.BytesCollected != 512 {
		t.Errorf("snapshot.BytesCollected = %d, want 512", snapshot.BytesCollected)
	}
	if snapshot.CollectionErrors != 1 {
		t.Errorf("snapshot.CollectionErrors = %d, want 1", snapshot.CollectionErrors)
	}
	if snapshot.RetryAttempts != 1 {
		t.Errorf("snapshot.RetryAttempts = %d, want 1", snapshot.RetryAttempts)
	}
}

func TestReset(t *testing.T) {
	m := &CollectorMetrics{}

	// Record various metrics
	m.RecordMessagesCollected(100)
	m.RecordMessageSent()
	m.RecordBatchSent(1024)

	// Verify metrics are recorded
	snapshot := m.GetSnapshot()
	if snapshot.MessagesCollected != 100 || snapshot.MessagesSent != 1 {
		t.Error("metrics not recorded properly before reset")
	}

	// Reset
	m.Reset()

	// Verify all metrics are zero
	snapshot = m.GetSnapshot()
	if snapshot.MessagesCollected != 0 || snapshot.MessagesSent != 0 || snapshot.BatchesSent != 0 {
		t.Error("reset failed to clear metrics")
	}
}

func TestConcurrentMetricsAccess(t *testing.T) {
	m := &CollectorMetrics{}
	numGoroutines := 100
	operationsPerGoroutine := 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				m.RecordMessagesCollected(1)
				m.RecordMessageSent()
				m.RecordBatchSent(10)
			}
		}()
	}

	wg.Wait()

	snapshot := m.GetSnapshot()
	expectedMessages := int64(numGoroutines * operationsPerGoroutine)
	expectedBatches := int64(numGoroutines * operationsPerGoroutine)
	expectedBytes := int64(numGoroutines * operationsPerGoroutine * 10)

	if snapshot.MessagesCollected != expectedMessages {
		t.Errorf("MessagesCollected = %d, want %d", snapshot.MessagesCollected, expectedMessages)
	}
	if snapshot.MessagesSent != expectedMessages {
		t.Errorf("MessagesSent = %d, want %d", snapshot.MessagesSent, expectedMessages)
	}
	if snapshot.BatchesSent != expectedBatches {
		t.Errorf("BatchesSent = %d, want %d", snapshot.BatchesSent, expectedBatches)
	}
	if snapshot.BytesCollected != expectedBytes {
		t.Errorf("BytesCollected = %d, want %d", snapshot.BytesCollected, expectedBytes)
	}
}

func TestAtomicOperations(t *testing.T) {
	// Test that atomic operations don't have race conditions
	m := &CollectorMetrics{}

	// Read and write simultaneously
	done := make(chan bool)
	go func() {
		for i := 0; i < 1000; i++ {
			m.RecordMessagesCollected(1)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			_ = m.GetSnapshot()
		}
		done <- true
	}()

	<-done
	<-done

	// If we get here without a race condition, test passes
	snapshot := m.GetSnapshot()
	if snapshot.MessagesCollected != 1000 {
		t.Errorf("MessagesCollected = %d, want 1000", snapshot.MessagesCollected)
	}
}
