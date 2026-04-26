package metrics

import (
	"sync/atomic"
)

// CollectorMetrics holds collector metrics
type CollectorMetrics struct {
	messagesCollected   int64
	messagesSent        int64
	messagesFailed      int64
	batchesSent         int64
	bytesCollected      int64
	collectionErrors    int64
	retryAttempts       int64
	circuitBreakerTrips int64
}

// RecordMessagesCollected records collected messages
func (m *CollectorMetrics) RecordMessagesCollected(count int64) {
	atomic.AddInt64(&m.messagesCollected, count)
}

// RecordMessageSent records sent message
func (m *CollectorMetrics) RecordMessageSent() {
	atomic.AddInt64(&m.messagesSent, 1)
}

// RecordMessageFailed records failed message
func (m *CollectorMetrics) RecordMessageFailed() {
	atomic.AddInt64(&m.messagesFailed, 1)
}

// RecordBatchSent records batch send
func (m *CollectorMetrics) RecordBatchSent(byteCount int64) {
	atomic.AddInt64(&m.batchesSent, 1)
	atomic.AddInt64(&m.bytesCollected, byteCount)
}

// RecordCollectionError records collection error
func (m *CollectorMetrics) RecordCollectionError() {
	atomic.AddInt64(&m.collectionErrors, 1)
}

// RecordRetryAttempt records retry attempt
func (m *CollectorMetrics) RecordRetryAttempt() {
	atomic.AddInt64(&m.retryAttempts, 1)
}

// RecordCircuitBreakerTrip records circuit breaker trip
func (m *CollectorMetrics) RecordCircuitBreakerTrip() {
	atomic.AddInt64(&m.circuitBreakerTrips, 1)
}

// Snapshot represents current metrics state
type Snapshot struct {
	MessagesCollected   int64
	MessagesSent        int64
	MessagesFailed      int64
	BatchesSent         int64
	BytesCollected      int64
	CollectionErrors    int64
	RetryAttempts       int64
	CircuitBreakerTrips int64
}

// GetSnapshot returns current metrics snapshot
func (m *CollectorMetrics) GetSnapshot() Snapshot {
	return Snapshot{
		MessagesCollected:   atomic.LoadInt64(&m.messagesCollected),
		MessagesSent:        atomic.LoadInt64(&m.messagesSent),
		MessagesFailed:      atomic.LoadInt64(&m.messagesFailed),
		BatchesSent:         atomic.LoadInt64(&m.batchesSent),
		BytesCollected:      atomic.LoadInt64(&m.bytesCollected),
		CollectionErrors:    atomic.LoadInt64(&m.collectionErrors),
		RetryAttempts:       atomic.LoadInt64(&m.retryAttempts),
		CircuitBreakerTrips: atomic.LoadInt64(&m.circuitBreakerTrips),
	}
}

// Reset resets all metrics
func (m *CollectorMetrics) Reset() {
	atomic.StoreInt64(&m.messagesCollected, 0)
	atomic.StoreInt64(&m.messagesSent, 0)
	atomic.StoreInt64(&m.messagesFailed, 0)
	atomic.StoreInt64(&m.batchesSent, 0)
	atomic.StoreInt64(&m.bytesCollected, 0)
	atomic.StoreInt64(&m.collectionErrors, 0)
	atomic.StoreInt64(&m.retryAttempts, 0)
	atomic.StoreInt64(&m.circuitBreakerTrips, 0)
}
