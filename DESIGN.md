# Telemetry Collector - Design Document

## 1. Overview

Telemetry Collector is a highly available, fault-tolerant Go service designed to collect telemetry data from multiple sources and publish it to a message broker. The service is built for horizontal scalability, allowing multiple instances to run concurrently to increase collection throughput.

### Key Characteristics
- **Language**: Go 1.21
- **Architecture**: Microservice with event-driven design
- **Resilience**: Circuit breaker pattern with exponential backoff retries
- **Scalability**: Horizontal scaling via multiple instances
- **Observability**: Health check endpoints and structured JSON logging

## 2. Architecture

### 2.1 Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                  Telemetry Collector                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Collection Loop (Configurable)          │   │
│  │  - Read sources at configurable frequency            │   │
│  │  - Aggregate results into batches                    │   │
│  │  - Apply backpressure logic                          │   │
│  └──────────────────────────────────────────────────────┘   │
│                         │                                     │
│                         ▼                                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Source Collectors (Extensible)             │   │
│  │  - System metrics (uptime, health)                   │   │
│  │  - Application metrics (status)                      │   │
│  │  - Custom sources (via registration)                 │   │
│  └──────────────────────────────────────────────────────┘   │
│                         │                                     │
│                         ▼                                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │          Batching & Queueing Layer                   │   │
│  │  - Batch accumulation (configurable size)            │   │
│  │  - Timeout-based flushing                            │   │
│  │  - In-memory queue                                   │   │
│  └──────────────────────────────────────────────────────┘   │
│                         │                                     │
│                         ▼                                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │       Resilience & Fault Tolerance Layer             │   │
│  │  - Circuit breaker (3-state model)                   │   │
│  │  - Exponential backoff retries                       │   │
│  │  - Error categorization                              │   │
│  └──────────────────────────────────────────────────────┘   │
│                         │                                     │
│                         ▼                                     │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Message Broker Publisher                     │   │
│  │  - Topic-based routing                               │   │
│  │  - Partition awareness                               │   │
│  │  - Delivery guarantees                               │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Health & Observability Endpoints             │   │
│  │  - /health: Service health status                    │   │
│  │  - /ready: Readiness probe                           │   │
│  │  - /metrics: Metrics snapshot                        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
          │
          ├─────► Message Broker (Kafka, etc.)
          │
          └─────► Prometheus Metrics Scraper
```

### 2.2 Data Flow

1. **Collection Phase** (Every `READ_FREQUENCY_MS`):
   - Request all registered source collectors
   - Aggregate results into a map
   - Apply filtering and validation
   
2. **Batching Phase**:
   - Accumulate messages up to `BATCH_SIZE`
   - Trigger flush on either:
     - Batch size threshold reached
     - `BATCH_TIMEOUT_MS` elapsed
   
3. **Publishing Phase**:
   - Apply circuit breaker check
   - Send batch to message broker
   - On failure: backoff retry with exponential delay
   
4. **Resilience Phase**:
   - Track failures per batch send attempt
   - Update circuit breaker state
   - Log comprehensive error details

### 2.3 Concurrency Model

- **Collection Loop**: Single goroutine per instance (prevents race conditions)
- **Metrics**: Atomic operations (sync/atomic) for lock-free thread-safety
- **Source Registration**: RWMutex protected map for concurrent readers
- **HTTP Handlers**: Standard Go http multiplexer (thread-safe by default)

## 3. Core Features

### 3.1 Configurable Collection Frequency

The service reads data at configurable intervals via `READ_FREQUENCY_MS`:

```yaml
READ_FREQUENCY_MS: 1000  # Read every 1 second
```

Benefits:
- Adjust collection speed based on load
- Multiple instances can run with same or different frequencies
- Horizontal scaling increases overall throughput

**Scaling Example**:
- 1 instance @ 1000ms = 1 collection/sec
- 3 instances @ 1000ms = 3 collections/sec (partitioned by load balancer)
- 1 instance @ 500ms = 2 collections/sec

### 3.2 Circuit Breaker Pattern

3-state finite state machine for fault tolerance:

```
CLOSED (Normal)
    │
    ├─[Failure Threshold Exceeded]──────┐
    │                                    ▼
    │                              OPEN (Circuit Tripped)
    │                                    │
    │                   [Reset Timeout Elapsed]
    │                                    │
    │                                    ▼
    │                            HALF-OPEN (Testing)
    │                                    │
    └──[5 Successes]───────────────────┤
    │                   [Any Failure]────┘
    │
    └──[Requests Allowed, Monitored]
```

**Configuration**:
- `CIRCUIT_BREAKER_THRESHOLD`: Failures to trigger open (default: 10)
- `Recovery Timeout`: 30 seconds to half-open
- `Half-Open Success Count`: 5 successful sends to close

**Benefits**:
- Prevents cascade failures
- Automatic recovery detection
- Gradual traffic restoration

### 3.3 Exponential Backoff Retries

Retry strategy for transient failures:

```
Attempt 1: Immediate
Attempt 2: Wait 100ms (RETRY_BACKOFF_MS)
Attempt 3: Wait 200ms
Attempt 4: Wait 400ms
Attempt 5: Wait 800ms
Attempt 6: Failed (MAX_RETRIES reached)
```

**Configuration**:
- `MAX_RETRIES`: Maximum retry attempts (default: 5)
- `RETRY_BACKOFF_MS`: Initial backoff duration (default: 100ms)

### 3.4 Atomic Metrics Collection

Thread-safe metrics without locks:

```go
type CollectorMetrics struct {
    messagesCollected   int64  // Messages read from sources
    messagesSent        int64  // Messages published successfully
    messagesFailed      int64  // Messages failed to publish
    batchesSent         int64  // Successful batch publishes
    bytesCollected      int64  // Total bytes processed
    collectionErrors    int64  // Collection failures
    retryAttempts       int64  // Total retry attempts made
    circuitBreakerTrips int64  // Circuit breaker transitions
}
```

**Snapshot Model**: Atomic reads ensure consistent metric views

## 4. Configuration

### 4.1 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_NAME` | telemetry-collector | Service identifier |
| `SERVICE_ID` | collector | Instance identifier |
| `HEALTH_PORT` | 8080 | Health check port |
| `LOG_LEVEL` | info | Logging level (debug/info/warn/error) |
| `READ_FREQUENCY_MS` | 1000 | Collection interval in milliseconds |
| `BATCH_SIZE` | 50 | Messages per batch |
| `BATCH_TIMEOUT_MS` | 5000 | Batch flush timeout in milliseconds |
| `MAX_POLL_RECORDS` | 100 | Maximum records per source read |
| `MAX_RETRIES` | 5 | Retry attempts for failed sends |
| `RETRY_BACKOFF_MS` | 100 | Initial retry backoff in milliseconds |
| `CIRCUIT_BREAKER_THRESHOLD` | 10 | Failures to trigger circuit break |
| `HEALTH_CHECK_INTERVAL` | 30 | Health check interval in seconds |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | 30 | Shutdown timeout in seconds |
| `BROKER_ADDRESSES` | localhost:9092 | Comma-separated broker addresses |
| `TOPIC` | telemetry-events | Message broker topic |

### 4.2 Tuning Profiles

**High-Throughput Mode** (Real-time collection):
```env
READ_FREQUENCY_MS=100        # Very frequent reads
BATCH_SIZE=200               # Larger batches
BATCH_TIMEOUT_MS=1000        # Aggressive flushing
MAX_POLL_RECORDS=500         # More records per read
```

**Low-Latency Mode** (Minimal delay):
```env
READ_FREQUENCY_MS=100        # Fast reads
BATCH_SIZE=10                # Small batches
BATCH_TIMEOUT_MS=500         # Quick flushing
MAX_POLL_RECORDS=50          # Conservative polling
```

**Resilient Mode** (Fault tolerance focus):
```env
MAX_RETRIES=10               # More retry attempts
RETRY_BACKOFF_MS=500         # Longer backoff
CIRCUIT_BREAKER_THRESHOLD=5  # Tighter tolerance
HEALTH_CHECK_INTERVAL=60     # Frequent health checks
```

## 5. Deployment Scenarios

### 5.1 Single Instance
- Monolithic deployment on single machine
- For low-volume telemetry collection
- Sufficient for proof-of-concept and testing

### 5.2 Multi-Instance (Kubernetes)
- Deploy 3+ replicas for high availability
- Use service mesh for load balancing
- Auto-scaling based on CPU/memory utilization
- StatelessDeployment pattern

### 5.3 Multi-Instance (Traditional Infrastructure)
- Deploy via Ansible to multiple hosts
- Use load balancer for distribution
- Systemd service management
- Central configuration management

## 6. Reliability & Resilience

### 6.1 Failure Handling

| Scenario | Response |
|----------|----------|
| Source read timeout | Log error, skip source, continue |
| Batch send fails | Retry with exponential backoff |
| Circuit breaker open | Return error, trigger health degradation |
| Broker unreachable | Exponential backoff + circuit breaker |
| OOM condition | Graceful shutdown signal |

### 6.2 Graceful Shutdown

1. Receive SIGINT/SIGTERM
2. Stop accepting new collections (grace period: `GRACEFUL_SHUTDOWN_TIMEOUT`)
3. Flush any pending batches
4. Close broker connections
5. Log final metrics
6. Exit with code 0

### 6.3 Health Checks

- **Liveness**: Service process running (Kubernetes LivenessProbe)
- **Readiness**: Service accepting traffic (Kubernetes ReadinessProbe)
- **Metrics**: Current operation status (Prometheus scrape)

## 7. Monitoring & Observability

### 7.1 Metrics Endpoints

**GET /metrics** - JSON metrics snapshot:
```json
{
  "messagesCollected": 1000,
  "messagesSent": 950,
  "messagesFailed": 50,
  "batchesSent": 20,
  "bytesCollected": 524288,
  "collectionErrors": 2,
  "retryAttempts": 15,
  "circuitBreakerTrips": 1
}
```

### 7.2 Logging

**Structured JSON Logging** (go.uber.org/zap):
```json
{
  "level": "info",
  "ts": "2024-01-15T10:30:45.123Z",
  "logger": "collector",
  "msg": "batch sent successfully",
  "batch_size": 50,
  "duration_ms": 145
}
```

### 7.3 Prometheus Integration

- Scrape `/metrics` endpoint at 30s interval
- Alert on circuit breaker trips
- Alert on collection error rate > 5%
- Track message latency percentiles

## 8. Source Collector Interface

Custom sources can be registered at runtime:

```go
type SourceCollectorFunc func(ctx context.Context) (map[string]interface{}, error)

collector.RegisterSource("custom_source", func(ctx context.Context) (map[string]interface{}, error) {
    return map[string]interface{}{
        "metric": value,
    }, nil
})
```

**Built-in Sources**:
- `system`: Uptime, health status
- `application`: Service running status

## 9. Performance Characteristics

### 9.1 Throughput

- **Single instance**: ~1000-5000 messages/sec (depends on source latency)
- **Scaling**: Linear with number of instances
- **Message size**: 100-1000 bytes typical

### 9.2 Latency

- **Collection latency**: ~10-50ms (read + batch)
- **Publish latency**: ~50-200ms (network + broker)
- **P99 latency**: <500ms (with circuit breaker overhead)

### 9.3 Resource Usage

- **Memory**: 50-100MB per instance
- **CPU**: 50-200m per instance (100% utilization = 1000m)
- **Network**: ~100-500KB/sec per instance

## 10. Security Considerations

- Non-root user execution (telemetry:1000)
- Read-only filesystem where possible
- TLS for broker communication (future)
- Resource limits (ulimits, OOM killer)
- Capability dropping in containers

## 11. Testing Strategy

### 11.1 Unit Tests
- Configuration validation
- Metrics atomic operations
- Circuit breaker state transitions
- Batch accumulation and flushing
- Error handling and retries

### 11.2 Integration Tests
- Multiple source collection
- Broker publish workflows
- Graceful shutdown
- Concurrent access patterns

### 11.3 Load Testing
- Throughput benchmarks
- Memory leak detection
- Circuit breaker under load
- Retry backoff timing

## 12. Future Enhancements

1. **TLS Support**: Encrypted broker communication
2. **Custom Metrics**: Prometheus client library integration
3. **Dead Letter Queue**: Failed message persistence
4. **Source Health**: Per-source circuit breakers
5. **Dynamic Configuration**: Runtime config updates
6. **Distributed Tracing**: OpenTelemetry integration
7. **Rate Limiting**: Token bucket algorithm
8. **Adaptive Batching**: Dynamic batch size based on load

---

**Document Version**: 1.0.0
**Last Updated**: 2024
