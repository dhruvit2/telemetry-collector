# Configuration Guide - Telemetry Collector

## Overview

Telemetry Collector is configured entirely through environment variables. This guide covers all configuration options, their defaults, and recommended values for different deployment scenarios.

## Environment Variables

### Service Identity

**SERVICE_NAME**
- Type: String
- Default: `telemetry-collector`
- Description: Service name for identification in logs and monitoring
- Example: `SERVICE_NAME=telemetry-collector`

**SERVICE_ID**
- Type: String
- Default: `collector`
- Description: Instance identifier for metrics and logging (useful in multi-instance deployments)
- Example: `SERVICE_ID=collector-1`

### Operational Configuration

**HEALTH_PORT**
- Type: Integer
- Default: `8080`
- Valid Range: 1024-65535
- Description: Port for health check endpoints (/health, /ready, /metrics)
- Example: `HEALTH_PORT=8080`

**LOG_LEVEL**
- Type: String (enum)
- Default: `info`
- Valid Values: `debug`, `info`, `warn`, `error`
- Description: Structured logging level
  - `debug`: Verbose logging including low-level operations
  - `info`: Normal operational logging
  - `warn`: Warning-level events and above
  - `error`: Error-level events only
- Example: `LOG_LEVEL=info`

### Collection Configuration

**READ_FREQUENCY_MS**
- Type: Integer
- Default: `1000`
- Valid Range: 100-60000
- Description: How frequently to collect telemetry from sources (in milliseconds)
- Example: `READ_FREQUENCY_MS=1000`
- **Important**: This is the PRIMARY parameter for controlling throughput
  - Lower values = more frequent reads = higher throughput
  - Higher values = less frequent reads = lower latency jitter
  - Scaling: Multiple instances run independently, so 3×1000ms instances = 3× throughput

**BATCH_SIZE**
- Type: Integer
- Default: `50`
- Valid Range: 1-1000
- Description: Target number of messages per batch to the message broker
- Example: `BATCH_SIZE=50`
- Notes:
  - Larger batches = better network efficiency
  - Smaller batches = lower latency
  - Total memory usage ≈ BATCH_SIZE × average_message_size

**BATCH_TIMEOUT_MS**
- Type: Integer
- Default: `5000`
- Valid Range: 100-60000
- Description: Maximum time to wait before flushing a non-full batch (in milliseconds)
- Example: `BATCH_TIMEOUT_MS=5000`
- Notes:
  - Ensures timely publishing even with low message rate
  - Should be >= READ_FREQUENCY_MS

**MAX_POLL_RECORDS**
- Type: Integer
- Default: `100`
- Valid Range: 1-10000
- Description: Maximum records to read from each source in a single collection cycle
- Example: `MAX_POLL_RECORDS=100`
- Notes:
  - Controls memory usage per collection cycle
  - Higher values allow more throughput but increase memory pressure

### Resilience Configuration

**MAX_RETRIES**
- Type: Integer
- Default: `5`
- Valid Range: 0-20
- Description: Maximum retry attempts for failed batch sends
- Example: `MAX_RETRIES=5`
- Notes:
  - More retries = longer recovery time for transient failures
  - Fewer retries = faster failure detection but more message loss

**RETRY_BACKOFF_MS**
- Type: Integer
- Default: `100`
- Valid Range: 10-5000
- Description: Initial backoff duration for retry (in milliseconds), doubles on each attempt
- Example: `RETRY_BACKOFF_MS=100`
- Sequence:
  - Attempt 1: Immediate
  - Attempt 2: 100ms
  - Attempt 3: 200ms
  - Attempt 4: 400ms
  - Attempt 5: 800ms
  - Attempt 6: 1600ms (if MAX_RETRIES=6)

**CIRCUIT_BREAKER_THRESHOLD**
- Type: Integer
- Default: `10`
- Valid Range: 1-100
- Description: Number of consecutive failures before circuit breaker opens
- Example: `CIRCUIT_BREAKER_THRESHOLD=10`
- Notes:
  - Circuit breaker prevents cascading failures
  - Opens after N failures, then rejects requests for 30 seconds
  - Reopens with 5 consecutive successes during recovery phase

### Operational Tuning

**HEALTH_CHECK_INTERVAL**
- Type: Integer
- Default: `30`
- Valid Range: 5-300
- Description: Health check evaluation interval (in seconds)
- Example: `HEALTH_CHECK_INTERVAL=30`

**GRACEFUL_SHUTDOWN_TIMEOUT**
- Type: Integer
- Default: `30`
- Valid Range: 5-300
- Description: Maximum time to wait for graceful shutdown before force-kill (in seconds)
- Example: `GRACEFUL_SHUTDOWN_TIMEOUT=30`
- Notes:
  - On SIGTERM/SIGINT, service waits this long before exiting
  - Final batch is flushed during this period

### Message Broker Configuration

**BROKER_ADDRESSES**
- Type: String (comma-separated)
- Default: `localhost:9092`
- Description: Message broker addresses in format `host1:port1,host2:port2,...`
- Examples:
  - Single broker: `BROKER_ADDRESSES=kafka.example.com:9092`
  - Multiple brokers: `BROKER_ADDRESSES=kafka-1:9092,kafka-2:9092,kafka-3:9092`
- Notes:
  - MUST be accessible from collector instance
  - Use DNS names for cloud/Kubernetes deployments
  - Supports Kafka-compatible brokers

**TOPIC**
- Type: String
- Default: `telemetry-events`
- Description: Message broker topic to publish to
- Example: `TOPIC=telemetry-events`
- Notes:
  - Topic MUST exist on broker before startup
  - Multiple instances can publish to same topic
  - Consider partitioning for distributed consumption

## Configuration Profiles

### 1. Development/Testing

For local development and testing:

```env
LOG_LEVEL=debug
READ_FREQUENCY_MS=5000          # Slow collection
BATCH_SIZE=10                   # Small batches
BATCH_TIMEOUT_MS=10000          # Long timeout
MAX_RETRIES=2                   # Fewer retries
HEALTH_PORT=8080
BROKER_ADDRESSES=localhost:9092
TOPIC=telemetry-test
```

### 2. Production - High Throughput

For maximum message throughput:

```env
LOG_LEVEL=info
READ_FREQUENCY_MS=100           # Very frequent collection
BATCH_SIZE=200                  # Large batches
BATCH_TIMEOUT_MS=1000           # Aggressive flushing
MAX_POLL_RECORDS=500            # Higher poll limit
MAX_RETRIES=7                   # More robust
RETRY_BACKOFF_MS=50             # Faster backoff
CIRCUIT_BREAKER_THRESHOLD=20    # Tolerant
HEALTH_PORT=8080
BROKER_ADDRESSES=kafka-1:9092,kafka-2:9092,kafka-3:9092
TOPIC=telemetry-events
```

### 3. Production - Low Latency

For minimal message latency:

```env
LOG_LEVEL=info
READ_FREQUENCY_MS=100           # Fast collection
BATCH_SIZE=10                   # Small batches
BATCH_TIMEOUT_MS=500            # Quick flushing
MAX_POLL_RECORDS=50             # Conservative polling
MAX_RETRIES=5
RETRY_BACKOFF_MS=100
CIRCUIT_BREAKER_THRESHOLD=10
HEALTH_PORT=8080
BROKER_ADDRESSES=kafka-1:9092,kafka-2:9092,kafka-3:9092
TOPIC=telemetry-events
```

### 4. Production - High Reliability

For maximum fault tolerance:

```env
LOG_LEVEL=info
READ_FREQUENCY_MS=2000          # Conservative collection
BATCH_SIZE=50                   # Moderate batches
BATCH_TIMEOUT_MS=5000           # Patient flushing
MAX_POLL_RECORDS=100            # Standard polling
MAX_RETRIES=10                  # Very robust
RETRY_BACKOFF_MS=200            # Generous backoff
CIRCUIT_BREAKER_THRESHOLD=5     # Sensitive
HEALTH_CHECK_INTERVAL=60        # Frequent health checks
GRACEFUL_SHUTDOWN_TIMEOUT=60    # Long shutdown window
HEALTH_PORT=8080
BROKER_ADDRESSES=kafka-1:9092,kafka-2:9092,kafka-3:9092
TOPIC=telemetry-events
```

### 5. Kubernetes - Auto-scaling

Tuned for horizontal auto-scaling:

```env
LOG_LEVEL=info
READ_FREQUENCY_MS=1000
BATCH_SIZE=50
BATCH_TIMEOUT_MS=5000
MAX_POLL_RECORDS=100
MAX_RETRIES=5
CIRCUIT_BREAKER_THRESHOLD=10
HEALTH_CHECK_INTERVAL=30
GRACEFUL_SHUTDOWN_TIMEOUT=30
HEALTH_PORT=8080
BROKER_ADDRESSES=kafka-0.kafka-headless:9092,kafka-1.kafka-headless:9092,kafka-2.kafka-headless:9092
TOPIC=telemetry-events
```

## Deployment Configuration Methods

### 1. Kubernetes ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: telemetry-collector-config
data:
  LOG_LEVEL: "info"
  READ_FREQUENCY_MS: "1000"
  BATCH_SIZE: "50"
  BROKER_ADDRESSES: "kafka:9092"
  TOPIC: "telemetry-events"
```

Reference in Deployment:
```yaml
envFrom:
- configMapRef:
    name: telemetry-collector-config
```

### 2. Docker Environment File

Create `.env` file:
```env
LOG_LEVEL=info
READ_FREQUENCY_MS=1000
BATCH_SIZE=50
BROKER_ADDRESSES=kafka:9092
TOPIC=telemetry-events
```

Run container:
```bash
docker run --env-file .env telemetry-collector:latest
```

### 3. Ansible Variables

In `deployment/ansible/deploy.yml`:
```yaml
vars:
  log_level: "info"
  read_frequency_ms: 1000
  batch_size: 50
  broker_addresses: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  topic: "telemetry-events"
```

### 4. Helm Values

In `values.yaml`:
```yaml
config:
  logLevel: "info"
  readFrequencyMs: 1000
  batchSize: 50

messagebroker:
  addresses: "kafka-0:9092,kafka-1:9092,kafka-2:9092"
  topic: "telemetry-events"
```

## Configuration Validation

The service validates configuration on startup:

```
✓ READ_FREQUENCY_MS must be > 0
✓ MAX_RETRIES must be >= 0
✓ BROKER_ADDRESSES must not be empty
✓ TOPIC must not be empty
✓ CIRCUIT_BREAKER_THRESHOLD must be > 0
✓ All port values must be in range 1-65535
```

If validation fails, service exits with error code 1.

## Performance Tuning Guide

### Increasing Throughput

1. **Decrease READ_FREQUENCY_MS**: More frequent collection
   - 1000ms → 500ms = 2× throughput
   - Monitor CPU and memory impact

2. **Increase BATCH_SIZE**: Larger batches to broker
   - 50 → 200 = 4× network efficiency
   - But increases memory usage

3. **Increase MAX_POLL_RECORDS**: Read more per cycle
   - 100 → 500 = higher parallelism
   - Check source performance

4. **Horizontal Scaling**: Deploy more instances
   - 1 instance → 3 instances = 3× throughput
   - Most effective for stateless workloads

### Reducing Latency

1. **Increase READ_FREQUENCY_MS**: Less frequent checks
   - Reduces jitter in collection cycle
   - Lower overall throughput

2. **Decrease BATCH_TIMEOUT_MS**: Flush more aggressively
   - 5000ms → 1000ms = lower latency
   - But more network overhead

3. **Decrease BATCH_SIZE**: Smaller batches
   - Fewer messages queued = lower latency
   - More overhead per message

### Improving Reliability

1. **Increase MAX_RETRIES**: More recovery attempts
   - Handles transient failures better
   - Slower failure detection

2. **Adjust RETRY_BACKOFF_MS**: Control retry speed
   - Higher value = longer recovery time
   - Prevents overwhelming broker

3. **Lower CIRCUIT_BREAKER_THRESHOLD**: Open faster
   - 10 → 5 failures to open
   - Protects broker better

## Monitoring Configuration

Check current configuration:

```bash
# Kubernetes
kubectl get cm telemetry-collector-config -o yaml

# Systemd
cat /etc/telemetry-collector/collector.env

# Docker
docker inspect telemetry-collector | grep -A 50 "Env"
```

Verify runtime configuration:

```bash
# Via metrics endpoint
curl http://localhost:8080/metrics
```

## Changing Configuration

### Kubernetes (Rolling Update)

```bash
# Update ConfigMap
kubectl edit cm telemetry-collector-config

# Rollout restart to apply
kubectl rollout restart deployment/telemetry-collector
```

### Systemd (Machine)

```bash
# Edit configuration
sudo nano /etc/telemetry-collector/collector.env

# Restart service
sudo systemctl restart telemetry-collector
```

### Docker Compose

Update `.env` file and restart:
```bash
docker-compose restart telemetry-collector
```

## Troubleshooting Configuration Issues

### Service won't start

1. Check configuration syntax: `cat /etc/telemetry-collector/collector.env`
2. Validate required variables are set
3. Check logs: `journalctl -u telemetry-collector -n 20`

### Broker connection fails

1. Verify BROKER_ADDRESSES: `telnet <host> <port>`
2. Check firewall rules
3. Ensure topic exists on broker
4. Review logs for connection errors

### Performance issues

1. Check resource limits: `free -h`, `top`
2. Review metrics: `curl /metrics | jq`
3. Adjust configuration profile
4. Consider horizontal scaling

### Memory leaks

1. Monitor memory over time
2. Check for unbounded queues
3. Verify batch_size is reasonable
4. Review garbage collection logs

---

**Version**: 1.0.0
**Last Updated**: 2024
