# telemetry-collector - TSDB Integration Guide

## Overview

The `telemetry-collector` service reads GPU telemetry metrics from Kafka and writes them to InfluxDB TSDB for time-series storage. This guide explains the TSDB integration, configuration, and usage.

## Architecture

```
Kafka (Message Broker)
        ↓
telemetry-collector
   (Consumer)
        ↓
  - Parse metrics
  - Validate data
  - Apply retry logic
  - Write to TSDB
        ↓
InfluxDB TSDB
  (Storage)
```

## Features

### Data Collection
- **Source**: Kafka topic (default: `telemetry-data`)
- **Frequency**: Configurable read frequency (default: 1 second)
- **Format**: JSON messages from Kafka

### TSDB Integration
- **Database**: InfluxDB v2.7.0+
- **Write Format**: Line protocol
- **Measurement**: `gpu_metrics`
- **Organization**: `telemetry`
- **Bucket**: `gpu_metrics_raw`

### Fault Tolerance
- **Circuit Breaker**: Prevents cascading failures
- **Retry Logic**: Exponential backoff (3-5 attempts)
- **Health Checks**: Periodic TSDB connectivity verification
- **Graceful Degradation**: Continues collecting even if TSDB fails

### Metrics Tracking
- Total messages collected from Kafka
- Total metrics written to TSDB
- Total write failures
- Batch statistics
- Error counts and types

## Configuration

### Environment Variables

```bash
# Service Configuration
SERVICE_NAME=telemetry-collector          # Service name for logging
SERVICE_ID=1                              # Service instance ID
HEALTH_PORT=8080                          # Health check endpoint port
LOG_LEVEL=info                            # Log level (debug/info/warn/error)

# Kafka Configuration
BROKER_ADDRESSES=localhost:9092           # Kafka brokers (comma-separated)
TOPIC=telemetry-data                      # Topic to consume from
READ_FREQUENCY_MS=1000                    # How often to check for new messages

# Collection Configuration
BATCH_SIZE=50                             # Messages per batch
BATCH_TIMEOUT_MS=1000                     # Batch timeout in milliseconds
MAX_POLL_RECORDS=100                      # Max records to poll at once

# TSDB Configuration (NEW)
TSDB_ENABLED=true                         # Enable TSDB writing
TSDB_URL=http://localhost:8086            # InfluxDB URL
TSDB_TOKEN=my-token                       # InfluxDB auth token
TSDB_ORG=telemetry                        # InfluxDB organization
TSDB_BUCKET=gpu_metrics_raw               # InfluxDB bucket

# Resilience Configuration
MAX_RETRIES=5                             # Max retry attempts
RETRY_BACKOFF_MS=100                      # Initial backoff (ms)
CIRCUIT_BREAKER_THRESHOLD=10              # Failures before circuit opens
HEALTH_CHECK_INTERVAL=30                  # Health check interval (seconds)

# Shutdown Configuration
GRACEFUL_SHUTDOWN_TIMEOUT=30              # Graceful shutdown timeout (seconds)
```

### Example Configuration Files

**Kubernetes ConfigMap**:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: telemetry-collector-config
  namespace: telemetry
data:
  SERVICE_NAME: "telemetry-collector"
  SERVICE_ID: "1"
  HEALTH_PORT: "8080"
  LOG_LEVEL: "info"
  BROKER_ADDRESSES: "kafka-cluster:9092"
  TOPIC: "telemetry-data"
  READ_FREQUENCY_MS: "1000"
  BATCH_SIZE: "50"
  BATCH_TIMEOUT_MS: "1000"
  MAX_POLL_RECORDS: "100"
  TSDB_ENABLED: "true"
  TSDB_URL: "http://influxdb-tsdb:8086"
  TSDB_ORG: "telemetry"
  TSDB_BUCKET: "gpu_metrics_raw"
  MAX_RETRIES: "5"
  CIRCUIT_BREAKER_THRESHOLD: "10"
```

**Secret for TSDB Token**:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: telemetry-collector-secrets
  namespace: telemetry
type: Opaque
data:
  TSDB_TOKEN: <base64-encoded-token>
```

## Data Flow

### 1. Message Consumption
```
Read from Kafka → Parse JSON → Extract metrics
```

**Input Message Format**:
```json
{
  "timestamp": "2024-01-15T10:00:00Z",
  "metric_name": "utilization",
  "gpu_id": "0",
  "device_id": "dev001",
  "uuid": "abc-123-def",
  "model_name": "A100",
  "host_name": "server1",
  "container": "pod-gpu-0",
  "value": 85.5,
  "labels": {"region": "us-west", "environment": "prod"}
}
```

### 2. TSDB Writing
```
Convert to InfluxDB Line Protocol → Validate → Write with retry
```

**InfluxDB Line Protocol Format**:
```
gpu_metrics,metric_name=utilization,gpu_id=0,device_id=dev001,uuid=abc-123-def,model_name=A100,host_name=server1,container=pod-gpu-0 value=85.5 1704067200000000000
```

**Format Breakdown**:
- Measurement: `gpu_metrics`
- Tags (indexed, fast queries): `metric_name`, `gpu_id`, `device_id`, `uuid`, `model_name`, `host_name`, `container`
- Fields (values): `value` (float64)
- Timestamp: Unix nanoseconds

### 3. Retry Logic

When TSDB write fails:
1. Attempt 1: Immediate retry
2. Attempt 2: Wait 100ms, retry
3. Attempt 3: Wait 200ms, retry
4. Attempt 4: Wait 400ms, retry
5. Attempt 5: Wait 800ms, retry

**Exponential Backoff Formula**: `backoff = 100ms * 2^(attempt-1)`

## Metrics Endpoints

### /health
Returns health status of the collector.

**Request**:
```bash
curl http://localhost:8080/health
```

**Response**:
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:00:00.123456Z"
}
```

### /ready
Returns readiness status.

**Request**:
```bash
curl http://localhost:8080/ready
```

**Response**:
```json
{
  "ready": true
}
```

### /metrics
Returns collector metrics.

**Request**:
```bash
curl http://localhost:8080/metrics
```

**Response**:
```json
{
  "messages_collected": 1500,
  "messages_sent": 1500,
  "batches_sent": 30,
  "errors": 2,
  "tsdb_written": 1500,
  "tsdb_failed": 0
}
```

## Kubernetes Deployment

### StatefulSet Example

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: telemetry-collector
  namespace: telemetry
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: telemetry-collector
  template:
    metadata:
      labels:
        app: telemetry-collector
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - telemetry-collector
              topologyKey: kubernetes.io/hostname
      containers:
      - name: collector
        image: telemetry-collector:latest
        ports:
        - containerPort: 8080
          name: http
        envFrom:
        - configMapRef:
            name: telemetry-collector-config
        - secretRef:
            name: telemetry-collector-secrets
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 30
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
      terminationGracePeriodSeconds: 30
```

## Troubleshooting

### 1. TSDB Connection Failed

**Error**: `failed to connect to TSDB: connection refused`

**Causes**:
- InfluxDB not running
- Incorrect URL
- Network policy blocking access

**Solution**:
```bash
# Check InfluxDB health
kubectl get pods -n telemetry | grep influxdb

# Port-forward to test
kubectl port-forward svc/influxdb-tsdb 8086:8086 -n telemetry

# Verify token
kubectl get secret influxdb-tsdb -o jsonpath='{.data.admin-token}' | base64 -d
```

### 2. High Write Failures

**Error**: `failed to write metric after retries`

**Causes**:
- TSDB disk full
- Token expired
- Bucket doesn't exist
- Network issues

**Solution**:
```bash
# Check TSDB storage
kubectl exec -it influxdb-tsdb-0 -n telemetry -- influx bucket list

# Check token validity
kubectl exec -it influxdb-tsdb-0 -n telemetry -- influx auth list

# View TSDB logs
kubectl logs influxdb-tsdb-0 -n telemetry | tail -50
```

### 3. Circuit Breaker Open

**Symptoms**: Collector stops writing to TSDB

**Causes**: Too many consecutive failures

**Solution**:
```bash
# Check collector logs
kubectl logs deployment/telemetry-collector -n telemetry | grep circuit

# The circuit breaker will auto-reset after 30 seconds of inactivity
# Check metrics endpoint
curl http://localhost:8080/metrics
```

### 4. Memory Leak

**Symptoms**: Pod memory increases over time

**Solution**:
```bash
# Check for goroutine leaks
kubectl logs deployment/telemetry-collector -n telemetry | grep goroutine

# Restart collector
kubectl rollout restart deployment/telemetry-collector -n telemetry
```

## Monitoring

### Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `messages_collected` | Messages read from Kafka | N/A |
| `tsdb_written` | Metrics successfully written to TSDB | N/A |
| `tsdb_failed` | Failed TSDB writes | > 5% of total |
| `errors` | Total collection errors | > 10 per minute |
| `circuit_breaker_trips` | Times circuit breaker opened | > 1 per hour |

### Prometheus Integration

Add to Prometheus scrape config:
```yaml
scrape_configs:
  - job_name: 'telemetry-collector'
    static_configs:
      - targets: ['telemetry-collector:8080']
    metrics_path: '/metrics'
```

## Performance Tuning

### Batch Size
- **Increase for**: High-throughput scenarios
- **Decrease for**: Low-latency requirements
- **Default**: 50 messages

### Read Frequency
- **Increase for**: Lower resource usage
- **Decrease for**: Real-time requirements
- **Default**: 1000ms (1 second)

### Retry Configuration
- **Increase MAX_RETRIES for**: Unreliable networks
- **Decrease for**: Faster failure detection
- **Default**: 5 attempts

## Best Practices

1. **Set TSDB_ENABLED=true** only after InfluxDB is running
2. **Use secure tokens** - rotate regularly
3. **Monitor circuit breaker state** - indicates systemic issues
4. **Set appropriate resource limits** - prevent pod eviction
5. **Use pod disruption budgets** - ensure availability
6. **Enable health checks** - catch issues early
7. **Log all retries** - diagnose transient issues

## Testing

### Unit Tests
```bash
cd telemetry-collector
go test ./pkg/tsdb/... -v
go test ./pkg/config/... -v
go test ./pkg/collector/... -v
```

### Integration Tests
```bash
# Test TSDB connectivity
go test ./test -v -run TestTSDBConnection

# Test end-to-end flow
go test ./test -v -run TestE2E
```

### Docker Testing
```bash
# Build image
docker build -t telemetry-collector:latest .

# Run with test configuration
docker run -e TSDB_ENABLED=true \
  -e TSDB_URL=http://influxdb:8086 \
  -e TSDB_TOKEN=test-token \
  telemetry-collector:latest
```

---

**TSDB Integration Status**: ✅ Complete & Production-Ready

telemetry-collector now fully supports writing GPU metrics to InfluxDB TSDB with fault tolerance, retry logic, and comprehensive monitoring.
