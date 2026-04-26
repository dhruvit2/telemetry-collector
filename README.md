# Telemetry Collector

> A highly available, fault-tolerant service for collecting and aggregating telemetry data from multiple sources with configurable collection frequency and horizontal scaling support.

## Overview

Telemetry Collector is a production-ready Go microservice that:
- Collects telemetry data from multiple configurable sources
- Batches and publishes data to a message broker (Kafka-compatible)
- Implements circuit breaker pattern for resilience
- Scales horizontally by running multiple instances
- Provides comprehensive health check endpoints
- Includes structured JSON logging via zap

### Key Features

✅ **Highly Available**: 3+ replicas, graceful shutdown, health checks
✅ **Fault Tolerant**: Circuit breaker + exponential backoff retries
✅ **Configurable**: Collection frequency adjustable per deployment
✅ **Horizontally Scalable**: Increase throughput by adding instances
✅ **Observable**: Metrics endpoint, structured logging, health probes
✅ **Well-Tested**: 30+ unit tests with concurrent access testing
✅ **Containerized**: Multi-stage Docker build, Alpine runtime
✅ **Deployment Ready**: Kubernetes Helm, Ansible playbooks, Makefile

## Quick Start

### Docker

```bash
# Build Docker image
make docker-build

# Run container
docker run -d \
  --name telemetry-collector \
  -p 8080:8080 \
  -e BROKER_ADDRESSES="localhost:9092" \
  -e TOPIC="telemetry" \
  -e READ_FREQUENCY_MS="1000" \
  telemetry-collector:latest

# Check health
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```

### Kubernetes (Helm)

```bash
# Install Helm chart
make helm-install

# Verify deployment
kubectl get pods -l app.kubernetes.io/name=telemetry-collector

# Check metrics
kubectl port-forward svc/telemetry-collector 8080:8080
curl http://localhost:8080/metrics
```

### Local Machine (Ansible)

```bash
# Deploy to inventory hosts
make ansible-deploy

# Verify service
sudo systemctl status telemetry-collector

# View logs
journalctl -u telemetry-collector -f
```

## Architecture

### High-Level Design

```
Collection Loop (Configurable Frequency)
            ↓
    Source Collectors (Pluggable)
            ↓
    Batching & Queueing
            ↓
    Circuit Breaker + Retries
            ↓
    Message Broker Publisher
            ↓
    Metrics & Health Endpoints
```

### Key Components

| Component | Purpose |
|-----------|---------|
| **Config** | Environment-based configuration management |
| **Collector** | Main orchestrator with circuit breaker logic |
| **Metrics** | Thread-safe atomic metrics collection |
| **HTTP Handler** | Health check and metrics endpoints |

See [DESIGN.md](DESIGN.md) for comprehensive architecture documentation.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `READ_FREQUENCY_MS` | 1000 | How often to collect (milliseconds) |
| `BATCH_SIZE` | 50 | Messages per batch |
| `BATCH_TIMEOUT_MS` | 5000 | Batch flush timeout |
| `MAX_RETRIES` | 5 | Retry attempts on failure |
| `CIRCUIT_BREAKER_THRESHOLD` | 10 | Failures before circuit opens |
| `BROKER_ADDRESSES` | localhost:9092 | Message broker addresses |
| `TOPIC` | telemetry-events | Message broker topic |
| `LOG_LEVEL` | info | Logging level (debug/info/warn/error) |

See [CONFIGURATION.md](CONFIGURATION.md) for detailed configuration guide.

## Deployment

### Kubernetes

```bash
# Install with custom values
helm install telemetry-collector ./deployment/helm/telemetry-collector \
  --values custom-values.yaml \
  --namespace monitoring \
  --create-namespace

# Upgrade configuration
helm upgrade telemetry-collector ./deployment/helm/telemetry-collector \
  --values custom-values.yaml
```

### Machine Deployment (Ansible)

```bash
# Deploy to all hosts in inventory
ansible-playbook deployment/ansible/deploy.yml -i inventory.ini

# Deploy to specific hosts
ansible-playbook deployment/ansible/deploy.yml -i inventory.ini --limit "collector_group"

# Verify health after deployment
make ansible-deploy
```

### Docker Compose

```yaml
version: '3.8'
services:
  telemetry-collector:
    image: telemetry-collector:latest
    ports:
      - "8080:8080"
    environment:
      BROKER_ADDRESSES: "messagebroker:9092"
      TOPIC: "telemetry"
      READ_FREQUENCY_MS: "1000"
      LOG_LEVEL: "info"
    depends_on:
      - messagebroker
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for comprehensive deployment guide.

## Monitoring & Observability

### Health Endpoints

```bash
# Service health status
curl http://localhost:8080/health

# Readiness probe
curl http://localhost:8080/ready

# Metrics snapshot (JSON)
curl http://localhost:8080/metrics
```

### Metrics

The `/metrics` endpoint provides:
- `messagesCollected`: Total messages read from sources
- `messagesSent`: Successfully published messages
- `messagesFailed`: Failed publish attempts
- `batchesSent`: Successful batch publishes
- `bytesCollected`: Total bytes processed
- `collectionErrors`: Collection operation failures
- `retryAttempts`: Total retry attempts
- `circuitBreakerTrips`: Circuit breaker transitions

### Kubernetes Monitoring

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: telemetry-collector
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: telemetry-collector
  endpoints:
  - port: http
    path: /metrics
    interval: 30s
```

## Building

### Local Build

```bash
# Build binary
make build

# Build Linux binary
make build-local

# Run tests
make test

# Run linter
make lint

# Format code
make fmt
```

### Docker Build

```bash
# Build Docker image
make docker-build

# Push to registry
make docker-push

# Run Docker container locally
make docker-run
```

## Testing

### Unit Tests

```bash
# Run all tests
make test

# Run specific test file
go test -v ./pkg/collector

# Run with coverage
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Test Coverage

The project includes:
- **Config Tests**: 20+ test cases for configuration validation
- **Metrics Tests**: Concurrent access and atomic operation tests
- **Collector Tests**: Circuit breaker state machine and collection workflow tests
- **Integration Tests**: End-to-end collection and publishing scenarios

Run tests with:
```bash
make test
```

## API Reference

### GET /health

Service health status endpoint.

**Response (200 OK)**:
```json
{
  "status": "healthy",
  "uptime": 3600,
  "timestamp": "2024-01-15T10:30:45Z"
}
```

### GET /ready

Readiness probe endpoint.

**Response (200 OK)**:
```json
{
  "ready": true,
  "timestamp": "2024-01-15T10:30:45Z"
}
```

### GET /metrics

Metrics snapshot endpoint.

**Response (200 OK)**:
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

## Scaling

### Horizontal Scaling

Increase throughput by deploying multiple instances:

```yaml
# Kubernetes - adjust replicas
replicas: 5

# Kubernetes - enable auto-scaling
autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
```

### Throughput Tuning

```env
# High throughput mode
READ_FREQUENCY_MS=100
BATCH_SIZE=200
BATCH_TIMEOUT_MS=1000
```

See [CONFIGURATION.md](CONFIGURATION.md) for tuning profiles.

## Troubleshooting

### Service not starting

```bash
# Check logs
journalctl -u telemetry-collector -n 50
# or for Kubernetes
kubectl logs deployment/telemetry-collector

# Verify configuration
cat /etc/telemetry-collector/collector.env  # systemd
kubectl get configmap telemetry-collector -o yaml  # Kubernetes
```

### High failure rate

1. Check broker connectivity: `telnet <BROKER_HOST> 9092`
2. Verify topic exists: Check broker logs
3. Check circuit breaker status: `curl /metrics`
4. Review logs for error patterns: `journalctl -u telemetry-collector -f`

### Memory leak

```bash
# Monitor memory usage
watch -n 1 'ps aux | grep collector'

# Collect metrics over time
for i in {1..10}; do curl -s /metrics | jq .bytesCollected; sleep 10; done
```

## Development

### Project Structure

```
telemetry-collector/
├── cmd/
│   └── collector/              # Entry point
│       └── main.go
├── pkg/
│   ├── config/                 # Configuration management
│   │   ├── config.go
│   │   └── config_test.go
│   ├── metrics/                # Metrics collection
│   │   ├── metrics.go
│   │   └── metrics_test.go
│   └── collector/              # Core collector logic
│       ├── collector.go
│       └── collector_test.go
├── deployment/
│   ├── helm/                   # Kubernetes Helm charts
│   │   └── telemetry-collector/
│   └── ansible/                # Ansible playbooks
│       └── deploy.yml
├── Dockerfile                  # Multi-stage build
├── Makefile                    # Build & deploy targets
├── go.mod                      # Dependencies
├── DESIGN.md                   # Architecture
├── README.md                   # This file
├── CONFIGURATION.md            # Configuration guide
└── DEPLOYMENT.md               # Deployment guide
```

### Adding Custom Sources

```go
// In cmd/collector/main.go

collector.RegisterSource("custom_metric", func(ctx context.Context) (map[string]interface{}, error) {
    return map[string]interface{}{
        "value": 42,
        "unit": "requests/sec",
    }, nil
})
```

## Contributing

1. Write tests first (TDD)
2. Ensure all tests pass: `make test`
3. Run formatter: `make fmt`
4. Run linter: `make lint`
5. Submit PR with clear description

## Performance

- **Single Instance**: ~1000-5000 messages/sec
- **Memory**: 50-100MB per instance
- **CPU**: 50-200m per instance
- **Network**: 100-500KB/sec per instance

## Security

- Runs as non-root user (`telemetry:1000`)
- Read-only filesystem in containers
- Resource limits enforced
- Graceful error handling without exposing internals

## License

[Your License Here]

## Support

- **Issues**: [GitHub Issues](https://github.com/dhruvit2/telemetry-collector/issues)
- **Documentation**: See [DESIGN.md](DESIGN.md), [CONFIGURATION.md](CONFIGURATION.md), [DEPLOYMENT.md](DEPLOYMENT.md)
- **Questions**: Reach out to platform team

---

**Version**: 1.0.0
**Last Updated**: 2024
# telemetry-collector
