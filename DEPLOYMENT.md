# Deployment Guide - Telemetry Collector

## Overview

This guide covers deploying Telemetry Collector across different platforms:
- **Kubernetes**: Helm charts for cloud-native deployments
- **Machine Deployments**: Ansible playbooks for traditional infrastructure
- **Docker**: Local development and Docker Compose
- **Manual**: Step-by-step systemd setup

## Kubernetes Deployment

### Prerequisites

- Kubernetes 1.20+ cluster
- kubectl configured
- Helm 3+
- Message broker (Kafka) accessible
- Adequate CPU/Memory: 500m CPU + 256Mi RAM per replica

### Quick Start

```bash
# Install default deployment (3 replicas)
helm install telemetry-collector ./deployment/helm/telemetry-collector \
  --namespace monitoring \
  --create-namespace

# Verify deployment
kubectl get pods -l app.kubernetes.io/name=telemetry-collector
kubectl logs -l app.kubernetes.io/name=telemetry-collector -f
```

### Custom Configuration

Create `custom-values.yaml`:

```yaml
replicaCount: 5

image:
  tag: "1.0.0"

resources:
  requests:
    cpu: 250m
    memory: 128Mi
  limits:
    cpu: 2000m
    memory: 1Gi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 60

config:
  logLevel: "info"
  readFrequencyMs: 500

messagebroker:
  addresses: "kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092"
  topic: "telemetry"
```

Install with custom values:
```bash
helm install telemetry-collector ./deployment/helm/telemetry-collector \
  --namespace monitoring \
  --create-namespace \
  --values custom-values.yaml
```

### Helm Chart Structure

```
deployment/helm/telemetry-collector/
├── Chart.yaml                 # Chart metadata
├── values.yaml                # Default configuration
└── templates/
    ├── _helpers.tpl          # Template functions
    ├── deployment.yaml        # Kubernetes Deployment
    ├── service.yaml          # Kubernetes Service
    ├── serviceaccount.yaml   # ServiceAccount
    └── hpa.yaml              # HorizontalPodAutoscaler
```

### Helm Operations

```bash
# Lint chart
helm lint ./deployment/helm/telemetry-collector

# Template validation (dry run)
helm template telemetry-collector ./deployment/helm/telemetry-collector \
  --values custom-values.yaml

# Package chart for distribution
helm package ./deployment/helm/telemetry-collector

# Install from package
helm install telemetry-collector telemetry-collector-1.0.0.tgz

# Upgrade existing deployment
helm upgrade telemetry-collector ./deployment/helm/telemetry-collector \
  --values updated-values.yaml

# Rollback to previous version
helm rollback telemetry-collector 1

# List releases
helm list -n monitoring

# Get current values
helm get values telemetry-collector -n monitoring
```

### Health Checks in Kubernetes

The Helm chart includes:
- **Liveness Probe**: Restarts pod if /health fails 3 times
- **Readiness Probe**: Removes from load balancer if /ready fails 2 times
- **PodDisruptionBudget**: (optional) Prevents concurrent pod disruptions

```yaml
# In pod spec
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 10
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Monitoring in Kubernetes

```bash
# Port-forward for local access
kubectl port-forward svc/telemetry-collector 8080:8080 -n monitoring

# View metrics
curl http://localhost:8080/metrics | jq

# View logs
kubectl logs -l app.kubernetes.io/name=telemetry-collector -n monitoring -f

# Check events
kubectl describe pod <pod-name> -n monitoring

# Resource usage
kubectl top pods -l app.kubernetes.io/name=telemetry-collector -n monitoring
```

### Kubernetes Manifests (Without Helm)

If using plain YAML instead of Helm:

```bash
# Generate manifests
helm template telemetry-collector ./deployment/helm/telemetry-collector \
  -f custom-values.yaml > manifests.yaml

# Deploy
kubectl apply -f manifests.yaml

# Update
kubectl apply -f manifests.yaml  # Reapply with changes

# Delete
kubectl delete -f manifests.yaml
```

## Machine Deployment (Ansible)

### Prerequisites

- Ansible 2.9+
- Target machines: Linux (Debian/RHEL)
- SSH access with sudo privileges
- Message broker accessible
- Go 1.21+ installed OR pre-built binary

### Quick Start

Create inventory file `inventory.ini`:

```ini
[telemetry_collectors]
collector1 ansible_host=192.168.1.10
collector2 ansible_host=192.168.1.11
collector3 ansible_host=192.168.1.12

[telemetry_collectors:vars]
ansible_user=ubuntu
ansible_ssh_private_key_file=~/.ssh/id_rsa
```

Deploy:
```bash
ansible-playbook deployment/ansible/deploy.yml -i inventory.ini
```

### Ansible Roles

The playbook includes 4 roles:

1. **install-dependencies**: System packages and user setup
2. **configure-collector**: Systemd service and environment
3. **deploy-collector**: Binary installation and activation
4. **health-check**: Verification and health probe testing

### Custom Variables

Create `vars.yml`:

```yaml
---
install_dependencies: true
service_id: "collector"
health_port: 8080
log_level: "info"
read_frequency_ms: 1000
batch_size: 50
broker_addresses: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
topic: "telemetry-events"
collector_download_url: "https://releases.example.com/telemetry-collector/v1.0.0/collector"
save_manifest: true
```

Deploy with custom variables:
```bash
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  -e @vars.yml
```

### Ansible Operations

```bash
# Dry run (check mode)
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  --check

# Deploy to specific hosts
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  --limit "collector1,collector2"

# Deploy only configure role
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  --tags configure

# Skip dependencies installation
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  -e install_dependencies=false

# Verbose output
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  -v  # or -vv, -vvv for more verbosity
```

### Service Management

After deployment with Ansible:

```bash
# Check service status
sudo systemctl status telemetry-collector

# View logs
journalctl -u telemetry-collector -f

# Restart service
sudo systemctl restart telemetry-collector

# Stop service
sudo systemctl stop telemetry-collector

# Start service
sudo systemctl start telemetry-collector

# Reload configuration
sudo systemctl reload telemetry-collector

# Enable on boot
sudo systemctl enable telemetry-collector

# Disable autostart
sudo systemctl disable telemetry-collector
```

### File Locations

After Ansible deployment:

```
/opt/telemetry/
├── bin/
│   └── collector              # Executable
├── config/
├── logs/
└── [other app files]

/etc/telemetry-collector/
└── collector.env             # Environment variables

/etc/systemd/system/
└── telemetry-collector.service  # Systemd unit

/var/log/
└── [systemd journal logs]

/etc/logrotate.d/
└── telemetry-collector       # Log rotation config
```

## Docker Deployment

### Build Docker Image

```bash
# Standard build
docker build -t telemetry-collector:latest .

# Build with tag
docker build -t myregistry.azurecr.io/telemetry-collector:1.0.0 .

# Build with build args
docker build --build-arg GO_VERSION=1.21 -t telemetry-collector .
```

### Run Docker Container

```bash
# Interactive
docker run -it \
  -p 8080:8080 \
  -e BROKER_ADDRESSES="kafka:9092" \
  -e TOPIC="telemetry" \
  -e LOG_LEVEL="info" \
  telemetry-collector:latest

# Detached
docker run -d \
  --name telemetry-collector \
  -p 8080:8080 \
  --restart unless-stopped \
  -e BROKER_ADDRESSES="kafka:9092" \
  -e TOPIC="telemetry" \
  telemetry-collector:latest

# With volume mounts
docker run -d \
  --name telemetry-collector \
  -p 8080:8080 \
  -v /data/telemetry:/data \
  -v /etc/telemetry:/etc/telemetry \
  telemetry-collector:latest

# With resource limits
docker run -d \
  --name telemetry-collector \
  -p 8080:8080 \
  --memory=512m \
  --cpus=1.0 \
  telemetry-collector:latest
```

### Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  telemetry-collector:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: telemetry-collector
    ports:
      - "8080:8080"
    environment:
      BROKER_ADDRESSES: "messagebroker:9092"
      TOPIC: "telemetry"
      LOG_LEVEL: "info"
      READ_FREQUENCY_MS: "1000"
      BATCH_SIZE: "50"
    depends_on:
      - messagebroker
    restart: unless-stopped
    networks:
      - telemetry-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s

  messagebroker:
    image: confluentinc/cp-kafka:7.0.0
    container_name: messagebroker
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    depends_on:
      - zookeeper
    networks:
      - telemetry-net

  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.0
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - telemetry-net

networks:
  telemetry-net:
```

Deploy:
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f telemetry-collector

# Stop services
docker-compose down

# Remove volumes
docker-compose down -v
```

## Manual Deployment (Systemd)

For detailed manual setup without Ansible:

### 1. Create User and Directories

```bash
sudo useradd -r -s /bin/false telemetry
sudo mkdir -p /opt/telemetry/{bin,config,logs}
sudo chown -R telemetry:telemetry /opt/telemetry
```

### 2. Deploy Binary

```bash
# Build locally
go build -o telemetry-collector ./cmd/collector

# Copy to target
sudo cp telemetry-collector /opt/telemetry/bin/
sudo chmod 755 /opt/telemetry/bin/telemetry-collector
```

### 3. Create Configuration

```bash
sudo tee /etc/telemetry-collector/collector.env > /dev/null <<EOF
SERVICE_NAME=telemetry-collector
SERVICE_ID=collector
HEALTH_PORT=8080
LOG_LEVEL=info
READ_FREQUENCY_MS=1000
BATCH_SIZE=50
BROKER_ADDRESSES=localhost:9092
TOPIC=telemetry-events
EOF
sudo chmod 600 /etc/telemetry-collector/collector.env
```

### 4. Create Systemd Service

```bash
sudo tee /etc/systemd/system/telemetry-collector.service > /dev/null <<EOF
[Unit]
Description=Telemetry Collector Service
After=network.target

[Service]
Type=simple
User=telemetry
Group=telemetry
WorkingDirectory=/opt/telemetry
ExecStart=/opt/telemetry/bin/telemetry-collector
Restart=on-failure
RestartSec=10
EnvironmentFile=/etc/telemetry-collector/collector.env

[Install]
WantedBy=multi-user.target
EOF
```

### 5. Enable and Start

```bash
sudo systemctl daemon-reload
sudo systemctl enable telemetry-collector
sudo systemctl start telemetry-collector
sudo systemctl status telemetry-collector
```

## Monitoring and Verification

### Health Checks

```bash
# Health endpoint
curl http://localhost:8080/health

# Readiness endpoint
curl http://localhost:8080/ready

# Metrics endpoint
curl http://localhost:8080/metrics | jq
```

### Logs

```bash
# Systemd
journalctl -u telemetry-collector -f

# Docker
docker logs -f telemetry-collector

# File (if configured)
tail -f /var/log/telemetry-collector.log
```

### Performance Metrics

```bash
# CPU and Memory
ps aux | grep telemetry-collector

# Network connections
netstat -an | grep 8080

# Metrics snapshot
curl -s http://localhost:8080/metrics | jq .
```

## Troubleshooting

### Service fails to start

1. Check logs: `journalctl -u telemetry-collector -n 50`
2. Verify configuration: `cat /etc/telemetry-collector/collector.env`
3. Test broker connectivity: `telnet <broker> 9092`
4. Check permissions: `ls -la /opt/telemetry/bin/`

### High error rate

1. Check broker: `docker logs messagebroker`
2. Verify topic exists
3. Review circuit breaker status: `curl /metrics`
4. Check network: `ping <broker>`

### Memory issues

1. Monitor memory: `free -h`
2. Check process: `ps aux | grep collector`
3. Review configuration (BATCH_SIZE, MAX_POLL_RECORDS)
4. Consider horizontal scaling

### Connection timeouts

1. Verify firewall: `ufw status`
2. Check hostname resolution: `nslookup <broker>`
3. Test connectivity: `nc -zv <broker> 9092`
4. Review logs for detailed errors

## Scaling Strategies

### Horizontal Scaling

**Kubernetes**:
```bash
# Increase replicas
kubectl scale deployment telemetry-collector --replicas=10

# Or use auto-scaling
kubectl autoscale deployment telemetry-collector --min=3 --max=10
```

**Machines**:
```bash
# Deploy to more hosts in inventory
ansible-playbook deployment/ansible/deploy.yml \
  -i expanded-inventory.ini
```

### Vertical Scaling

**Kubernetes**:
```yaml
resources:
  requests:
    cpu: 1000m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 1Gi
```

**Machine**: Adjust `READ_FREQUENCY_MS` and `BATCH_SIZE`

## Rollback Procedures

### Kubernetes

```bash
# Rollback to previous version
helm rollback telemetry-collector 1

# Or revert to specific version
helm history telemetry-collector
helm rollback telemetry-collector <REVISION>
```

### Machine (Ansible)

```bash
# Deploy previous version
ansible-playbook deployment/ansible/deploy.yml \
  -i inventory.ini \
  -e collector_version=0.9.0
```

### Docker

```bash
# Switch to previous image
docker stop telemetry-collector
docker rm telemetry-collector
docker run -d --name telemetry-collector telemetry-collector:previous-tag
```

---

**Version**: 1.0.0
**Last Updated**: 2024
