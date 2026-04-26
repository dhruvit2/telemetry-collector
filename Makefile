.PHONY: help build build-local clean test fmt lint docker-build docker-run docker-stop docker-push helm-package helm-lint helm-install helm-upgrade ansible-deploy run-local run-collector

# Variables
PROJECT_NAME := telemetry-collector
DOCKER_REGISTRY := docker.io
DOCKER_IMAGE := $(DOCKER_REGISTRY)/$(PROJECT_NAME)
DOCKER_TAG := latest
SERVICE_NAME := collector
GO := go
HELM_CHART_PATH := ./deployment/helm/telemetry-collector
ANSIBLE_PLAYBOOK := ./deployment/ansible/deploy.yml

# Local run configuration
BROKER_ADDRESSES ?= localhost:9091
TOPIC ?= telemetry-data
TSDB_URL ?= http://localhost:8086
TSDB_TOKEN ?= 7h2UjNBHN7ApaRrwz49uyRi6sySH-NaICaNLz4ZP5ROt2Jf8lDfJqtyU_e-45STGcnvD71x5sa9dRlgb9H2kKg==
TSDB_ORG ?= telemetry
TSDB_BUCKET ?= gpu_metrics_raw
READ_FREQUENCY_MS ?= 1000

# Help target
help:
	@echo "$(PROJECT_NAME) - Telemetry Collector Service"
	@echo ""
	@echo "Build targets:"
	@echo "  make build              - Build binary for current OS"
	@echo "  make build-local        - Build Linux binary locally"
	@echo "  make clean              - Remove build artifacts"
	@echo ""
	@echo "Local Development:"
	@echo "  make run-local          - Build and run collector locally"
	@echo "  make run-collector      - Run collector (must build first)"
	@echo ""
	@echo "Quality targets:"
	@echo "  make test               - Run unit tests with coverage"
	@echo "  make fmt                - Format code"
	@echo "  make lint               - Run linter checks"
	@echo "  make mod-tidy           - Tidy Go modules"
	@echo ""
	@echo "Docker targets:"
	@echo "  make docker-build       - Build Docker image"
	@echo "  make docker-run         - Run Docker container locally"
	@echo "  make docker-stop        - Stop running Docker container"
	@echo "  make docker-push        - Push Docker image to registry"
	@echo ""
	@echo "Helm targets:"
	@echo "  make helm-package       - Package Helm chart"
	@echo "  make helm-lint          - Lint Helm chart"
	@echo "  make helm-install       - Install Helm chart to cluster"
	@echo "  make helm-upgrade       - Upgrade Helm chart in cluster"
	@echo ""
	@echo "Ansible targets:"
	@echo "  make ansible-deploy     - Deploy using Ansible playbook"
	@echo ""

# Build targets
build:
	@echo "Building $(PROJECT_NAME) for current OS..."
	$(GO) build -o ./bin/$(SERVICE_NAME) ./cmd/collector

build-local:
	@echo "Building $(PROJECT_NAME) Linux binary..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build \
		-ldflags="-w -s" \
		-o ./bin/$(SERVICE_NAME) \
		./cmd/collector

clean:
	@echo "Cleaning build artifacts..."
	rm -rf ./bin
	$(GO) clean

# Quality targets
test:
	@echo "Running unit tests with coverage..."
	$(GO) test -v -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...
	goimports -w .

lint:
	@echo "Running linter..."
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	}
	golangci-lint run ./...

mod-tidy:
	@echo "Tidying Go modules..."
	$(GO) mod tidy
	$(GO) mod download

# Docker targets
docker-build: build-local
	@echo "Building Docker image: $(DOCKER_IMAGE):$(DOCKER_TAG)"
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) -f Dockerfile .
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(DOCKER_IMAGE):latest
	@echo "Docker image built successfully"

docker-run: docker-build
	@echo "Running Docker container in tsdb-network..."
	docker run -d \
		--name telemetry-collector \
		--network=tsdb-network \
		-p 8080:8080 \
		-e BROKER_ADDRESSES="messagebroker:9092" \
		-e TOPIC="$(TOPIC)" \
		-e TSDB_URL="http://influxdb-tsdb:8086" \
		-e TSDB_TOKEN="$(TSDB_TOKEN)" \
		-e TSDB_ORG="$(TSDB_ORG)" \
		-e TSDB_BUCKET="$(TSDB_BUCKET)" \
		-e READ_FREQUENCY_MS="$(READ_FREQUENCY_MS)" \
		-e LOG_LEVEL="info" \
		$(DOCKER_IMAGE):$(DOCKER_TAG)
	@echo "✓ Container started: telemetry-collector"
	@echo "  Network: tsdb-network"
	@echo "  Health: http://localhost:8080/health"

docker-stop:
	@echo "Stopping Docker container..."
	docker stop telemetry-collector || true
	docker rm telemetry-collector || true
	@echo "✓ Container stopped"

docker-push: docker-build
	@echo "Pushing Docker image to registry..."
	@read -p "Enter Docker registry username: " REGISTRY_USER; \
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $$REGISTRY_USER/$(PROJECT_NAME):$(DOCKER_TAG); \
	docker push $$REGISTRY_USER/$(PROJECT_NAME):$(DOCKER_TAG)

# Helm targets
helm-package:
	@echo "Packaging Helm chart..."
	@if [ ! -d "$(HELM_CHART_PATH)" ]; then \
		echo "Error: Helm chart not found at $(HELM_CHART_PATH)"; \
		exit 1; \
	fi
	helm package $(HELM_CHART_PATH) -d ./helm-packages
	@echo "Helm chart packaged to ./helm-packages/"

helm-lint:
	@echo "Linting Helm chart..."
	@if [ ! -d "$(HELM_CHART_PATH)" ]; then \
		echo "Error: Helm chart not found at $(HELM_CHART_PATH)"; \
		exit 1; \
	fi
	helm lint $(HELM_CHART_PATH)

helm-install: helm-lint
	@echo "Installing Helm chart to cluster..."
	@read -p "Enter Kubernetes namespace (default: default): " KUBE_NS; \
	KUBE_NS=$${KUBE_NS:-default}; \
	helm install $(PROJECT_NAME) $(HELM_CHART_PATH) \
		--namespace $$KUBE_NS \
		--create-namespace \
		--values $(HELM_CHART_PATH)/values.yaml
	@echo "Helm chart installed"

helm-upgrade:
	@echo "Upgrading Helm chart in cluster..."
	@read -p "Enter Kubernetes namespace (default: default): " KUBE_NS; \
	KUBE_NS=$${KUBE_NS:-default}; \
	helm upgrade $(PROJECT_NAME) $(HELM_CHART_PATH) \
		--namespace $$KUBE_NS \
		--values $(HELM_CHART_PATH)/values.yaml
	@echo "Helm chart upgraded"

# Local run targets (for development and testing)
run-local: build-local ## Build and run collector locally
	@echo "Starting telemetry-collector service..."
	@echo "Broker: $(BROKER_ADDRESSES)"
	@echo "Topic: $(TOPIC)"
	@echo "TSDB: $(TSDB_URL) ($(TSDB_ORG)/$(TSDB_BUCKET))"
	@BROKER_ADDRESSES=$(BROKER_ADDRESSES) TOPIC=$(TOPIC) TSDB_URL=$(TSDB_URL) TSDB_TOKEN=$(TSDB_TOKEN) TSDB_ORG=$(TSDB_ORG) TSDB_BUCKET=$(TSDB_BUCKET) READ_FREQUENCY_MS=$(READ_FREQUENCY_MS) ./bin/$(SERVICE_NAME)

run-collector: ## Run collector (must build first)
	@BROKER_ADDRESSES=$(BROKER_ADDRESSES) TOPIC=$(TOPIC) TSDB_URL=$(TSDB_URL) TSDB_TOKEN=$(TSDB_TOKEN) TSDB_ORG=$(TSDB_ORG) TSDB_BUCKET=$(TSDB_BUCKET) READ_FREQUENCY_MS=$(READ_FREQUENCY_MS) ./bin/$(SERVICE_NAME)

# Ansible targets
ansible-deploy:
	@echo "Deploying with Ansible..."
	@if [ ! -f "$(ANSIBLE_PLAYBOOK)" ]; then \
		echo "Error: Ansible playbook not found at $(ANSIBLE_PLAYBOOK)"; \
		exit 1; \
	fi
	@read -p "Enter target hosts/inventory: " INVENTORY; \
	ansible-playbook $(ANSIBLE_PLAYBOOK) -i $$INVENTORY
	@echo "Ansible deployment completed"
