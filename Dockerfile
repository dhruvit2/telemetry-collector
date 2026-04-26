# Multi-stage build for telemetry-collector service
FROM golang:1.26-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Copy go.mod and go.sum
COPY go.mod go.sum* ./

# Download dependencies first
RUN go mod download

# Copy entire project
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /build/collector \
    ./cmd/collector

# Runtime stage
FROM alpine:3.18

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata curl

# Create non-root user for security
RUN addgroup -g 1000 telemetry && \
    adduser -D -u 1000 -G telemetry telemetry

# Copy binary from builder
COPY --from=builder /build/collector /app/collector

# Set ownership
RUN chown -R telemetry:telemetry /app

# Switch to non-root user
USER telemetry

# Default environment variables
ENV SERVICE_NAME="telemetry-collector" \
    HEALTH_PORT=8080 \
    LOG_LEVEL=info \
    READ_FREQUENCY_MS=1000 \
    BATCH_SIZE=50 \
    MAX_RETRIES=5 \
    RETRY_BACKOFF_MS=100 \
    CIRCUIT_BREAKER_THRESHOLD=10 \
    HEALTH_CHECK_INTERVAL=30 \
    GRACEFUL_SHUTDOWN_TIMEOUT=30

# Expose health check port
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${HEALTH_PORT}/health || exit 1

# Run the collector
CMD ["/app/collector"]
