package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"telemetry-collector/pkg/collector"
	"telemetry-collector/pkg/config"
	"telemetry-collector/pkg/consumer"
	"telemetry-collector/pkg/tsdb"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	logger := setupLogger(cfg.LogLevel)
	defer logger.Sync()

	logger.Info("Starting telemetry collector service",
		zap.String("service_name", cfg.ServiceName),
		zap.String("service_id", cfg.ServiceID),
		zap.Int("read_frequency_ms", cfg.ReadFrequencyMs),
		zap.Int("broker_count", len(cfg.BrokerAddresses)),
		zap.String("topic", cfg.Topic),
		zap.Int("batch_size", cfg.BatchSize),
		zap.Int("max_retries", cfg.MaxRetries),
		zap.Bool("tsdb_enabled", cfg.TSDBEnabled))

	// Create collector
	telemetryCollector, err := collector.NewTelemetryCollector(
		cfg.BrokerAddresses,
		cfg.Topic,
		cfg.ReadFrequencyMs,
		cfg.BatchSize,
		cfg.BatchTimeoutMs,
		cfg.MaxRetries,
		logger,
	)
	if err != nil {
		logger.Fatal("failed to create collector", zap.Error(err))
	}

	// Setup TSDB writer if enabled
	if cfg.TSDBEnabled {
		tsdbConfig := &tsdb.TSDBConfig{
			URL:        cfg.TSDBURL,
			Token:      cfg.TSDBToken,
			Org:        cfg.TSDBOrg,
			Bucket:     cfg.TSDBBucket,
			MaxRetries: cfg.MaxRetries,
		}
		tsdbWriter, err := tsdb.NewTSDBWriter(tsdbConfig, logger)
		if err != nil {
			logger.Fatal("failed to create TSDB writer", zap.Error(err))
		}
		telemetryCollector.SetTSDBWriter(tsdbWriter)
		defer tsdbWriter.Close()
		logger.Info("TSDB writer initialized", zap.String("url", cfg.TSDBURL), zap.String("bucket", cfg.TSDBBucket))
	}

	// Register default telemetry sources
	registerDefaultSources(telemetryCollector, logger, cfg.BrokerAddresses, cfg.Topic)

	// Start collector
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	telemetryCollector.Start(ctx)

	// Start health check endpoint
	go startHealthCheckServer(cfg.HealthPort, logger, telemetryCollector)

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("telemetry collector service running")

	// Wait for shutdown signal
	<-sigChan
	logger.Info("received shutdown signal")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.GracefulShutdownTimeout)
	defer shutdownCancel()

	// Close collector
	if err := telemetryCollector.Close(); err != nil {
		logger.Error("error closing collector", zap.Error(err))
	}

	// Final metrics
	metrics := telemetryCollector.GetMetrics()
	logger.Info("telemetry collector stopped",
		zap.Int64("total_messages_collected", metrics.MessagesCollected),
		zap.Int64("total_messages_sent", metrics.MessagesSent),
		zap.Int64("total_errors", metrics.CollectionErrors),
		zap.Int64("total_batches_sent", metrics.BatchesSent))

	cancel()
	select {
	case <-shutdownCtx.Done():
		logger.Warn("shutdown timeout exceeded")
	default:
	}
}

// registerDefaultSources registers default telemetry collection sources
func registerDefaultSources(tc *collector.TelemetryCollector, logger *zap.Logger, brokerAddresses []string, topic string) {
	// MessageBroker consumer - reads telemetry data from messagebroker
	// Use first broker address, or default if empty
	brokerAddr := "messagebroker:9091"
	if len(brokerAddresses) > 0 {
		brokerAddr = brokerAddresses[0]
	}

	mbConsumer, err := consumer.NewMessageBrokerConsumer(
		brokerAddr,
		topic,
		"telemetry-collector-group",
		logger,
	)
	if err != nil {
		logger.Warn("failed to create messagebroker consumer", zap.Error(err))
	} else {
		tc.RegisterSource("messagebroker", func(ctx context.Context) (map[string]interface{}, error) {
			return mbConsumer.ConsumeMessage(ctx)
		})
		logger.Info("messagebroker consumer source registered", zap.String("broker", brokerAddr), zap.String("topic", topic))
	}

	// Example: System metrics collector
	tc.RegisterSource("system", func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339Nano),
			"type":      "system",
			"uptime":    time.Since(startTime).Seconds(),
			"metrics": map[string]interface{}{
				"healthy": true,
			},
		}, nil
	})

	// Example: Custom application metrics
	tc.RegisterSource("application", func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339Nano),
			"type":      "application",
			"status":    "running",
		}, nil
	})

	logger.Info("default telemetry sources registered")
}

var startTime = time.Now()

// setupLogger creates and configures logger
func setupLogger(level string) *zap.Logger {
	var logLevel zapcore.Level
	switch level {
	case "debug":
		logLevel = zapcore.DebugLevel
	case "info":
		logLevel = zapcore.InfoLevel
	case "warn":
		logLevel = zapcore.WarnLevel
	case "error":
		logLevel = zapcore.ErrorLevel
	default:
		logLevel = zapcore.InfoLevel
	}

	config := zap.Config{
		Level:       zap.NewAtomicLevelAt(logLevel),
		Development: false,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "timestamp",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			MessageKey:     "message",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.RFC3339NanoTimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	logger, err := config.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}

	return logger
}

// startHealthCheckServer starts the health check HTTP endpoint
func startHealthCheckServer(port int, logger *zap.Logger, tc *collector.TelemetryCollector) {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"status\": \"healthy\", \"timestamp\": \"%s\"}\n", time.Now().Format(time.RFC3339Nano))
	})

	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, "{\"ready\": true}\n")
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics := tc.GetMetrics()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"messages_collected":%d,"messages_sent":%d,"batches_sent":%d,"errors":%d}`+"\n",
			metrics.MessagesCollected, metrics.MessagesSent, metrics.BatchesSent, metrics.CollectionErrors)
	})

	addr := fmt.Sprintf(":%d", port)
	logger.Info("starting health check server", zap.String("address", addr))

	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("health check server error", zap.Error(err))
	}
}
