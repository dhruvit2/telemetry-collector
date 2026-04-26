package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	pb "github.com/dhruvit2/messagebroker/pkg/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MessageBrokerConsumer consumes messages from messagebroker
type MessageBrokerConsumer struct {
	brokerAddr string
	topic      string
	groupID    string
	logger     *zap.Logger
	client     pb.MessageBrokerClient
	conn       *grpc.ClientConn
	lock       sync.RWMutex
	offsets    map[int32]int64 // partition -> offset
}

// NewMessageBrokerConsumer creates a new messagebroker consumer
func NewMessageBrokerConsumer(brokerAddr, topic, groupID string, logger *zap.Logger) (*MessageBrokerConsumer, error) {
	// Dial the messagebroker
	conn, err := grpc.NewClient(
		brokerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10*1024*1024)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial messagebroker: %w", err)
	}

	client := pb.NewMessageBrokerClient(conn)

	consumer := &MessageBrokerConsumer{
		brokerAddr: brokerAddr,
		topic:      topic,
		groupID:    groupID,
		logger:     logger,
		client:     client,
		conn:       conn,
		offsets:    make(map[int32]int64),
	}

	// Initialize partition offsets
	if err := consumer.initializePartitions(context.Background()); err != nil {
		consumer.logger.Warn("failed to initialize partitions", zap.Error(err))
		// Don't fail, try again on first consume
	}

	return consumer, nil
}

// initializePartitions fetches partition metadata and initializes offsets
func (c *MessageBrokerConsumer) initializePartitions(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	metadata, err := c.client.GetTopicMetadata(ctx, &pb.GetTopicMetadataRequest{
		Topic: c.topic,
	})
	if err != nil {
		return fmt.Errorf("failed to get topic metadata: %w", err)
	}

	// Initialize offsets for each partition
	for _, partition := range metadata.Partitions {
		if _, exists := c.offsets[partition.Id]; !exists {
			c.offsets[partition.Id] = 0
		}
	}

	c.logger.Info("partitions initialized", zap.Int("num_partitions", len(metadata.Partitions)))
	return nil
}

// ConsumeMessage consumes a single message from any partition
// This is a simple round-robin approach that cycles through partitions
func (c *MessageBrokerConsumer) ConsumeMessage(ctx context.Context) (map[string]interface{}, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.offsets) == 0 {
		// Try to initialize partitions if not done yet
		if err := c.initializePartitions(ctx); err != nil {
			return nil, fmt.Errorf("no partitions available: %w", err)
		}
	}

	// Try each partition in order
	for partitionID, offset := range c.offsets {
		resp, err := c.client.ConsumeMessages(ctx, &pb.ConsumeRequest{
			Topic:       c.topic,
			Partition:   partitionID,
			Offset:      offset,
			MaxMessages: 1,
		})
		if err != nil {
			c.logger.Debug("failed to consume from partition",
				zap.Int32("partition", partitionID),
				zap.Error(err),
			)
			continue
		}

		if len(resp.Messages) == 0 {
			continue
		}

		// Update offset for this partition
		msg := resp.Messages[0]
		c.offsets[partitionID] = msg.Offset + 1

		// Parse message value
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			c.logger.Warn("failed to unmarshal message",
				zap.Int32("partition", partitionID),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			// Return raw data with metadata
			data = map[string]interface{}{
				"_raw": string(msg.Value),
			}
		}

		// Add source metadata
		data["_source_topic"] = c.topic
		data["_source_partition"] = partitionID
		data["_source_offset"] = msg.Offset

		return data, nil
	}

	// No messages found in any partition
	return nil, fmt.Errorf("no messages available in any partition")
}

// Close closes the connection to the messagebroker
func (c *MessageBrokerConsumer) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
