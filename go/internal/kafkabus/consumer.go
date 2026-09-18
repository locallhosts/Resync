package kafkabus

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

type Consumer struct {
	r *kafka.Reader
}

// NewConsumer joins consumer group groupID on topic. Multiple worker
// replicas started with the same groupID automatically split the
// topic's partitions between them — that's the horizontal scaling story
// for Week 6's "simulate 100s of concurrent alerts" load test: just run
// more replicas, Kafka handles the rebalancing.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
	}
}

// Message is the minimal shape callers need; keeps kafka.Message (and
// therefore the kafka-go import) out of every package that consumes
// commands.
type Message struct {
	Key   []byte
	Value []byte
}

// Fetch blocks until the next command is available and returns it.
// It uses ReadMessage (not FetchMessage), which commits the offset for
// group GroupID as part of the read. That gives at-least-once delivery:
// if the worker crashes after Fetch returns but before it finishes
// processing, the message is already committed and will NOT be
// redelivered — so Run (see internal/actions) and the event store's
// UNIQUE(case_id, seq) constraint together are what make recovery safe,
// not Kafka redelivery. The recovery/replay demo (Week 4) is driven by
// the event log, deliberately independent of Kafka delivery semantics.
func (c *Consumer) Fetch(ctx context.Context) (Message, error) {
	m, err := c.r.ReadMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("read message: %w", err)
	}
	return Message{Key: m.Key, Value: m.Value}, nil
}

func (c *Consumer) Close() error { return c.r.Close() }
