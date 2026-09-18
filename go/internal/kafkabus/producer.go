// Package kafkabus is a thin wrapper around segmentio/kafka-go, kept
// deliberately small: the rest of the codebase depends on this package's
// Producer/Consumer types, not on kafka-go directly, so swapping brokers
// or client libraries later only touches this one package.
package kafkabus

import (
	"context"
	"encoding/json"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

type Producer struct {
	w *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // same key -> same partition -> per-case ordering
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish sends v (JSON-encoded) keyed by key. Using the case ID as the
// key everywhere is what guarantees a single case's commands and events
// land on the same partition and are therefore delivered in order — this
// matters a lot for an event-sourced system, since replay assumes order.
func (p *Producer) Publish(ctx context.Context, key string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: payload,
	})
}

func (p *Producer) Close() error { return p.w.Close() }
