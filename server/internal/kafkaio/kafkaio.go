package kafkaio

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func Brokers(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			result = append(result, broker)
		}
	}
	return result
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
	}
}

type Publisher interface {
	WriteMessages(context.Context, ...kafka.Message) error
}

func Publish(ctx context.Context, writer Publisher, key string, value []byte) error {
	return writer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: value, Time: time.Now().UTC()})
}
