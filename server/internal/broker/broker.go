package broker

import (
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func netJoin(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}

// EnsureTopic creates the topic with the wanted partition count.
// It is idempotent: an existing topic is left untouched.
func EnsureTopic(brokers []string, topic string, partitions int) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cconn, err := kafka.Dial("tcp", netJoin(controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer cconn.Close()

	return cconn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}

// NewWriter returns a producer keyed by productId.
// The hash balancer pins one product to one partition, keeping its
// orders in order and free of stock races.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		// Single orders must go out immediately; the default batching
		// would hold every message for the full BatchTimeout (1s).
		BatchSize: 1,
	}
}

// NewReader returns a consumer-group reader. ReadMessage commits offsets,
// so reopening the reader resumes from the last committed offset.
// clientID must be unique per process: two readers sharing one client ID
// livelock the group rebalance and nobody consumes.
func NewReader(brokers []string, topic, groupID, clientID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			ClientID:  clientID,
			KeepAlive: 30 * time.Second,
		},
	})
}
