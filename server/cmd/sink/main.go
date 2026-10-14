package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"server/internal/events"
	"server/internal/kafkaio"
	pipelinemetrics "server/internal/metrics"
	mongostore "server/internal/mongo"
)

type sinkConfig struct {
	topic string
	group string
}

var sinkDefaults = map[string]sinkConfig{
	"raw":        {topic: "transactions.raw", group: "mongodb-raw-sink-group"},
	"prediction": {topic: "transactions.scored", group: "mongodb-sink-group"},
	"alert":      {topic: "transactions.anomalies", group: "alert-group"},
}

func main() {
	var (
		kind        = flag.String("kind", "", "raw, prediction or alert")
		brokers     = flag.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
		topic       = flag.String("topic", "", "optional input topic override")
		group       = flag.String("group-id", "", "optional consumer group override")
		mongoURI    = flag.String("mongo-uri", "mongodb://localhost:27017", "MongoDB URI")
		mongoDB     = flag.String("mongo-db", "financial_anomaly", "MongoDB database")
		dlqTopic    = flag.String("dlq-topic", "transactions.dlq", "DLQ topic")
		maxRetries  = flag.Int("max-retries", 3, "processing retries per input")
		metricsPort = flag.Int("metrics-port", 0, "Prometheus port; zero disables metrics")
	)
	flag.Parse()
	defaults, ok := sinkDefaults[*kind]
	if !ok {
		log.Fatal("-kind must be raw, prediction or alert")
	}
	if *maxRetries < 0 {
		log.Fatal("max-retries must be non-negative")
	}
	if *topic != "" {
		defaults.topic = *topic
	}
	if *group != "" {
		defaults.group = *group
	}
	addresses := kafkaio.Brokers(*brokers)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: addresses, Topic: defaults.topic, GroupID: defaults.group,
		StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10e6,
		MaxWait: 250 * time.Millisecond,
	})
	dlqWriter := kafkaio.NewWriter(addresses, *dlqTopic)
	store, err := mongostore.New(context.Background(), *mongoURI, *mongoDB)
	if err != nil {
		log.Fatalf("connect MongoDB: %v", err)
	}
	metrics, err := pipelinemetrics.New("sink_"+*kind, *metricsPort)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
	defer dlqWriter.Close()
	defer store.Close(context.Background())
	defer metrics.Close(context.Background())

	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("fetch %s event: %v", *kind, err)
			time.Sleep(time.Second)
			continue
		}
		metrics.ConsumerLag(reader.Stats().Lag)
		if err := processWithRetry(context.Background(), reader, message, *kind, store, dlqWriter, *maxRetries, metrics); err != nil {
			log.Printf("%s sink stopped without committing offset %d: %v", *kind, message.Offset, err)
			return
		}
	}
}

func processWithRetry(ctx context.Context, reader *kafka.Reader, message kafka.Message, kind string, store *mongostore.Store, dlqWriter *kafka.Writer, maxRetries int, metrics *pipelinemetrics.Metrics) error {
	for attempt := 0; ; attempt++ {
		started := time.Now()
		err := processMessage(ctx, message, kind, store, dlqWriter, metrics)
		metrics.Batch(time.Since(started).Seconds())
		if err == nil {
			err := reader.CommitMessages(ctx, message)
			if err != nil {
				metrics.Failure("commit")
				return err
			}
			metrics.Committed(1)
			metrics.Completion(message.Time)
			return nil
		}
		metrics.Failure("batch")
		if attempt >= maxRetries {
			return fmt.Errorf("offset %d failed after %d retries: %w", message.Offset, attempt, err)
		}
		delay := time.Duration(1<<min(attempt, 4)) * time.Second
		metrics.Retry()
		log.Printf("%s offset %d failed; retry %d/%d in %s: %v", kind, message.Offset, attempt+1, maxRetries, delay, err)
		time.Sleep(delay)
	}
}

func processMessage(ctx context.Context, message kafka.Message, kind string, store *mongostore.Store, dlqWriter *kafka.Writer, metrics *pipelinemetrics.Metrics) error {
	switch kind {
	case "raw":
		event, err := events.DecodeRaw(message.Value)
		if err != nil {
			return publishDLQ(ctx, dlqWriter, message, "INVALID_SCHEMA", err.Error())
		}
		err = store.UpsertRaw(ctx, event)
		if err == nil {
			metrics.Stored()
		}
		return err
	case "prediction", "alert":
		event, err := events.DecodePrediction(message.Value)
		if err != nil {
			return publishDLQ(ctx, dlqWriter, message, "INVALID_SCHEMA", err.Error())
		}
		if kind == "alert" && !event.IsAnomaly {
			return publishDLQ(ctx, dlqWriter, message, "INVALID_SCHEMA", "anomaly topic event must have isAnomaly=true")
		}
		if kind == "prediction" {
			err := store.UpsertPrediction(ctx, event)
			if err == nil {
				metrics.Stored()
			}
			return err
		}
		err = store.UpsertAlert(ctx, event)
		if err == nil {
			metrics.Stored()
		}
		return err
	default:
		return fmt.Errorf("unsupported sink kind %s", kind)
	}
}

func publishDLQ(ctx context.Context, writer *kafka.Writer, message kafka.Message, code, detail string) error {
	event := events.DLQEvent{
		SourceTopic: message.Topic, SourcePartition: message.Partition, SourceOffset: message.Offset,
		ErrorCode: code, ErrorMessage: detail, FailedAt: time.Now().UTC(),
		OriginalPayload: events.DecodeDLQPayload(message.Value),
	}
	payload, err := events.Encode(event)
	if err != nil {
		return err
	}
	return kafkaio.Publish(ctx, writer, fmt.Sprintf("%s-%d-%d", message.Topic, message.Partition, message.Offset), payload)
}

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
