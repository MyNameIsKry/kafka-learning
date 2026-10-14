package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"server/internal/events"
	"server/internal/inference"
	"server/internal/kafkaio"
	pipelinemetrics "server/internal/metrics"
)

func main() {
	var (
		brokers      = flag.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
		rawTopic     = flag.String("topic", "transactions.raw", "raw input topic")
		groupID      = flag.String("group-id", "gatv2-inference-group", "Kafka consumer group")
		scoredTopic  = flag.String("scored-topic", "transactions.scored", "scored output topic")
		anomalyTopic = flag.String("anomalies-topic", "transactions.anomalies", "anomaly output topic")
		dlqTopic     = flag.String("dlq-topic", "transactions.dlq", "DLQ topic")
		maxRetries   = flag.Int("max-retries", 3, "processing retries per input")
		score        = flag.Float64("mock-score", 0.1, "mock anomaly score")
		threshold    = flag.Float64("mock-threshold", 0.8, "mock anomaly threshold")
		modelVersion = flag.String("model-version", "mock-v1", "model version")
		metricsPort  = flag.Int("metrics-port", 0, "Prometheus port; zero disables metrics")
		dedupeSize   = flag.Int("dedupe-cache-size", 100000, "bounded in-memory transaction dedupe size")
	)
	flag.Parse()
	if *maxRetries < 0 {
		log.Fatal("max-retries must be non-negative")
	}
	if *dedupeSize <= 0 {
		log.Fatal("dedupe-cache-size must be positive")
	}
	addresses := kafkaio.Brokers(*brokers)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: addresses, Topic: *rawTopic, GroupID: *groupID,
		StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10e6,
		MaxWait: 250 * time.Millisecond,
	})
	scoredWriter := kafkaio.NewWriter(addresses, *scoredTopic)
	anomalyWriter := kafkaio.NewWriter(addresses, *anomalyTopic)
	dlqWriter := kafkaio.NewWriter(addresses, *dlqTopic)
	defer reader.Close()
	defer scoredWriter.Close()
	defer anomalyWriter.Close()
	defer dlqWriter.Close()

	engine := inference.MockEngine{Score: *score, Threshold: *threshold, ModelVersion: *modelVersion}
	metrics, err := pipelinemetrics.New("inference", *metricsPort)
	if err != nil {
		log.Fatal(err)
	}
	defer metrics.Close(context.Background())
	seen := newDedupeCache(*dedupeSize)
	for {
		message, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Printf("fetch raw event: %v", err)
			time.Sleep(time.Second)
			continue
		}
		metrics.ConsumerLag(reader.Stats().Lag)
		if err := processWithRetry(context.Background(), reader, message, engine, scoredWriter, anomalyWriter, dlqWriter, *scoredTopic, *anomalyTopic, *maxRetries, seen, metrics); err != nil {
			log.Printf("inference stopped without committing offset %d: %v", message.Offset, err)
			return
		}
	}
}

type dedupeCache struct {
	maxSize int
	items   map[string]events.RawTransactionEvent
	order   []string
}

func newDedupeCache(maxSize int) *dedupeCache {
	return &dedupeCache{maxSize: maxSize, items: make(map[string]events.RawTransactionEvent)}
}

func (cache *dedupeCache) Get(id string) (events.RawTransactionEvent, bool) {
	value, ok := cache.items[id]
	return value, ok
}

func (cache *dedupeCache) Remember(event events.RawTransactionEvent) {
	if _, exists := cache.items[event.TransactionID]; exists {
		return
	}
	cache.items[event.TransactionID] = event
	cache.order = append(cache.order, event.TransactionID)
	if len(cache.order) > cache.maxSize {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.items, oldest)
	}
}

func processWithRetry(ctx context.Context, reader *kafka.Reader, message kafka.Message, engine inference.Engine, scoredWriter, anomalyWriter, dlqWriter *kafka.Writer, scoredTopic, anomalyTopic string, maxRetries int, seen *dedupeCache, metrics *pipelinemetrics.Metrics) error {
	for attempt := 0; ; attempt++ {
		started := time.Now()
		err := processMessage(ctx, message, engine, scoredWriter, anomalyWriter, dlqWriter, scoredTopic, anomalyTopic, seen, metrics)
		metrics.Batch(time.Since(started).Seconds())
		if err == nil {
			if err := reader.CommitMessages(ctx, message); err != nil {
				metrics.Failure("commit")
				return fmt.Errorf("commit offset %d: %w", message.Offset, err)
			}
			metrics.Committed(1)
			metrics.Completion(message.Time)
			return nil
		}
		metrics.Failure("batch")
		if attempt >= maxRetries {
			return fmt.Errorf("message offset %d failed after %d retries: %w", message.Offset, attempt, err)
		}
		delay := time.Duration(1<<min(attempt, 4)) * time.Second
		metrics.Retry()
		log.Printf("raw offset %d failed; retry %d/%d in %s: %v", message.Offset, attempt+1, maxRetries, delay, err)
		time.Sleep(delay)
	}
}

func processMessage(ctx context.Context, message kafka.Message, engine inference.Engine, scoredWriter, anomalyWriter, dlqWriter *kafka.Writer, scoredTopic, anomalyTopic string, seen *dedupeCache, metrics *pipelinemetrics.Metrics) error {
	raw, err := events.DecodeRaw(message.Value)
	if err != nil {
		return publishDLQ(ctx, dlqWriter, message, "INVALID_SCHEMA", err.Error())
	}
	if previous, ok := seen.Get(raw.TransactionID); ok {
		if previous != raw {
			return fmt.Errorf("conflicting duplicate transaction %s", raw.TransactionID)
		}
		return nil
	}
	started := time.Now()
	predictions, err := engine.Predict([]events.ModelInputDTO{raw.ModelInput()})
	metrics.Inference(time.Since(started).Seconds())
	if err != nil {
		return err
	}
	if len(predictions) != 1 || predictions[0].TransactionID != raw.TransactionID || predictions[0].Step != raw.Step {
		return errors.New("inference output does not map to input transaction")
	}
	prediction := predictions[0]
	payload, err := events.Encode(prediction)
	if err != nil {
		return err
	}
	if err := kafkaio.Publish(ctx, scoredWriter, prediction.TransactionID, payload); err != nil {
		return fmt.Errorf("publish scored: %w", err)
	}
	metrics.Published(scoredTopic)
	if prediction.IsAnomaly {
		if err := kafkaio.Publish(ctx, anomalyWriter, prediction.TransactionID, payload); err != nil {
			return fmt.Errorf("publish anomaly: %w", err)
		}
		metrics.Published(anomalyTopic)
	}
	seen.Remember(raw)
	return nil
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
