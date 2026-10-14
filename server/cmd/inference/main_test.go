package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"server/internal/events"
	"server/internal/metrics"
)

type testEngine struct{}

func (testEngine) Predict(inputs []events.ModelInputDTO) ([]events.TransactionPredictionEvent, error) {
	result := make([]events.TransactionPredictionEvent, 0, len(inputs))
	for _, input := range inputs {
		result = append(result, events.TransactionPredictionEvent{
			SchemaVersion: events.PredictionSchemaVersion, TransactionID: input.TransactionID,
			Step: input.Step, AnomalyScore: 0.95, IsAnomaly: true, ModelVersion: "test-engine",
			ProcessedAt: time.Now().UTC(), ReasonCodes: []string{},
		})
	}
	return result, nil
}

type recordingStore struct {
	messages []kafka.Message
	err      error
}

func (s *recordingStore) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	if s.err != nil {
		return s.err
	}
	s.messages = append(s.messages, messages...)
	return nil
}

func TestInferenceAppendsOutputsBeforeRemembering(t *testing.T) {
	raw := events.RawTransactionEvent{SchemaVersion: 2, TransactionID: "x", Step: 1, Type: "PAYMENT", Amount: 1, NameOrig: "C1", NameDest: "M1"}
	payload, err := events.Encode(raw)
	if err != nil {
		t.Fatal(err)
	}
	message := kafka.Message{Topic: "raw", Value: payload}
	scored, anomaly, dlq := &recordingStore{}, &recordingStore{err: errors.New("store down")}, &recordingStore{}
	m, err := metrics.New("test", 0)
	if err != nil {
		t.Fatal(err)
	}
	cache := newDedupeCache(10)
	engine := testEngine{}
	err = processMessage(context.Background(), message, engine, scored, anomaly, dlq, "scored", "anomaly", cache, m)
	if err == nil {
		t.Fatal("store failure must propagate without commit")
	}
	if _, ok := cache.Get("x"); ok {
		t.Fatal("partial append cannot deduplicate away unfinished anomaly")
	}
	anomaly.err = nil
	if err := processMessage(context.Background(), message, engine, scored, anomaly, dlq, "scored", "anomaly", cache, m); err != nil {
		t.Fatal(err)
	}
	if len(scored.messages) != 2 || len(anomaly.messages) != 1 {
		t.Fatal("retry did not complete persisted outputs")
	}
	if _, ok := cache.Get("x"); !ok {
		t.Fatal("completed input not remembered")
	}
}

func TestInvalidInputAppendsDLQ(t *testing.T) {
	scored, anomaly, dlq := &recordingStore{}, &recordingStore{}, &recordingStore{}
	m, err := metrics.New("test", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := processMessage(context.Background(), kafka.Message{Topic: "raw", Value: []byte("invalid")}, testEngine{}, scored, anomaly, dlq, "scored", "anomaly", newDedupeCache(10), m); err != nil {
		t.Fatal(err)
	}
	if len(dlq.messages) != 1 || len(scored.messages) != 0 {
		t.Fatal("invalid input must be persisted only as DLQ")
	}
}
