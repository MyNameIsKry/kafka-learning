package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"server/internal/events"
	"server/internal/eventstore"
	"server/internal/inference"
)

type memoryProjection struct{ raw, prediction, alert int }

func (p *memoryProjection) UpsertRaw(context.Context, events.RawTransactionEvent) error {
	p.raw++
	return nil
}
func (p *memoryProjection) UpsertPrediction(context.Context, events.TransactionPredictionEvent) error {
	p.prediction++
	return nil
}
func (p *memoryProjection) UpsertAlert(context.Context, events.TransactionPredictionEvent) error {
	p.alert++
	return nil
}

type publisher struct {
	messages []kafka.Message
	err      error
}

func (p *publisher) WriteMessages(_ context.Context, m ...kafka.Message) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, m...)
	return nil
}

func TestDirectProjectionAndRelay(t *testing.T) {
	raw := events.RawTransactionEvent{SchemaVersion: 2, TransactionID: "t", Step: 1, Type: "PAYMENT", NameOrig: "C1", NameDest: "M1"}
	payload, _ := events.Encode(raw)
	envelope := eventstore.Envelope{EventType: "RawTransactionRecorded", Payload: payload, Topic: "transactions.raw", Key: "C1", EventID: "id"}
	store := &memoryProjection{}
	if err := Project(context.Background(), store, envelope); err != nil || store.raw != 1 {
		t.Fatal("direct projection failed", err)
	}
	out := &publisher{}
	if err := Relay(context.Background(), out, envelope); err != nil {
		t.Fatal(err)
	}
	if len(out.messages) != 1 || out.messages[0].Topic != envelope.Topic || string(out.messages[0].Key) != "C1" {
		t.Fatal("relay routing changed")
	}
}

func TestInferencePersistsBothOutputsAndProjects(t *testing.T) {
	raw := events.RawTransactionEvent{SchemaVersion: 2, TransactionID: "t", Step: 1, Type: "PAYMENT", NameOrig: "C1", NameDest: "M1", OldBalanceOrg: 100}
	payload, _ := events.Encode(raw)
	message := kafka.Message{Value: payload}
	scored, anomaly, dlq := &publisher{}, &publisher{err: errors.New("store down")}, &publisher{}
	engine := inference.MockEngine{Score: .95, Threshold: .8, ModelVersion: "test"}
	if err := Infer(context.Background(), engine, message, scored, anomaly, dlq); err == nil {
		t.Fatal("partial persistence must fail")
	}
	anomaly.err = nil
	if err := Infer(context.Background(), engine, message, scored, anomaly, dlq); err != nil {
		t.Fatal(err)
	}
	store := &memoryProjection{}
	for _, tc := range []struct {
		kind string
		data []byte
	}{{"TransactionScored", scored.messages[0].Value}, {"TransactionAnomalyDetected", anomaly.messages[0].Value}} {
		if err := Project(context.Background(), store, eventstore.Envelope{EventType: tc.kind, Payload: tc.data}); err != nil {
			t.Fatal(err)
		}
	}
	if store.prediction != 1 || store.alert != 1 {
		t.Fatal("output projections missing")
	}
}

func TestInferenceDLQStoreFirst(t *testing.T) {
	scored, anomaly, dlq := &publisher{}, &publisher{}, &publisher{}
	if err := Infer(context.Background(), inference.MockEngine{}, kafka.Message{Topic: "raw", Value: []byte(`{"isFraud":1}`)}, scored, anomaly, dlq); err != nil {
		t.Fatal(err)
	}
	if len(dlq.messages) != 1 || len(scored.messages) != 0 {
		t.Fatal("invalid schema must go only to event store DLQ writer")
	}
}
