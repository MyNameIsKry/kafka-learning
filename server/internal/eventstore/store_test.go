package eventstore

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/segmentio/kafka-go"
)

func TestImmutableIdentity(t *testing.T) {
	if Identity("raw", "a") != Identity("raw", "a") || Identity("raw", "a") == Identity("scored", "a") {
		t.Fatal("unstable or colliding identities")
	}
	a := Envelope{EventID: "id", Topic: "scored", Key: "x", Payload: json.RawMessage(`{"anomalyScore":0.9,"processedAt":"a"}`)}
	b := a
	b.Payload = json.RawMessage(`{"anomalyScore":0.9,"processedAt":"b"}`)
	if !Equivalent(a, b) {
		t.Fatal("retry timestamp must not change identity")
	}
	b.Payload = json.RawMessage(`{"anomalyScore":0.1,"processedAt":"b"}`)
	if Equivalent(a, b) {
		t.Fatal("conflicting predictions must fail")
	}
}

// Uses a unique event identity; never deletes existing user data.
func TestKurrentAppendRetryConflict(t *testing.T) {
	connection := os.Getenv("KURRENTDB_TEST_CONNECTION")
	if connection == "" {
		t.Skip("set KURRENTDB_TEST_CONNECTION for integration test")
	}
	w, err := NewWriter(connection, "test.raw")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.StreamPrefix = "test-financial-event-"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := uuid.NewString()
	message := kafka.Message{Key: []byte("C1"), Value: []byte(`{"transactionId":"` + id + `","amount":10}`)}
	for i := 0; i < 2; i++ {
		if err := w.WriteMessages(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	message.Value = []byte(`{"transactionId":"` + id + `","amount":20}`)
	if err := w.WriteMessages(ctx, message); err == nil {
		t.Fatal("expected immutable conflict")
	}
	reader, err := w.Client.ReadStream(ctx, w.StreamPrefix+Identity(w.Topic, id).String(), kurrentdb.ReadStreamOptions{From: kurrentdb.Start{}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	event, err := reader.Recv()
	if err != nil {
		t.Fatal(err)
	}
	var stored Envelope
	if err := json.Unmarshal(event.OriginalEvent().Data, &stored); err != nil {
		t.Fatal(err)
	}
	if string(stored.Payload) != `{"transactionId":"`+id+`","amount":10}` {
		t.Fatal("stored event overwritten")
	}
	if _, err := reader.Recv(); err == nil {
		t.Fatal("duplicate event appended")
	}
}
