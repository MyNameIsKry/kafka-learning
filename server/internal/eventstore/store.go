// Package eventstore persists events before a separate relay delivers them to Kafka.
package eventstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/segmentio/kafka-go"
)

const DefaultConnection = "kurrentdb://localhost:2113?tls=false"
const Prefix = "financial-anomaly-event-"

var ErrConflict = errors.New("conflicting immutable event")

type Envelope struct {
	SchemaVersion int             `json:"schemaVersion"`
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Topic         string          `json:"topic"`
	Key           string          `json:"key"`
	Payload       json.RawMessage `json:"payload"`
}

type Writer struct {
	Client       *kurrentdb.Client
	Topic        string
	StreamPrefix string
	EventType    string
}

func NewWriter(connection, topic string) (*Writer, error) {
	config, err := kurrentdb.ParseConnectionString(connection)
	if err != nil {
		return nil, err
	}
	client, err := kurrentdb.NewClient(config)
	if err != nil {
		return nil, err
	}
	return &Writer{Client: client, Topic: topic, StreamPrefix: Prefix, EventType: "FinancialEvent"}, nil
}

func (w *Writer) Close() error { return w.Client.Close() }

func Identity(topic, key string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(topic+"\x00"+key))
}

// Each logical event gets a single-event stream. NoStream prevents conflicting
// writes; retries read and compare the existing immutable record.
func (w *Writer) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	if w.Topic == "" || w.StreamPrefix == "" {
		return fmt.Errorf("event topic and stream prefix must not be empty")
	}
	for _, message := range messages {
		key := string(message.Key)
		var payload map[string]any
		if err := json.Unmarshal(message.Value, &payload); err != nil {
			return err
		}
		if payload == nil {
			return fmt.Errorf("event payload must be a JSON object")
		}
		// Raw Kafka keys are account IDs, not event identities.
		if id, ok := payload["transactionId"].(string); ok {
			key = id
			if version, ok := payload["modelVersion"].(string); ok {
				key += "\x00" + version
			}
		}
		id := Identity(w.Topic, key)
		stream := w.StreamPrefix + id.String()
		envelope := Envelope{SchemaVersion: 1, EventID: id.String(), EventType: w.EventType, OccurredAt: time.Now().UTC(), Topic: w.Topic, Key: string(message.Key), Payload: message.Value}
		data, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		operation, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = w.Client.AppendToStream(operation, stream, kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}}, kurrentdb.EventData{EventID: id, EventType: w.EventType, ContentType: kurrentdb.ContentTypeJson, Data: data})
		cancel()
		// Verify even successful appends: the server may deduplicate by EventID
		// without comparing payload bytes. Also resolves ambiguous append timeouts.
		operation, cancel = context.WithTimeout(ctx, 10*time.Second)
		reader, readErr := w.Client.ReadStream(operation, stream, kurrentdb.ReadStreamOptions{From: kurrentdb.Start{}}, 1)
		if readErr != nil {
			cancel()
			if err != nil {
				return fmt.Errorf("append event: %w", err)
			}
			return fmt.Errorf("verify append: %w", readErr)
		}
		existing, readErr := reader.Recv()
		reader.Close()
		cancel()
		if readErr != nil {
			return fmt.Errorf("read existing event: %w", readErr)
		}
		var previous Envelope
		if json.Unmarshal(existing.OriginalEvent().Data, &previous) != nil || !Equivalent(previous, envelope) {
			return fmt.Errorf("%w %s", ErrConflict, id)
		}
	}
	return nil
}

func Equivalent(a, b Envelope) bool {
	if a.SchemaVersion != b.SchemaVersion || a.EventType != b.EventType || a.EventID != b.EventID || a.Topic != b.Topic || a.Key != b.Key {
		return false
	}
	decode := func(data []byte) (map[string]any, error) {
		var value map[string]any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		err := decoder.Decode(&value)
		// Retry wall-clock metadata is not the logical event's identity.
		delete(value, "processedAt")
		delete(value, "failedAt")
		return value, err
	}
	x, err := decode(a.Payload)
	if err != nil {
		return false
	}
	y, err := decode(b.Payload)
	return err == nil && reflect.DeepEqual(x, y)
}
