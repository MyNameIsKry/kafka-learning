// Package runtime runs the demo's independent background workers in one process.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/segmentio/kafka-go"
	"server/internal/events"
	"server/internal/eventstore"
	"server/internal/inference"
	"server/internal/kafkaio"
)

type Projection interface {
	UpsertRaw(context.Context, events.RawTransactionEvent) error
	UpsertPrediction(context.Context, events.TransactionPredictionEvent) error
	UpsertAlert(context.Context, events.TransactionPredictionEvent) error
}

// Subscribe uses separate durable subscriptions for projection and relay.
// Neither branch waits for the other. ACK follows its own successful side effect.
func Subscribe(ctx context.Context, client *kurrentdb.Client, group, prefix string, handle func(context.Context, eventstore.Envelope) error) error {
	settings := kurrentdb.SubscriptionSettingsDefault()
	settings.MaxSubscriberCount = 1
	settings.CheckpointLowerBound = 1
	settings.CheckpointUpperBound = 1
	err := client.CreatePersistentSubscriptionToAll(ctx, group, kurrentdb.PersistentAllSubscriptionOptions{
		StartFrom: kurrentdb.Start{}, Settings: &settings,
		Filter: &kurrentdb.SubscriptionFilter{Type: kurrentdb.StreamFilterType, Prefixes: []string{prefix}},
	})
	if err != nil {
		if _, check := client.GetPersistentSubscriptionInfoToAll(ctx, group, kurrentdb.GetPersistentSubscriptionOptions{}); check != nil {
			return err
		}
	}
	for ctx.Err() == nil {
		sub, err := client.SubscribeToPersistentSubscriptionToAll(ctx, group, kurrentdb.SubscribeToPersistentSubscriptionOptions{BufferSize: 1})
		if err != nil {
			log.Printf("%s subscribe: %v", group, err)
			pause(ctx)
			continue
		}
		for ctx.Err() == nil {
			received := sub.Recv()
			if received.SubscriptionDropped != nil {
				break
			}
			if received.EventAppeared == nil {
				continue
			}
			event := received.EventAppeared.Event
			var envelope eventstore.Envelope
			if err := json.Unmarshal(event.OriginalEvent().Data, &envelope); err != nil || envelope.SchemaVersion != 1 || envelope.EventID != event.OriginalEvent().EventID.String() || envelope.Topic == "" {
				sub.Close()
				return fmt.Errorf("%s: invalid immutable envelope; not ACKed", group)
			}
			for ctx.Err() == nil {
				if err = handle(ctx, envelope); err == nil {
					break
				}
				log.Printf("%s event %s failed; retry without ACK: %v", group, envelope.EventID, err)
				pause(ctx)
			}
			if ctx.Err() != nil {
				break
			}
			if err := sub.Ack(event); err != nil {
				break
			}
		}
		sub.Close()
		pause(ctx)
	}
	return ctx.Err()
}

func Project(ctx context.Context, store Projection, envelope eventstore.Envelope) error {
	switch envelope.EventType {
	case "RawTransactionRecorded":
		event, err := events.DecodeRaw(envelope.Payload)
		if err != nil {
			return err
		}
		return store.UpsertRaw(ctx, event)
	case "TransactionScored":
		event, err := events.DecodePrediction(envelope.Payload)
		if err != nil {
			return err
		}
		return store.UpsertPrediction(ctx, event)
	case "TransactionAnomalyDetected":
		event, err := events.DecodePrediction(envelope.Payload)
		if err != nil {
			return err
		}
		if !event.IsAnomaly {
			return fmt.Errorf("anomaly event has isAnomaly=false")
		}
		return store.UpsertAlert(ctx, event)
	case "InvalidEventRecorded":
		return nil // Stored in KurrentDB; no DLQ dashboard projection yet.
	default:
		return fmt.Errorf("unknown event type %q", envelope.EventType)
	}
}

func Relay(ctx context.Context, writer kafkaio.Publisher, envelope eventstore.Envelope) error {
	return writer.WriteMessages(ctx, kafka.Message{Topic: envelope.Topic, Key: []byte(envelope.Key), Value: envelope.Payload,
		Time: envelope.OccurredAt, Headers: []kafka.Header{{Key: "eventId", Value: []byte(envelope.EventID)}}})
}

func Infer(ctx context.Context, engine inference.Engine, message kafka.Message, scored, anomaly, dlq kafkaio.Publisher) error {
	raw, err := events.DecodeRaw(message.Value)
	if err != nil {
		event := events.DLQEvent{SourceTopic: message.Topic, SourcePartition: message.Partition, SourceOffset: message.Offset,
			ErrorCode: "INVALID_SCHEMA", ErrorMessage: err.Error(), FailedAt: time.Now().UTC(), OriginalPayload: events.DecodeDLQPayload(message.Value)}
		payload, err := events.Encode(event)
		if err != nil {
			return err
		}
		return kafkaio.Publish(ctx, dlq, fmt.Sprintf("%s-%d-%d", message.Topic, message.Partition, message.Offset), payload)
	}
	predictions, err := engine.Predict([]events.ModelInputDTO{raw.ModelInput()})
	if err != nil {
		return err
	}
	if len(predictions) != 1 || predictions[0].TransactionID != raw.TransactionID || predictions[0].Step != raw.Step {
		return fmt.Errorf("prediction does not map to input")
	}
	prediction := predictions[0]
	if err := prediction.Validate(); err != nil {
		return err
	}
	payload, err := events.Encode(prediction)
	if err != nil {
		return err
	}
	if err := kafkaio.Publish(ctx, scored, prediction.TransactionID, payload); err != nil {
		return err
	}
	if prediction.IsAnomaly {
		return kafkaio.Publish(ctx, anomaly, prediction.TransactionID, payload)
	}
	return nil
}

func Consume(ctx context.Context, reader *kafka.Reader, engine inference.Engine, scored, anomaly, dlq kafkaio.Publisher) error {
	for ctx.Err() == nil {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("model fetch: %v", err)
			pause(ctx)
			continue
		}
		// Do not fetch the next input until output persistence AND offset commit succeed.
		for ctx.Err() == nil {
			if err = Infer(ctx, engine, message, scored, anomaly, dlq); err == nil {
				break
			}
			log.Printf("model offset %d: %v", message.Offset, err)
			pause(ctx)
		}
		for ctx.Err() == nil {
			if err = reader.CommitMessages(ctx, message); err == nil {
				break
			}
			log.Printf("model commit: %v", err)
			pause(ctx)
		}
	}
	return ctx.Err()
}

func pause(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
}
