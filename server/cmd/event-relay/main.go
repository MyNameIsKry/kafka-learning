package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/segmentio/kafka-go"
	"server/internal/eventstore"
	"server/internal/kafkaio"
	"server/internal/metrics"
)

func main() {
	connection := flag.String("eventstore", eventstore.DefaultConnection, "KurrentDB connection string")
	brokers := flag.String("bootstrap-servers", "localhost:9092", "Kafka brokers")
	group := flag.String("group", "financial-kafka-relay", "persistent subscription; use a new group to replay history")
	prefix := flag.String("stream-prefix", eventstore.Prefix, "event stream namespace")
	metricsPort := flag.Int("metrics-port", 0, "Prometheus port; zero disables metrics")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	m, err := metrics.New("event_relay", *metricsPort)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close(context.Background())
	store, err := eventstore.NewWriter(*connection, "")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	settings := kurrentdb.SubscriptionSettingsDefault()
	settings.MaxSubscriberCount = 1
	settings.CheckpointLowerBound = 1
	settings.CheckpointUpperBound = 1
	err = store.Client.CreatePersistentSubscriptionToAll(ctx, *group, kurrentdb.PersistentAllSubscriptionOptions{
		StartFrom: kurrentdb.Start{}, Settings: &settings,
		Filter: &kurrentdb.SubscriptionFilter{Type: kurrentdb.StreamFilterType, Prefixes: []string{*prefix}},
	})
	if err != nil {
		// Only tolerate an existing subscription, not authentication/network errors.
		if _, checkErr := store.Client.GetPersistentSubscriptionInfoToAll(ctx, *group, kurrentdb.GetPersistentSubscriptionOptions{}); checkErr != nil {
			log.Fatal(err)
		}
	}
	writer := kafkaio.NewWriter(kafkaio.Brokers(*brokers), "")
	defer writer.Close()
	for ctx.Err() == nil {
		sub, err := store.Client.SubscribeToPersistentSubscriptionToAll(ctx, *group, kurrentdb.SubscribeToPersistentSubscriptionOptions{BufferSize: 1})
		if err != nil {
			log.Printf("subscribe: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for ctx.Err() == nil {
			received := sub.Recv()
			if received.SubscriptionDropped != nil {
				log.Printf("subscription dropped: %v", received.SubscriptionDropped)
				break
			}
			if received.EventAppeared == nil {
				continue
			}
			event := received.EventAppeared.Event
			var envelope eventstore.Envelope
			if err := json.Unmarshal(event.OriginalEvent().Data, &envelope); err != nil || envelope.SchemaVersion != 1 || envelope.EventType == "" || envelope.Topic == "" || envelope.EventID != event.OriginalEvent().EventID.String() {
				log.Printf("invalid stored envelope; stopping without ACK")
				sub.Close()
				return
			}
			// Retry in place; never ACK or advance past an undelivered event.
			for ctx.Err() == nil {
				err = writer.WriteMessages(ctx, kafka.Message{Topic: envelope.Topic, Key: []byte(envelope.Key), Value: envelope.Payload,
					Headers: []kafka.Header{{Key: "eventId", Value: []byte(envelope.EventID)}}, Time: time.Now().UTC()})
				if err == nil {
					m.Published(envelope.Topic)
					break
				}
				m.Failure("publish")
				m.Retry()
				log.Printf("Kafka publish failed: %v", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			if ctx.Err() != nil {
				break
			}
			if err := sub.Ack(event); err != nil {
				log.Printf("ACK failed: %v", err)
				break
			}
		}
		sub.Close()
	}
}
