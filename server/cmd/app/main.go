package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"server/internal/api"
	"server/internal/events"
	"server/internal/eventstore"
	"server/internal/inference"
	"server/internal/kafkaio"
	mongostore "server/internal/mongo"
	workers "server/internal/runtime"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := flag.String("addr", env("API_ADDR", ":8080"), "HTTP address")
	connection := flag.String("eventstore", env("KURRENTDB", eventstore.DefaultConnection), "KurrentDB URI")
	prefix := flag.String("stream-prefix", eventstore.Prefix, "event stream namespace")
	brokers := flag.String("bootstrap-servers", env("BOOTSTRAP_SERVERS", "localhost:9092"), "Kafka brokers")
	database := flag.String("mongo-db", env("MONGO_DATABASE", "financial_anomaly"), "Mongo database")
	uri := flag.String("mongo-uri", env("MONGO_URI", "mongodb://localhost:27017"), "Mongo URI")
	group := flag.String("group-prefix", "financial-app", "durable worker group namespace")
	topic := flag.String("topic-prefix", "transactions", "Kafka topic namespace")
	score := flag.Float64("mock-score", 0.1, "fixed demo score; not ML quality")
	threshold := flag.Float64("mock-threshold", 0.8, "explicit mock threshold")
	version := flag.String("model-version", "mock-v1", "model version")
	flag.Parse()
	if *prefix == "" || *group == "" || *topic == "" {
		return errors.New("namespaces must not be empty")
	}
	if math.IsNaN(*threshold) || math.IsInf(*threshold, 0) {
		return errors.New("mock threshold must be finite")
	}
	engine := inference.MockEngine{Score: *score, Threshold: *threshold, ModelVersion: *version}
	if _, err := engine.Predict([]events.ModelInputDTO{{TransactionID: "validation", Step: 1}}); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, stop := context.WithTimeout(ctx, 10*time.Second)
	store, err := mongostore.New(startup, *uri, *database)
	stop()
	if err != nil {
		return err
	}
	defer func() {
		c, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = store.Close(c)
	}()
	raw, err := eventstore.NewWriter(*connection, *topic+".raw")
	if err != nil {
		return err
	}
	defer raw.Close()
	raw.StreamPrefix = *prefix
	raw.EventType = "RawTransactionRecorded"
	// Writers share one client but persist different event types/topics.
	scored := &eventstore.Writer{Client: raw.Client, Topic: *topic + ".scored", StreamPrefix: *prefix, EventType: "TransactionScored"}
	anomaly := &eventstore.Writer{Client: raw.Client, Topic: *topic + ".anomalies", StreamPrefix: *prefix, EventType: "TransactionAnomalyDetected"}
	dlq := &eventstore.Writer{Client: raw.Client, Topic: *topic + ".dlq", StreamPrefix: *prefix, EventType: "InvalidEventRecorded"}
	writer := kafkaio.NewWriter(kafkaio.Brokers(*brokers), "")
	defer writer.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: kafkaio.Brokers(*brokers), Topic: raw.Topic, GroupID: *group + "-model",
		StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10e6, MaxWait: 250 * time.Millisecond})
	defer reader.Close()
	server := &http.Server{Addr: *addr, Handler: api.NewHandler(store, env("API_CORS_ORIGIN", "http://localhost:5173"), api.EventStoreAppender{Writer: raw}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	var wg sync.WaitGroup
	failures := make(chan error, 4)
	start := func(work func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := work(); err != nil && ctx.Err() == nil {
				failures <- err
				cancel()
			}
		}()
	}
	start(func() error {
		return workers.Subscribe(ctx, raw.Client, *group+"-projection", *prefix, func(c context.Context, e eventstore.Envelope) error { return workers.Project(c, store, e) })
	})
	start(func() error {
		return workers.Subscribe(ctx, raw.Client, *group+"-relay", *prefix, func(c context.Context, e eventstore.Envelope) error { return workers.Relay(c, writer, e) })
	})
	start(func() error { return workers.Consume(ctx, reader, engine, scored, anomaly, dlq) })
	start(func() error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})
	log.Printf("server %s: API + KurrentDB projection + Kafka relay + mock model (score=%g threshold=%g)", *addr, *score, *threshold)
	<-ctx.Done()
	shutdown, done := context.WithTimeout(context.Background(), 30*time.Second)
	err = server.Shutdown(shutdown)
	done()
	if err != nil {
		_ = server.Close()
	}
	wg.Wait()
	select {
	case err := <-failures:
		return err
	default:
		return err
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
