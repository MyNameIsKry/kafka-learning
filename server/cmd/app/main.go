package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"server/internal/api"
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
	modelPath := flag.String("model-path", env("MODEL_PATH", "gatv2-pipeline/artifacts/mock/anomaly_score.onnx"), "ONNX model path")
	threshold := flag.Float64("threshold", envFloat("ANOMALY_THRESHOLD", 0.8), "anomaly score threshold")
	version := flag.String("model-version", env("MODEL_VERSION", "mock-onnx-v1"), "model version")
	flag.Parse()
	if *prefix == "" || *group == "" || *topic == "" {
		return errors.New("namespaces must not be empty")
	}
	engine, err := inference.NewONNXEngine(*modelPath, *threshold, *version)
	if err != nil {
		return fmt.Errorf("initialize ONNX inference: %w", err)
	}
	defer engine.Close()
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
	log.Printf("server %s: API + KurrentDB projection + Kafka relay + ONNX model=%s threshold=%g version=%s", *addr, *modelPath, *threshold, *version)
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

func envFloat(key string, fallback float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
