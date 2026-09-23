package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"server/internal/events"
	mongostore "server/internal/mongo"
	"server/internal/service"
)

// mongoStock adapts the shared Mongo stock to service.StockStore.
// Reset maps to ResetStock; every instance sees the same numbers.
type mongoStock struct{ *mongostore.Client }

func (m mongoStock) Reset() { _ = m.ResetStock() }

func (m mongoStock) Decrease(productID string, qty int) (int, error) {
	return m.DecreaseStock(productID, qty)
}

func main() {
	mc, err := mongostore.Connect(
		service.Getenv("MONGO_URI", "mongodb://localhost:27017"),
		service.Getenv("MONGO_DB", "kafka-demo"),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := mc.EnsureStock(); err != nil {
		log.Fatal(err)
	}
	business := func(o events.Order) (string, error) {
		left, err := mc.DecreaseStock(o.ProductID, o.Qty)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Đã trừ %d × %s, còn %d", o.Qty, o.ProductID, left), nil
	}
	cfg := service.Config{
		Name:         "inventory",
		Instance:     service.Getenv("INSTANCE", "inventory-1"),
		Addr:         service.Getenv("ADDR", ":8081"),
		GroupID:      events.GroupInventory,
		Brokers:      strings.Split(service.Getenv("KAFKA_BROKERS", "localhost:9092"), ","),
		GatewayURL:   service.Getenv("GATEWAY_URL", "http://localhost:8080"),
		WorkMS:       service.GetenvInt("WORK_MS", 200),
		Capacity:     service.GetenvInt("CAPACITY", 5),
		QueueTimeout: time.Duration(service.GetenvInt("QUEUE_TIMEOUT_MS", 2000)) * time.Millisecond,
		WithStock:    true,
		Stock:        mongoStock{mc},
	}
	log.Fatal(service.Run(cfg, business))
}
