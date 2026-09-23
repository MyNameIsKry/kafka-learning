package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"server/internal/events"
	"server/internal/service"
)

func main() {
	business := func(o events.Order) (string, error) {
		return fmt.Sprintf("Đã gửi mail xác nhận đơn %s", o.ID), nil
	}
	cfg := service.Config{
		Name:         "email",
		Instance:     service.Getenv("INSTANCE", "email-1"),
		Addr:         service.Getenv("ADDR", ":8082"),
		GroupID:      events.GroupEmail,
		Brokers:      strings.Split(service.Getenv("KAFKA_BROKERS", "localhost:9092"), ","),
		GatewayURL:   service.Getenv("GATEWAY_URL", "http://localhost:8080"),
		WorkMS:       service.GetenvInt("WORK_MS", 700),
		Capacity:     service.GetenvInt("CAPACITY", 5),
		QueueTimeout: time.Duration(service.GetenvInt("QUEUE_TIMEOUT_MS", 2000)) * time.Millisecond,
	}
	log.Fatal(service.Run(cfg, business))
}
