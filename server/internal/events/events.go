package events

import "time"

// Topic and consumer groups shared by gateway and services.
const (
	TopicOrders = "orders"
	// NumPartitions is fixed at topic creation time.
	NumPartitions = 3

	GroupInventory = "inventory-group"
	GroupEmail     = "email-group"
	GroupShipping  = "shipping-group"
)

// Processing modes. They decide which UI column a log line belongs to.
const (
	ModeSync  = "sync"
	ModeKafka = "kafka"
)

// Step statuses shown on the UI timeline.
const (
	StatusStart = "start"
	StatusDone  = "done"
	StatusError = "error"
)

// InitialStock is the starting inventory for every product.
var InitialStock = map[string]int{
	"SP-01": 100,
	"SP-02": 100,
	"SP-03": 100,
}

// Order is both the HTTP body of the sync flow and the Kafka message value.
type Order struct {
	ID        string    `json:"id"`
	ProductID string    `json:"productId"`
	Qty       int       `json:"qty"`
	Mode      string    `json:"mode"`
	Burst     bool      `json:"burst"`
	CreatedAt time.Time `json:"createdAt"`
}

// StepLog is one realtime log line pushed to the UI over WebSocket.
type StepLog struct {
	ID         string    `json:"id"`
	Mode       string    `json:"mode"`
	Service    string    `json:"service"`
	Instance   string    `json:"instance"`
	OrderID    string    `json:"orderId"`
	ProductID  string    `json:"productId"`
	Qty        int       `json:"qty"`
	Status     string    `json:"status"`
	Message    string    `json:"message"`
	Partition  int       `json:"partition"`
	Offset     int64     `json:"offset"`
	DurationMs int64     `json:"durationMs"`
	Burst      bool      `json:"burst"`
	At         time.Time `json:"at"`
}
