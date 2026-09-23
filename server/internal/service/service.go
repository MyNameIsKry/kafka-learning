package service

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"server/internal/broker"
	"server/internal/events"
)

// BusinessFunc holds the business logic shared by the HTTP handler
// and the Kafka consumer. It returns the Vietnamese message shown on the UI.
type BusinessFunc func(order events.Order) (string, error)

// Config wires one service binary. Field defaults come from SPEC section 6.
type Config struct {
	Name         string
	Instance     string
	Addr         string
	GroupID      string
	Brokers      []string
	GatewayURL   string
	WorkMS       int
	Capacity     int
	QueueTimeout time.Duration
	WithStock    bool
	// Stock abstracts the inventory backend (in-memory or MongoDB) so the
	// HTTP handler, the consumer and /state observe the same stock.
	Stock StockStore
}

// StockStore is implemented by store.Store and by the Mongo adapter in
// cmd/inventory.
type StockStore interface {
	Decrease(productID string, qty int) (int, error)
	Snapshot() map[string]int
	Reset()
}

type Service struct {
	cfg       Config
	business  BusinessFunc
	stock     StockStore
	sem      chan struct{}
	down     atomic.Bool
	processed atomic.Int64
	client   *http.Client

	readerMu sync.Mutex
	reader   *kafka.Reader
}

// Run starts the HTTP server and the Kafka consumer loop.
func Run(cfg Config, business BusinessFunc) error {
	s := &Service{
		cfg:      cfg,
		business: business,
		sem:      make(chan struct{}, cfg.Capacity),
		client:   &http.Client{Timeout: 2 * time.Second},
	}
	if cfg.WithStock {
		if cfg.Stock == nil {
			log.Fatal("WithStock needs Config.Stock")
		}
		s.stock = cfg.Stock
	}
	go s.consumeLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /process", s.handleProcess)
	mux.HandleFunc("GET /state", s.handleState)
	mux.HandleFunc("POST /admin/down", s.handleDown)
	mux.HandleFunc("POST /admin/up", s.handleUp)
	mux.HandleFunc("POST /admin/reset", s.handleReset)

	log.Printf("%s (%s) listening on %s, group %s", cfg.Name, cfg.Instance, cfg.Addr, cfg.GroupID)
	return http.ListenAndServe(cfg.Addr, mux)
}

// process is the single business pipeline used by both entry points.
func (s *Service) process(order events.Order, partition int, offset int64) (string, int64, error) {
	start := time.Now()
	s.report(order, events.StatusStart, "bắt đầu xử lý", partition, offset, 0)
	time.Sleep(time.Duration(s.cfg.WorkMS) * time.Millisecond)
	msg, err := s.business(order)
	took := time.Since(start).Milliseconds()
	if err != nil {
		s.report(order, events.StatusError, err.Error(), partition, offset, took)
		return "", took, err
	}
	s.report(order, events.StatusDone, msg, partition, offset, took)
	return msg, took, nil
}

// handleProcess serves the sync flow. It is bounded by CAPACITY so the
// service fails fast under load instead of queuing forever.
func (s *Service) handleProcess(w http.ResponseWriter, r *http.Request) {
	if s.down.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "message": s.cfg.Name + " đang tắt"})
		return
	}
	var order events.Order
	if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "message": "body không hợp lệ"})
		return
	}
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-time.After(s.cfg.QueueTimeout):
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "message": s.cfg.Name + " quá tải"})
		return
	}
	msg, took, err := s.process(order, -1, 0)
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"ok": false, "message": err.Error(), "durationMs": took})
		return
	}
	s.processed.Add(1)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "message": msg, "durationMs": took})
}

// consumeLoop runs the event-driven flow. It has no capacity bound:
// messages simply wait in the topic (consumer lag) instead of failing.
// Fetches use a short timeout so the down flag takes effect promptly
// without ever closing the reader from an HTTP handler (Reader.Close
// blocks until background fetch/commit goroutines finish).
func (s *Service) consumeLoop() {
	for {
		if s.down.Load() {
			time.Sleep(time.Second)
			continue
		}
		rd := s.getReader()
		fetchCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := rd.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			// Keep the reader: kafka-go rejoins the group internally after
			// a rebalance. Recreating it here would use a new member ID and
			// trigger yet another rebalance (ping-pong forever).
			if !s.down.Load() {
				if fetchCtx.Err() != context.DeadlineExceeded {
					log.Printf("%s (%s) fetch error: %v", s.cfg.Name, s.cfg.Instance, err)
				}
				time.Sleep(time.Second)
			}
			continue
		}
		// Hold the fetched message through any downtime instead of dropping
		// it: dropping would skip past it (already buffered past the fetched
		// offset) without processing or committing. It stays uncommitted,
		// so a crash still redelivers it.
		for s.down.Load() {
			time.Sleep(200 * time.Millisecond)
		}
		var order events.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			rd.CommitMessages(context.Background(), msg) // poison: skip
			continue
		}
		if _, _, err := s.process(order, msg.Partition, msg.Offset); err == nil {
			s.processed.Add(1)
		}
		rd.CommitMessages(context.Background(), msg)
	}
}

func (s *Service) getReader() *kafka.Reader {
	s.readerMu.Lock()
	defer s.readerMu.Unlock()
	if s.reader == nil {
		s.reader = broker.NewReader(s.cfg.Brokers, events.TopicOrders, s.cfg.GroupID, s.cfg.Instance)
	}
	return s.reader
}

func (s *Service) handleState(w http.ResponseWriter, _ *http.Request) {
	state := map[string]any{
		"name":      s.cfg.Name,
		"instance":  s.cfg.Instance,
		"down":      s.down.Load(),
		"processed": s.processed.Load(),
	}
	if s.stock != nil {
		state["stock"] = s.stock.Snapshot()
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Service) handleDown(w http.ResponseWriter, _ *http.Request) {
	// Flag only: the consume loop notices within ~1s and stops committing,
	// so messages pile up as consumer lag. Never Close() here.
	s.down.Store(true)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "down": true})
}

func (s *Service) handleUp(w http.ResponseWriter, _ *http.Request) {
	s.down.Store(false)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "down": false})
}

func (s *Service) handleReset(w http.ResponseWriter, _ *http.Request) {
	s.processed.Store(0)
	if s.stock != nil {
		s.stock.Reset()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// report pushes one StepLog to the gateway over plain HTTP (never Kafka),
// so the sync flow stays visible even when the broker is down.
func (s *Service) report(order events.Order, status, msg string, partition int, offset int64, took int64) {
	entry := events.StepLog{
		ID:         uuid.NewString(),
		Mode:       order.Mode,
		Service:    s.cfg.Name,
		Instance:   s.cfg.Instance,
		OrderID:    order.ID,
		ProductID:  order.ProductID,
		Qty:        order.Qty,
		Status:     status,
		Message:    msg,
		Partition:  partition,
		Offset:     offset,
		DurationMs: took,
		Burst:      order.Burst,
		At:         time.Now(),
	}
	body, _ := json.Marshal(entry)
	req, _ := http.NewRequest(http.MethodPost, s.cfg.GatewayURL+"/internal/log", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Getenv helpers keep each cmd/*/main.go around 40 lines.
func Getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func GetenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
