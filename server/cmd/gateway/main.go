package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"server/internal/broker"
	"server/internal/events"
	"server/internal/hub"
	mongostore "server/internal/mongo"
	"server/internal/service"
)

type downstream struct {
	name string
	url  string
}

type step struct {
	Service    string `json:"service"`
	Ok         bool   `json:"ok"`
	DurationMs int64  `json:"durationMs"`
	Message    string `json:"message"`
}

var (
	hubCenter = hub.New()
	writer    *kafka.Writer
	db        *mongostore.Client
	callTO    time.Duration
	services  []downstream
)

func main() {
	addr := service.Getenv("ADDR", ":8080")
	brokers := strings.Split(service.Getenv("KAFKA_BROKERS", "localhost:9092"), ",")
	callTO = time.Duration(service.GetenvInt("SYNC_CALL_TIMEOUT_MS", 3000)) * time.Millisecond
	services = []downstream{
		{"inventory", service.Getenv("INVENTORY_URL", "http://localhost:8081")},
		{"email", service.Getenv("EMAIL_URL", "http://localhost:8082")},
		{"shipping", service.Getenv("SHIPPING_URL", "http://localhost:8083")},
	}

	var err error
	db, err = mongostore.Connect(
		service.Getenv("MONGO_URI", "mongodb://localhost:27017"),
		service.Getenv("MONGO_DB", "kafka-demo"),
	)
	if err != nil {
		log.Fatal(err)
	}

	waitForTopic(brokers)
	writer = broker.NewWriter(brokers, events.TopicOrders)
	defer writer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/orders/sync", handleSync)
	mux.HandleFunc("POST /api/orders/async", handleAsync)
	mux.HandleFunc("POST /api/burst", handleBurst)
	mux.HandleFunc("GET /api/state", handleState)
	mux.HandleFunc("GET /api/logs", handleLogs)
	mux.HandleFunc("GET /api/orders", handleOrders)
	mux.HandleFunc("POST /api/reset", handleReset)
	mux.HandleFunc("POST /api/services/{name}/down", handleServiceToggle)
	mux.HandleFunc("POST /api/services/{name}/up", handleServiceToggle)
	mux.HandleFunc("POST /internal/log", handleInternalLog)
	mux.HandleFunc("GET /ws", hubCenter.ServeWS)

	log.Printf("gateway listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, cors(mux)))
}

func waitForTopic(brokers []string) {
	for {
		if err := broker.EnsureTopic(brokers, events.TopicOrders, events.NumPartitions); err == nil {
			return
		}
		log.Printf("waiting for kafka at %v...", brokers)
		time.Sleep(2 * time.Second)
	}
}

// runSync pushes one order through the three services sequentially.
// The caller (user) waits until all three finish.
func runSync(order events.Order) (steps []step, totalMs int64, ok bool) {
	start := time.Now()
	ok = true
	for _, d := range services {
		st := callService(d, order)
		steps = append(steps, st)
		if !st.Ok {
			ok = false
			break
		}
	}
	return steps, time.Since(start).Milliseconds(), ok
}

func callService(d downstream, order events.Order) step {
	body, _ := json.Marshal(order)
	ctx, cancel := context.WithTimeout(context.Background(), callTO)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, d.url+"/process", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	took := time.Since(start).Milliseconds()
	if err != nil {
		return step{d.name, false, took, "không gọi được " + d.name}
	}
	defer resp.Body.Close()
	var out struct {
		Ok         bool   `json:"ok"`
		Message    string `json:"message"`
		DurationMs int64  `json:"durationMs"`
	}
	data, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(data, &out)
	if resp.StatusCode >= 300 || !out.Ok {
		return step{d.name, false, took, out.Message}
	}
	return step{d.name, true, out.DurationMs, out.Message}
}

func handleSync(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ProductID string `json:"productId"`
		Qty       int    `json:"qty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.ProductID == "" {
		writeErr(w, http.StatusBadRequest, "thiếu productId")
		return
	}
	if in.Qty <= 0 {
		in.Qty = 1
	}
	order := newOrder(in.ProductID, in.Qty, events.ModeSync, false)
	if err := db.InsertOrder(order); err != nil {
		writeErr(w, http.StatusBadGateway, "không ghi được đơn vào MongoDB")
		return
	}
	gwLog(order, events.StatusStart, "nhận đơn đồng bộ")
	steps, total, ok := runSync(order)
	db.FinishOrder(order.ID, total, ok)
	if ok {
		gwLog(order, events.StatusDone, fmt.Sprintf("xong cả 3 bước sau %d ms", total))
		writeJSON(w, http.StatusOK, map[string]any{"orderId": order.ID, "totalMs": total, "steps": steps, "ok": true})
		return
	}
	gwLog(order, events.StatusError, "đơn hỏng giữa chừng")
	writeJSON(w, http.StatusBadGateway, map[string]any{"orderId": order.ID, "totalMs": total, "steps": steps, "ok": false})
}

// runAsync publishes one order and returns immediately.
func runAsync(order events.Order) (int64, error) {
	start := time.Now()
	body, _ := json.Marshal(order)
	err := writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(order.ProductID),
		Value: body,
	})
	return time.Since(start).Milliseconds(), err
}

func handleAsync(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ProductID string `json:"productId"`
		Qty       int    `json:"qty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.ProductID == "" {
		writeErr(w, http.StatusBadRequest, "thiếu productId")
		return
	}
	if in.Qty <= 0 {
		in.Qty = 1
	}
	order := newOrder(in.ProductID, in.Qty, events.ModeKafka, false)
	if err := db.InsertOrder(order); err != nil {
		writeErr(w, http.StatusBadGateway, "không ghi được đơn vào MongoDB")
		return
	}
	took, err := runAsync(order)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "không ghi được vào topic orders")
		return
	}
	gwLog(order, events.StatusDone, fmt.Sprintf("đã ghi vào topic orders sau %d ms, consumer xử lý dần", took))
	writeJSON(w, http.StatusAccepted, map[string]any{"orderId": order.ID, "totalMs": took})
}

func handleBurst(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Mode      string `json:"mode"`
		Count     int    `json:"count"`
		ProductID string `json:"productId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "body không hợp lệ")
		return
	}
	if in.Mode != events.ModeSync && in.Mode != events.ModeKafka {
		writeErr(w, http.StatusBadRequest, "mode phải là sync hoặc kafka")
		return
	}
	if in.Count <= 0 {
		in.Count = 50
	}
	if in.Count > 200 {
		in.Count = 200
	}
	if in.ProductID == "" {
		in.ProductID = "SP-01"
	}

	start := time.Now()
	lat := make([]int64, in.Count)
	var okCount, failCount int64
	var mu sync.Mutex
	// Fire up to 50 orders at once: a real burst piles onto the services
	// instead of trickling through a narrow worker pool.
	workers := min(in.Count, 50)
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range lat {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			order := newOrder(in.ProductID, 1, in.Mode, true)
			var took int64
			var ok bool
			if err := db.InsertOrder(order); err != nil {
				mu.Lock()
				lat[i] = 0
				failCount++
				mu.Unlock()
				return
			}
			if in.Mode == events.ModeSync {
				var steps []step
				steps, took, ok = runSync(order)
				_ = steps
				db.FinishOrder(order.ID, took, ok)
			} else {
				var err error
				took, err = runAsync(order)
				ok = err == nil
			}
			mu.Lock()
			lat[i] = took
			if ok {
				okCount++
			} else {
				failCount++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	pct := func(p float64) int64 { return lat[min(int(p*float64(len(lat))), len(lat)-1)] }
	writeJSON(w, http.StatusOK, map[string]any{
		"mode": in.Mode, "count": in.Count,
		"ok": okCount, "failed": failCount,
		"p50": pct(0.5), "p95": pct(0.95), "max": lat[len(lat)-1],
		"elapsedMs": time.Since(start).Milliseconds(),
	})
}

func handleState(w http.ResponseWriter, _ *http.Request) {
	type svcState struct {
		Name      string `json:"name"`
		Instance  string `json:"instance"`
		Down      bool   `json:"down"`
		Processed int64  `json:"processed"`
	}
	out := []svcState{}
	stock := map[string]int{}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, d := range services {
		st := svcState{Name: d.name, Instance: d.name + "-1", Down: true}
		if resp, err := client.Get(d.url + "/state"); err == nil {
			var body struct {
				Name      string         `json:"name"`
				Instance  string         `json:"instance"`
				Down      bool           `json:"down"`
				Processed int64          `json:"processed"`
				Stock     map[string]int `json:"stock"`
			}
			if data, err := io.ReadAll(resp.Body); err == nil {
				_ = json.Unmarshal(data, &body)
			}
			resp.Body.Close()
			st.Instance, st.Down, st.Processed = body.Instance, body.Down, body.Processed
			if body.Stock != nil {
				stock = body.Stock
			}
		}
		out = append(out, st)
	}
	writeJSON(w, http.StatusOK, map[string]any{"stock": stock, "services": out})
}

func handleLogs(w http.ResponseWriter, _ *http.Request) {
	logs := hubCenter.Recent()
	if logs == nil {
		logs = []events.StepLog{}
	}
	writeJSON(w, http.StatusOK, logs)
}

// handleOrders serves the filtered, paginated order history from MongoDB.
func handleOrders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	valid := func(v string, allowed ...string) string {
		for _, a := range allowed {
			if v == a {
				return v
			}
		}
		return ""
	}
	page := 1
	if n, err := strconv.Atoi(q.Get("page")); err == nil && n > 0 {
		page = n
	}
	limit := 10
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = n
	}
	writeJSON(w, http.StatusOK, db.ListOrderPage(mongostore.OrderFilter{
		Mode:      valid(q.Get("mode"), events.ModeSync, events.ModeKafka),
		Status:    valid(q.Get("status"), mongostore.OrderReceived, mongostore.OrderProcessing, mongostore.OrderDone, mongostore.OrderError),
		ProductID: valid(q.Get("productId"), "SP-01", "SP-02", "SP-03"),
		Burst:     valid(q.Get("burst"), "true", "false"),
		Page:      page,
		Limit:     limit,
	}))
}

func handleReset(w http.ResponseWriter, _ *http.Request) {
	client := &http.Client{Timeout: 2 * time.Second}
	for _, d := range services {
		req, _ := http.NewRequest(http.MethodPost, d.url+"/admin/reset", nil)
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
		}
	}
	hubCenter.Clear()
	if err := db.ClearOrders(); err != nil {
		writeErr(w, http.StatusBadGateway, "không xoá được lịch sử đơn")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func handleServiceToggle(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	action := "up"
	if strings.HasSuffix(r.URL.Path, "/down") {
		action = "down"
	}
	var target string
	for _, d := range services {
		if d.name == name {
			target = d.url
		}
	}
	if target == "" {
		writeErr(w, http.StatusNotFound, "không có service "+name)
		return
	}
	req, _ := http.NewRequest(http.MethodPost, target+"/admin/"+action, nil)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "không gọi được "+name)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "service": name, "down": action == "down"})
}

func handleInternalLog(w http.ResponseWriter, r *http.Request) {
	var entry events.StepLog
	if err := json.NewDecoder(r.Body).Decode(&entry); err != nil {
		writeErr(w, http.StatusBadRequest, "log không hợp lệ")
		return
	}
	hubCenter.Add(entry)
	db.ApplyLog(entry)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func newOrder(productID string, qty int, mode string, burst bool) events.Order {
	return events.Order{
		ID:        "ORD-" + strings.ToUpper(uuid.NewString()[:4]),
		ProductID: productID,
		Qty:       qty,
		Mode:      mode,
		Burst:     burst,
		CreatedAt: time.Now(),
	}
}

func gwLog(order events.Order, status, msg string) {
	hubCenter.Add(events.StepLog{
		ID: uuid.NewString(), Mode: order.Mode, Service: "gateway", Instance: "gateway-1",
		OrderID: order.ID, ProductID: order.ProductID, Qty: order.Qty,
		Status: status, Message: msg, Partition: -1, Burst: order.Burst, At: time.Now(),
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"ok": false, "message": msg})
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
