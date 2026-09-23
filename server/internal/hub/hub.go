package hub

import (
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"server/internal/events"
)

// MaxLogs caps the in-memory history sent to freshly opened UIs.
const MaxLogs = 300

// Hub keeps the recent log ring buffer and fans new logs out to browsers.
type Hub struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]struct{}
	logs    []events.StepLog
}

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func New() *Hub {
	return &Hub{clients: map[*websocket.Conn]struct{}{}}
}

// Add stores a log line and broadcasts it to every connected browser.
func (h *Hub) Add(l events.StepLog) {
	h.mu.Lock()
	h.logs = append(h.logs, l)
	if len(h.logs) > MaxLogs {
		h.logs = h.logs[len(h.logs)-MaxLogs:]
	}
	clients := make([]*websocket.Conn, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	for _, c := range clients {
		if err := c.WriteJSON(l); err != nil {
			h.mu.Lock()
			delete(h.clients, c)
			h.mu.Unlock()
			c.Close()
		}
	}
}

func (h *Hub) Recent() []events.StepLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]events.StepLog, len(h.logs))
	copy(out, h.logs)
	return out
}

func (h *Hub) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = nil
}

// ServeWS upgrades the connection and parks it until the browser goes away.
// History is served via GET /api/logs, so nothing is replayed here.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	h.mu.Lock()
	h.clients[conn] = struct{}{}
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.clients, conn)
		h.mu.Unlock()
		conn.Close()
	}()
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}
