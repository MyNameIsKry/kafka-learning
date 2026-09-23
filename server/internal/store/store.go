package store

import (
	"fmt"
	"sync"

	"server/internal/events"
)

// Store is a thread-safe in-memory inventory.
// Only the inventory service owns one; other services pass nil.
type Store struct {
	mu    sync.Mutex
	stock map[string]int
}

func New() *Store {
	s := &Store{stock: map[string]int{}}
	s.Reset()
	return s
}

// Decrease subtracts qty from productID, returning the remaining stock.
func (s *Store) Decrease(productID string, qty int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	left, ok := s.stock[productID]
	if !ok {
		return 0, fmt.Errorf("không có sản phẩm %s", productID)
	}
	if left < qty {
		return left, fmt.Errorf("không đủ tồn kho %s (còn %d, cần %d)", productID, left, qty)
	}
	left -= qty
	s.stock[productID] = left
	return left, nil
}

func (s *Store) Snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.stock))
	for k, v := range s.stock {
		out[k] = v
	}
	return out
}

func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range events.InitialStock {
		s.stock[k] = v
	}
}
