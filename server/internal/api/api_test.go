package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeReader struct {
	summary      Summary
	transactions []Transaction
	alerts       []Alert
	pingErr      error
}

func (f fakeReader) Ping(context.Context) error { return f.pingErr }

func (f fakeReader) Summary(context.Context) (Summary, error) { return f.summary, nil }

func (f fakeReader) Transactions(_ context.Context, limit int, _ bool) ([]Transaction, error) {
	if limit < len(f.transactions) {
		return f.transactions[:limit], nil
	}
	return f.transactions, nil
}

func (f fakeReader) Alerts(_ context.Context, limit int) ([]Alert, error) {
	if limit < len(f.alerts) {
		return f.alerts[:limit], nil
	}
	return f.alerts, nil
}

func TestHealthAndReadiness(t *testing.T) {
	handler := NewHandler(fakeReader{}, "http://localhost:5173")

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d", health.Code)
	}
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status = %d", ready.Code)
	}
	if got := ready.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("cors origin = %q", got)
	}
}

func TestDashboardEndpoints(t *testing.T) {
	now := time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC)
	reader := fakeReader{
		summary:      Summary{TotalTransactions: 2, TotalPredictions: 2, TotalAnomalies: 1, AnomalyRate: .5, GeneratedAt: now},
		transactions: []Transaction{{TransactionID: "paysim-1", IsAnomaly: true}},
		alerts:       []Alert{{TransactionID: "paysim-1", ProcessedAt: now, ReasonCodes: []string{"high-score"}}},
	}
	handler := NewHandler(reader, "*")

	for _, path := range []string{"/api/v1/summary", "/api/v1/metrics", "/api/v1/transactions?limit=1&anomaliesOnly=true", "/api/v1/alerts?limit=1"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Errorf("%s status = %d", path, response.Code)
		}
		if response.Body.Len() == 0 {
			t.Errorf("%s returned an empty body", path)
		}
	}
}

func TestQueryValidationAndDatabaseFailure(t *testing.T) {
	handler := NewHandler(fakeReader{}, "*")
	for _, path := range []string{"/api/v1/transactions?limit=0", "/api/v1/alerts?limit=101", "/api/v1/transactions?anomaliesOnly=maybe"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s status = %d", path, response.Code)
		}
	}

	failing := NewHandler(fakeReader{pingErr: errors.New("mongo down")}, "*")
	response := httptest.NewRecorder()
	failing.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed readiness status = %d", response.Code)
	}
}
