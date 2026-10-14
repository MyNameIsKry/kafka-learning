package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLimit = 25
	maxLimit     = 100
)

type Summary struct {
	TotalTransactions  int64     `json:"totalTransactions"`
	TotalPredictions   int64     `json:"totalPredictions"`
	TotalAnomalies     int64     `json:"totalAnomalies"`
	AnomalyRate        float64   `json:"anomalyRate"`
	LatestStep         int64     `json:"latestStep"`
	LatestModelVersion string    `json:"latestModelVersion,omitempty"`
	GeneratedAt        time.Time `json:"generatedAt"`
}

type Transaction struct {
	TransactionID  string     `json:"transactionId"`
	SourceRowIndex int64      `json:"sourceRowIndex"`
	Step           int64      `json:"step"`
	Type           string     `json:"type"`
	Amount         float64    `json:"amount"`
	NameOrig       string     `json:"nameOrig"`
	NameDest       string     `json:"nameDest"`
	OldBalanceOrg  float64    `json:"oldbalanceOrg"`
	NewBalanceOrig float64    `json:"newbalanceOrig"`
	OldBalanceDest float64    `json:"oldbalanceDest"`
	NewBalanceDest float64    `json:"newbalanceDest"`
	AnomalyScore   *float64   `json:"anomalyScore,omitempty"`
	IsAnomaly      bool       `json:"isAnomaly"`
	Status         string     `json:"status"`
	ModelVersion   string     `json:"modelVersion,omitempty"`
	ProcessedAt    *time.Time `json:"processedAt,omitempty"`
}

type Alert struct {
	TransactionID string    `json:"transactionId"`
	Step          int64     `json:"step"`
	AnomalyScore  float64   `json:"anomalyScore"`
	ModelVersion  string    `json:"modelVersion"`
	ProcessedAt   time.Time `json:"processedAt"`
	ReasonCodes   []string  `json:"reasonCodes"`
}

type Reader interface {
	Ping(context.Context) error
	Summary(context.Context) (Summary, error)
	Transactions(context.Context, int, int, string) ([]Transaction, error)
	Alerts(context.Context, int, int) ([]Alert, error)
}

func NewHandler(reader Reader, corsOrigin string, ingestion ...TransactionAppender) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("GET /readyz", readyHandler(reader))
	mux.HandleFunc("GET /api/v1/summary", summaryHandler(reader))
	mux.HandleFunc("GET /api/v1/metrics", summaryHandler(reader))
	mux.HandleFunc("GET /api/v1/transactions", transactionsHandler(reader))
	mux.HandleFunc("GET /api/v1/alerts", alertsHandler(reader))
	if len(ingestion) > 0 {
		mux.HandleFunc("POST /api/v1/transactions", createTransactionHandler(ingestion[0]))
	}

	return withCORS(mux, corsOrigin)
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func readyHandler(reader Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := requestContext(r)
		defer cancel()
		if err := reader.Ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}

func summaryHandler(reader Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := requestContext(r)
		defer cancel()
		result, err := reader.Summary(ctx)
		if err != nil {
			writeServerError(w)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}

func transactionsHandler(reader Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit, err := parseLimit(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		anomaliesOnly, err := parseBoolQuery(r, "anomaliesOnly")
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		status, err := parseStatus(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if anomaliesOnly {
			status = "anomaly"
		}
		offset, err := parseOffset(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		ctx, cancel := requestContext(r)
		defer cancel()
		result, err := reader.Transactions(ctx, limit+1, offset, status)
		if err != nil {
			writeServerError(w)
			return
		}
		hasMore := len(result) > limit
		if hasMore {
			result = result[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": result, "limit": limit, "offset": offset, "hasMore": hasMore})
	}
}

func alertsHandler(reader Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit, err := parseLimit(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		offset, err := parseOffset(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		ctx, cancel := requestContext(r)
		defer cancel()
		result, err := reader.Alerts(ctx, limit+1, offset)
		if err != nil {
			writeServerError(w)
			return
		}
		hasMore := len(result) > limit
		if hasMore {
			result = result[:limit]
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": result, "limit": limit, "offset": offset, "hasMore": hasMore})
	}
}

func parseLimit(r *http.Request) (int, error) {
	value := r.URL.Query().Get("limit")
	if value == "" {
		return defaultLimit, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit < 1 || limit > maxLimit {
		return 0, errors.New("limit must be an integer between 1 and 100")
	}
	return limit, nil
}

func parseBoolQuery(r *http.Request, key string) (bool, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, errors.New(key + " must be true or false")
	}
	return parsed, nil
}

func parseOffset(r *http.Request) (int, error) {
	value := r.URL.Query().Get("offset")
	if value == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(value)
	if err != nil || offset < 0 || offset > 1_000_000 {
		return 0, errors.New("offset must be an integer between 0 and 1000000")
	}
	return offset, nil
}

func parseStatus(r *http.Request) (string, error) {
	value := r.URL.Query().Get("status")
	if value == "" {
		return "", nil
	}
	switch value {
	case "pending", "normal", "anomaly":
		return value, nil
	default:
		return "", errors.New("status must be pending, normal or anomaly")
	}
}

func requestContext(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 3*time.Second)
}

func withCORS(next http.Handler, origin string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowedOrigin := strings.TrimSpace(origin)
		if allowedOrigin == "" {
			allowedOrigin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeServerError(w http.ResponseWriter) {
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "data store unavailable"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
