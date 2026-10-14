package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"server/internal/events"
	"server/internal/eventstore"
	"server/internal/kafkaio"
)

type TransactionAppender interface {
	AppendTransaction(context.Context, events.RawTransactionEvent) (string, error)
}

type EventStoreAppender struct{ Writer *eventstore.Writer }

func (a EventStoreAppender) AppendTransaction(ctx context.Context, event events.RawTransactionEvent) (string, error) {
	payload, err := events.Encode(event)
	if err != nil {
		return "", err
	}
	err = kafkaio.Publish(ctx, a.Writer, event.NameOrig, payload)
	return eventstore.Identity(a.Writer.Topic, event.TransactionID).String(), err
}

// transactionId is the idempotency key. Retries must retain the same full body.
func createTransactionHandler(appender TransactionAppender) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "cannot read body (maximum 64 KiB)"})
			return
		}
		event, err := events.DecodeRaw(payload)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		// Do not silently treat missing/null numeric fields as Go zero values.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(payload, &fields); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be a JSON object"})
			return
		}
		for _, name := range []string{"schemaVersion", "transactionId", "sourceRowIndex", "step", "type", "amount", "nameOrig", "nameDest", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"} {
			value, ok := fields[name]
			if !ok || string(value) == "null" {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": name + " is required and must not be null"})
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()
		id, err := appender.AppendTransaction(ctx, event)
		if errors.Is(err, eventstore.ErrConflict) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "transactionId already exists with different content"})
			return
		}
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "event store unavailable; retry with the same transactionId and body"})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"eventId": id, "transactionId": event.TransactionID, "status": "accepted"})
	}
}
