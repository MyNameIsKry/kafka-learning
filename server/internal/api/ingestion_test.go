package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"server/internal/events"
	"server/internal/eventstore"
)

type fakeAppender struct {
	calls int
	err   error
}

func (a *fakeAppender) AppendTransaction(context.Context, events.RawTransactionEvent) (string, error) {
	a.calls++
	return "event-1", a.err
}

const validTransaction = `{"schemaVersion":2,"transactionId":"http-1","sourceRowIndex":1,"step":1,"type":"TRANSFER","amount":20,"nameOrig":"C1","nameDest":"C2","oldbalanceOrg":200,"newbalanceOrig":180,"oldbalanceDest":0,"newbalanceDest":20}`

func TestIngestionDurableAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"accepted", nil, 202}, {"outage", errors.New("store down"), 503}, {"conflict", eventstore.ErrConflict, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &fakeAppender{err: tc.err}
			handler := NewHandler(fakeReader{}, "*", a)
			r := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", strings.NewReader(validTransaction))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.status || a.calls != 1 {
				t.Fatalf("status=%d calls=%d", w.Code, a.calls)
			}
			if tc.err != nil && strings.Contains(w.Body.String(), `"status":"accepted"`) {
				t.Fatal("acknowledged failed persistence")
			}
		})
	}
}

func TestIngestionRejectsInvalidAndLabels(t *testing.T) {
	for _, payload := range []string{"null", `{}`, validTransaction + `{}`, strings.Replace(validTransaction, `"amount":20`, `"amount":-1`, 1), strings.Replace(validTransaction, `"step":1`, `"step":1,"isFraud":1`, 1), strings.Replace(validTransaction, `"amount":20,`, "", 1), strings.Replace(validTransaction, `"oldbalanceOrg":200`, `"oldbalanceOrg":null`, 1)} {
		a := &fakeAppender{}
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/v1/transactions", strings.NewReader(payload))
		r.Header.Set("Content-Type", "application/json")
		NewHandler(fakeReader{}, "*", a).ServeHTTP(w, r)
		if w.Code != 400 || a.calls != 0 {
			t.Fatalf("invalid request persisted: %s status=%d calls=%d", payload, w.Code, a.calls)
		}
	}
}

func TestIngestionBodyLimitAndMediaType(t *testing.T) {
	for _, tc := range []struct {
		body, contentType string
		status            int
	}{{validTransaction, "text/plain", 415}, {strings.Repeat("x", 65537), "application/json", 413}} {
		a := &fakeAppender{}
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/api/v1/transactions", strings.NewReader(tc.body))
		r.Header.Set("Content-Type", tc.contentType)
		NewHandler(fakeReader{}, "*", a).ServeHTTP(w, r)
		if w.Code != tc.status || a.calls != 0 {
			t.Fatalf("status=%d calls=%d", w.Code, a.calls)
		}
	}
}
