package events

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestRawContractAndModelWhitelist(t *testing.T) {
	payload := []byte(`{"schemaVersion":2,"transactionId":"paysim-1","sourceRowIndex":1,"step":2,"type":"TRANSFER","amount":10,"nameOrig":"C1","oldbalanceOrg":100,"newbalanceOrig":90,"nameDest":"C2","oldbalanceDest":0,"newbalanceDest":10}`)
	event, err := DecodeRaw(payload)
	if err != nil {
		t.Fatal(err)
	}
	modelPayload, err := Encode(event.ModelInput())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"} {
		if strings.Contains(string(modelPayload), forbidden) {
			t.Fatalf("model input leaked %s: %s", forbidden, modelPayload)
		}
	}
}

func TestRawRejectsUnknownAndNegativeFields(t *testing.T) {
	unknown := []byte(`{"schemaVersion":2,"transactionId":"x","sourceRowIndex":0,"step":1,"type":"PAYMENT","amount":1,"nameOrig":"C1","oldbalanceOrg":1,"newbalanceOrig":0,"nameDest":"M1","oldbalanceDest":0,"newbalanceDest":1,"isFraud":0}`)
	if _, err := DecodeRaw(unknown); err == nil {
		t.Fatal("expected unknown field rejection")
	}
	negative := []byte(`{"schemaVersion":2,"transactionId":"x","sourceRowIndex":0,"step":1,"type":"PAYMENT","amount":1,"nameOrig":"C1","oldbalanceOrg":-1,"newbalanceOrig":0,"nameDest":"M1","oldbalanceDest":0,"newbalanceDest":1}`)
	if _, err := DecodeRaw(negative); err == nil {
		t.Fatal("expected negative balance rejection")
	}
}

func TestPredictionRequiresTimezoneAndFiniteScore(t *testing.T) {
	event := TransactionPredictionEvent{
		SchemaVersion: PredictionSchemaVersion, TransactionID: "x", Step: 1,
		AnomalyScore: .9, IsAnomaly: true, ModelVersion: "mock-v1",
		ProcessedAt: time.Now().UTC(), ReasonCodes: []string{},
	}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	event.AnomalyScore = math.NaN()
	if err := event.Validate(); err == nil {
		t.Fatal("expected non-finite score rejection")
	}
}
