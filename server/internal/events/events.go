// Package events contains the Go runtime contracts shared by the producer,
// inference consumer and MongoDB projection sinks.
package events

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

const (
	RawSchemaVersion        = 2
	PredictionSchemaVersion = 1
)

var transactionTypes = map[string]struct{}{
	"CASH_IN": {}, "CASH_OUT": {}, "DEBIT": {}, "PAYMENT": {}, "TRANSFER": {},
}

type RawTransactionEvent struct {
	SchemaVersion  int     `json:"schemaVersion" bson:"schemaVersion"`
	TransactionID  string  `json:"transactionId" bson:"transactionId"`
	SourceRowIndex int64   `json:"sourceRowIndex" bson:"sourceRowIndex"`
	Step           int64   `json:"step" bson:"step"`
	Type           string  `json:"type" bson:"type"`
	Amount         float64 `json:"amount" bson:"amount"`
	NameOrig       string  `json:"nameOrig" bson:"nameOrig"`
	OldBalanceOrg  float64 `json:"oldbalanceOrg" bson:"oldbalanceOrg"`
	NewBalanceOrig float64 `json:"newbalanceOrig" bson:"newbalanceOrig"`
	NameDest       string  `json:"nameDest" bson:"nameDest"`
	OldBalanceDest float64 `json:"oldbalanceDest" bson:"oldbalanceDest"`
	NewBalanceDest float64 `json:"newbalanceDest" bson:"newbalanceDest"`
}

type ModelInputDTO struct {
	TransactionID  string  `json:"transactionId"`
	SourceRowIndex int64   `json:"sourceRowIndex"`
	Step           int64   `json:"step"`
	Type           string  `json:"type"`
	Amount         float64 `json:"amount"`
	NameOrig       string  `json:"nameOrig"`
	NameDest       string  `json:"nameDest"`
}

type TransactionPredictionEvent struct {
	SchemaVersion int       `json:"schemaVersion" bson:"schemaVersion"`
	TransactionID string    `json:"transactionId" bson:"transactionId"`
	Step          int64     `json:"step" bson:"step"`
	AnomalyScore  float64   `json:"anomalyScore" bson:"anomalyScore"`
	IsAnomaly     bool      `json:"isAnomaly" bson:"isAnomaly"`
	ModelVersion  string    `json:"modelVersion" bson:"modelVersion"`
	ProcessedAt   time.Time `json:"processedAt" bson:"processedAt"`
	ReasonCodes   []string  `json:"reasonCodes" bson:"reasonCodes"`
}

type GroundTruthDTO struct {
	TransactionID  string `json:"transactionId"`
	Step           int64  `json:"step"`
	IsFraud        int    `json:"isFraud"`
	IsFlaggedFraud int    `json:"isFlaggedFraud"`
}

type DLQEvent struct {
	SourceTopic     string         `json:"sourceTopic"`
	SourcePartition int            `json:"sourcePartition"`
	SourceOffset    int64          `json:"sourceOffset"`
	ErrorCode       string         `json:"errorCode"`
	ErrorMessage    string         `json:"errorMessage"`
	FailedAt        time.Time      `json:"failedAt"`
	OriginalPayload map[string]any `json:"originalPayload"`
}

func (event RawTransactionEvent) Validate() error {
	if event.SchemaVersion != RawSchemaVersion {
		return fmt.Errorf("schemaVersion must be %d", RawSchemaVersion)
	}
	if strings.TrimSpace(event.TransactionID) == "" || event.SourceRowIndex < 0 || event.Step < 1 {
		return errors.New("transactionId, sourceRowIndex and step are invalid")
	}
	if _, ok := transactionTypes[event.Type]; !ok {
		return fmt.Errorf("unsupported transaction type %q", event.Type)
	}
	if strings.TrimSpace(event.NameOrig) == "" || strings.TrimSpace(event.NameDest) == "" {
		return errors.New("account identifiers must not be empty")
	}
	for name, value := range map[string]float64{
		"amount": event.Amount, "oldbalanceOrg": event.OldBalanceOrg,
		"newbalanceOrig": event.NewBalanceOrig, "oldbalanceDest": event.OldBalanceDest,
		"newbalanceDest": event.NewBalanceDest,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return fmt.Errorf("%s must be finite and non-negative", name)
		}
	}
	return nil
}

func (event RawTransactionEvent) ModelInput() ModelInputDTO {
	return ModelInputDTO{
		TransactionID: event.TransactionID, SourceRowIndex: event.SourceRowIndex,
		Step: event.Step, Type: event.Type, Amount: event.Amount,
		NameOrig: event.NameOrig, NameDest: event.NameDest,
	}
}

func (event TransactionPredictionEvent) Validate() error {
	if event.SchemaVersion != PredictionSchemaVersion || strings.TrimSpace(event.TransactionID) == "" || event.Step < 1 {
		return errors.New("invalid prediction identity or schemaVersion")
	}
	if math.IsNaN(event.AnomalyScore) || math.IsInf(event.AnomalyScore, 0) {
		return errors.New("anomalyScore must be finite")
	}
	if strings.TrimSpace(event.ModelVersion) == "" || event.ProcessedAt.IsZero() {
		return errors.New("prediction modelVersion and timezone-aware processedAt are required")
	}
	return nil
}

func (event GroundTruthDTO) Validate() error {
	if strings.TrimSpace(event.TransactionID) == "" || event.Step < 1 || event.IsFraud < 0 || event.IsFraud > 1 || event.IsFlaggedFraud < 0 || event.IsFlaggedFraud > 1 {
		return errors.New("invalid ground truth")
	}
	return nil
}

func DecodeRaw(payload []byte) (RawTransactionEvent, error) {
	var event RawTransactionEvent
	if err := decodeStrict(payload, &event); err != nil {
		return event, err
	}
	return event, event.Validate()
}

func DecodePrediction(payload []byte) (TransactionPredictionEvent, error) {
	var event TransactionPredictionEvent
	if err := decodeStrict(payload, &event); err != nil {
		return event, err
	}
	return event, event.Validate()
}

func Encode(value any) ([]byte, error) { return json.Marshal(value) }

func DecodeDLQPayload(payload []byte) map[string]any {
	var value map[string]any
	if json.Unmarshal(payload, &value) == nil && value != nil {
		return value
	}
	return map[string]any{
		"rawBase64": base64.StdEncoding.EncodeToString(payload),
		"encoding":  "base64",
	}
}

func decodeStrict(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("payload contains multiple JSON values")
		}
		return err
	}
	return nil
}
