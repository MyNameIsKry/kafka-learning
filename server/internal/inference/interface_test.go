package inference

import (
	"math"
	"testing"

	"server/internal/events"
)

func TestMockEnginePredictMapsBatchAndAppliesThreshold(t *testing.T) {
	engine := MockEngine{Score: 0.8, Threshold: 0.8, ModelVersion: "mock-v1"}
	inputs := []events.ModelInputDTO{
		{TransactionID: "tx-1", SourceRowIndex: 10, Step: 3, Type: "TRANSFER", Amount: 500, NameOrig: "C1", NameDest: "C2"},
		{TransactionID: "tx-2", SourceRowIndex: 11, Step: 4, Type: "PAYMENT", Amount: 20, NameOrig: "C3", NameDest: "M4"},
	}

	got, err := engine.Predict(inputs)
	if err != nil {
		t.Fatalf("Predict() error = %v", err)
	}
	if len(got) != len(inputs) {
		t.Fatalf("Predict() returned %d predictions, want %d", len(got), len(inputs))
	}
	for index, prediction := range got {
		if prediction.TransactionID != inputs[index].TransactionID || prediction.Step != inputs[index].Step {
			t.Errorf("prediction[%d] identity = (%q, %d), want (%q, %d)", index, prediction.TransactionID, prediction.Step, inputs[index].TransactionID, inputs[index].Step)
		}
		if prediction.AnomalyScore != engine.Score || !prediction.IsAnomaly {
			t.Errorf("prediction[%d] score/anomaly = (%v, %v), want (%v, true)", index, prediction.AnomalyScore, prediction.IsAnomaly, engine.Score)
		}
		if prediction.ModelVersion != engine.ModelVersion || prediction.ProcessedAt.IsZero() || prediction.ProcessedAt.Location() != nil && prediction.ProcessedAt.Location().String() != "UTC" {
			t.Errorf("prediction[%d] metadata invalid: %+v", index, prediction)
		}
		if err := prediction.Validate(); err != nil {
			t.Errorf("prediction[%d] failed validation: %v", index, err)
		}
	}
}

func TestMockEnginePredictEmptyBatchReturnsEmpty(t *testing.T) {
	got, err := (MockEngine{Score: 0.1, Threshold: 0.8, ModelVersion: "mock-v1"}).Predict(nil)
	if err != nil {
		t.Fatalf("Predict() error = %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("Predict() = %#v, want empty non-nil slice", got)
	}
}

func TestMockEnginePredictRejectsInvalidConfigurationAndIdentity(t *testing.T) {
	validInput := events.ModelInputDTO{TransactionID: "tx-1", Step: 1}
	tests := []struct {
		name   string
		engine MockEngine
		input  []events.ModelInputDTO
	}{
		{name: "non-finite score", engine: MockEngine{Score: math.NaN(), Threshold: 0.5, ModelVersion: "mock-v1"}, input: []events.ModelInputDTO{validInput}},
		{name: "non-finite threshold", engine: MockEngine{Score: 0.2, Threshold: math.Inf(1), ModelVersion: "mock-v1"}, input: []events.ModelInputDTO{validInput}},
		{name: "missing model version", engine: MockEngine{Score: 0.2, Threshold: 0.5}, input: []events.ModelInputDTO{validInput}},
		{name: "missing transaction id", engine: MockEngine{Score: 0.2, Threshold: 0.5, ModelVersion: "mock-v1"}, input: []events.ModelInputDTO{{Step: 1}}},
		{name: "invalid step", engine: MockEngine{Score: 0.2, Threshold: 0.5, ModelVersion: "mock-v1"}, input: []events.ModelInputDTO{{TransactionID: "tx-1"}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.engine.Predict(test.input); err == nil {
				t.Fatal("Predict() error = nil, want an error")
			}
		})
	}
}
