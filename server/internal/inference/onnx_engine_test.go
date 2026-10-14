package inference

import (
	"os"
	"testing"

	"server/internal/events"
)

func TestMockFeaturesUseStableOrderAndDoNotUseLabelsOrBalances(t *testing.T) {
	features, err := mockFeatures(events.ModelInputDTO{
		TransactionID: "tx-1", Step: 372, Type: "TRANSFER", Amount: 100,
		NameOrig: "C1", NameDest: "C2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if features[2] != 0 || features[3] != 0 || features[4] != 0 || features[5] != 0 || features[6] != 1 {
		t.Fatalf("transaction-type one-hot = %v", features[2:])
	}
	if features[1] != 0.5 {
		t.Fatalf("step_fraction = %v, want 0.5", features[1])
	}
}

func TestONNXEnginePredictsAndMapsBatch(t *testing.T) {
	modelPath := os.Getenv("ONNX_MODEL_PATH")
	if modelPath == "" || os.Getenv("ONNXRUNTIME_SHARED_LIBRARY_PATH") == "" {
		t.Skip("set ONNX_MODEL_PATH and ONNXRUNTIME_SHARED_LIBRARY_PATH to run ONNX integration test")
	}
	engine, err := NewONNXEngine(modelPath, 0.8, "mock-onnx-v1")
	if err != nil {
		t.Fatalf("NewONNXEngine() error = %v", err)
	}
	defer engine.Close()

	inputs := []events.ModelInputDTO{
		{TransactionID: "low", Step: 1, Type: "PAYMENT", Amount: 1},
		{TransactionID: "high", Step: 744, Type: "TRANSFER", Amount: 100_000_000},
	}
	predictions, err := engine.Predict(inputs)
	if err != nil {
		t.Fatalf("Predict() error = %v", err)
	}
	if len(predictions) != len(inputs) {
		t.Fatalf("got %d predictions, want %d", len(predictions), len(inputs))
	}
	if predictions[0].TransactionID != "low" || predictions[0].IsAnomaly {
		t.Errorf("low-risk mock output = %+v", predictions[0])
	}
	if predictions[1].TransactionID != "high" || !predictions[1].IsAnomaly {
		t.Errorf("high-risk mock output = %+v", predictions[1])
	}
}
