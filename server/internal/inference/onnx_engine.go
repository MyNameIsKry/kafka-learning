package inference

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	ort "github.com/yalue/onnxruntime_go"
	"server/internal/events"
)

const mockFeatureCount = 7

var onnxEnvironmentMu sync.Mutex

// ONNXEngine runs a float32 ONNX model with [batch, 7] `features` input and
// [batch, 1] `scores` output. The current mock feature layout is in
// gatv2-pipeline/scripts/export_mock_onnx.py.
type ONNXEngine struct {
	session      *ort.DynamicAdvancedSession
	threshold    float64
	modelVersion string
	mu           sync.Mutex
	closed       bool
}

func NewONNXEngine(modelPath string, threshold float64, modelVersion string) (*ONNXEngine, error) {
	if math.IsNaN(threshold) || math.IsInf(threshold, 0) {
		return nil, fmt.Errorf("anomaly threshold must be finite")
	}
	if strings.TrimSpace(modelVersion) == "" {
		return nil, fmt.Errorf("model version must not be empty")
	}
	sharedLibrary := strings.TrimSpace(os.Getenv("ONNXRUNTIME_SHARED_LIBRARY_PATH"))
	if sharedLibrary == "" {
		return nil, fmt.Errorf("ONNXRUNTIME_SHARED_LIBRARY_PATH must point to ONNX Runtime shared library")
	}

	onnxEnvironmentMu.Lock()
	if !ort.IsInitialized() {
		ort.SetSharedLibraryPath(sharedLibrary)
		if err := ort.InitializeEnvironment(); err != nil {
			onnxEnvironmentMu.Unlock()
			return nil, fmt.Errorf("initialize ONNX Runtime: %w", err)
		}
	}
	onnxEnvironmentMu.Unlock()

	session, err := ort.NewDynamicAdvancedSession(modelPath, []string{"features"}, []string{"scores"}, nil)
	if err != nil {
		return nil, fmt.Errorf("load ONNX model %q: %w", modelPath, err)
	}
	return &ONNXEngine{session: session, threshold: threshold, modelVersion: modelVersion}, nil
}

func (engine *ONNXEngine) Predict(inputs []events.ModelInputDTO) ([]events.TransactionPredictionEvent, error) {
	if len(inputs) == 0 {
		return []events.TransactionPredictionEvent{}, nil
	}
	features := make([]float32, 0, len(inputs)*mockFeatureCount)
	for _, input := range inputs {
		row, err := mockFeatures(input)
		if err != nil {
			return nil, err
		}
		features = append(features, row[:]...)
	}
	inputTensor, err := ort.NewTensor(ort.NewShape(int64(len(inputs)), mockFeatureCount), features)
	if err != nil {
		return nil, fmt.Errorf("create ONNX feature tensor: %w", err)
	}
	defer inputTensor.Destroy()

	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return nil, fmt.Errorf("ONNX engine is closed")
	}
	outputs := []ort.Value{nil}
	if err := engine.session.Run([]ort.Value{inputTensor}, outputs); err != nil {
		return nil, fmt.Errorf("run ONNX model: %w", err)
	}
	outputTensor, ok := outputs[0].(*ort.Tensor[float32])
	if !ok || outputTensor == nil {
		return nil, fmt.Errorf("ONNX output `scores` must be a float32 tensor")
	}
	defer outputTensor.Destroy()
	shape := outputTensor.GetShape()
	if len(shape) != 2 || shape[0] != int64(len(inputs)) || shape[1] != 1 {
		return nil, fmt.Errorf("ONNX output shape %v, want [%d, 1]", shape, len(inputs))
	}
	scores := outputTensor.GetData()
	if len(scores) != len(inputs) {
		return nil, fmt.Errorf("ONNX model returned %d scores for %d transactions", len(scores), len(inputs))
	}

	processedAt := time.Now().UTC()
	predictions := make([]events.TransactionPredictionEvent, len(inputs))
	for index, score := range scores {
		if math.IsNaN(float64(score)) || math.IsInf(float64(score), 0) {
			return nil, fmt.Errorf("ONNX model returned non-finite score for input %d", index)
		}
		prediction := events.TransactionPredictionEvent{
			SchemaVersion: events.PredictionSchemaVersion,
			TransactionID: inputs[index].TransactionID,
			Step:          inputs[index].Step,
			AnomalyScore:  float64(score),
			IsAnomaly:     float64(score) >= engine.threshold,
			ModelVersion:  engine.modelVersion,
			ProcessedAt:   processedAt,
			ReasonCodes:   []string{},
		}
		if err := prediction.Validate(); err != nil {
			return nil, fmt.Errorf("ONNX prediction %d: %w", index, err)
		}
		predictions[index] = prediction
	}
	return predictions, nil
}

func (engine *ONNXEngine) Close() error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return nil
	}
	engine.closed = true
	return engine.session.Destroy()
}

func mockFeatures(input events.ModelInputDTO) ([mockFeatureCount]float32, error) {
	var features [mockFeatureCount]float32
	if strings.TrimSpace(input.TransactionID) == "" || input.Step < 1 || math.IsNaN(input.Amount) || math.IsInf(input.Amount, 0) || input.Amount < 0 {
		return features, fmt.Errorf("invalid model input identity, step, or amount")
	}
	features[0] = float32(math.Log1p(input.Amount))
	features[1] = float32(float64(input.Step) / 744.0)
	typeIndex := map[string]int{"CASH_IN": 2, "CASH_OUT": 3, "DEBIT": 4, "PAYMENT": 5, "TRANSFER": 6}
	index, ok := typeIndex[input.Type]
	if !ok {
		return features, fmt.Errorf("unsupported model transaction type %q", input.Type)
	}
	features[index] = 1
	return features, nil
}
