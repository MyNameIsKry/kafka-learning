package inference

import (
	"fmt"
	"math"
	"strings"
	"time"

	"server/internal/events"
)

type Engine interface {
	Predict(events []events.ModelInputDTO) ([]events.TransactionPredictionEvent, error)
}

type MockEngine struct {
	Score        float64
	Threshold    float64
	ModelVersion string
}

func (engine MockEngine) Predict(inputs []events.ModelInputDTO) ([]events.TransactionPredictionEvent, error) {
	if math.IsNaN(engine.Score) || math.IsInf(engine.Score, 0) {
		return nil, fmt.Errorf("mock score must be finite")
	}
	if math.IsNaN(engine.Threshold) || math.IsInf(engine.Threshold, 0) {
		return nil, fmt.Errorf("mock threshold must be finite")
	}
	if strings.TrimSpace(engine.ModelVersion) == "" {
		return nil, fmt.Errorf("mock model version must not be empty")
	}

	result := make([]events.TransactionPredictionEvent, 0, len(inputs))
	for _, input := range inputs {
		event := events.TransactionPredictionEvent{
			SchemaVersion: events.PredictionSchemaVersion,
			TransactionID: input.TransactionID,
			Step:          input.Step,
			AnomalyScore:  engine.Score,
			IsAnomaly:     engine.Score >= engine.Threshold,
			ModelVersion:  engine.ModelVersion,
			ProcessedAt:   time.Now().UTC(),
			ReasonCodes:   []string{},
		}
		if strings.TrimSpace(input.TransactionID) == "" || input.Step < 1 {
			return nil, fmt.Errorf("mock input has invalid transaction identity")
		}
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("mock prediction: %w", err)
		}
		result = append(result, event)
	}
	return result, nil
}
