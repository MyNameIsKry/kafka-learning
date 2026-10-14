package inference

import (
	"fmt"
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
		if err := event.Validate(); err != nil {
			return nil, fmt.Errorf("mock prediction: %w", err)
		}
		result = append(result, event)
	}
	return result, nil
}
