package inference

import "server/internal/events"

type Engine interface {
	Predict(events []events.ModelInputDTO) ([]events.TransactionPredictionEvent, error)
}
