"""AI-facing inference interface used by the Kafka adapter."""

from typing import Protocol

from contracts.events import ModelInputDTO, TransactionPredictionEvent


class InferenceEngine(Protocol):
    def predict(self, transactions: list[ModelInputDTO]) -> list[TransactionPredictionEvent]:
        """Return exactly one prediction for each accepted transaction ID."""
