"""Deterministic mock predictor for Kafka wiring tests only."""

from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite

from contracts.events import ModelInputDTO, TransactionPredictionEvent


class MockInferenceEngine:
    """Return a configurable score without performing ML inference."""

    def __init__(
        self,
        *,
        score: float = 0.1,
        threshold: float = 0.8,
        model_version: str = "mock-v1",
    ) -> None:
        if not isfinite(score) or not isfinite(threshold):
            raise ValueError("score and threshold must be finite")
        self.score = score
        self.threshold = threshold
        self.model_version = model_version

    def predict(self, transactions: list[ModelInputDTO]) -> list[TransactionPredictionEvent]:
        processed_at = datetime.now(timezone.utc)
        return [
            TransactionPredictionEvent(
                schemaVersion=1,
                transactionId=transaction.transactionId,
                step=transaction.step,
                anomalyScore=self.score,
                isAnomaly=self.score >= self.threshold,
                modelVersion=self.model_version,
                processedAt=processed_at,
                reasonCodes=[],
            )
            for transaction in transactions
        ]
