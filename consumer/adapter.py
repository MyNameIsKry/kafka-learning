"""Pure batch adapter between Kafka records and the inference engine."""

from __future__ import annotations

import json
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol

from contracts.events import (
    DLQEvent,
    ModelInputDTO,
    RawTransactionEvent,
    TransactionPredictionEvent,
    parse_raw_transaction,
    to_model_input,
)
from inference.interface import InferenceEngine
from runtime.payload import undecoded_payload


class EventPublisher(Protocol):
    def publish_prediction(self, prediction: TransactionPredictionEvent) -> None:
        ...

    def publish_dlq(self, event: DLQEvent) -> None:
        ...


@dataclass(frozen=True)
class AcceptedTransaction:
    raw: RawTransactionEvent
    model_input: ModelInputDTO


@dataclass(frozen=True)
class ConsumerRecord:
    topic: str
    partition: int
    offset: int
    value: bytes


class BatchInferenceError(RuntimeError):
    """Raised when model output cannot be mapped exactly to the input batch."""


class SeenTransactionCache:
    """Bounded best-effort dedupe cache for at-least-once redelivery."""

    def __init__(self, max_size: int = 100_000) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self.max_size = max_size
        self._items: OrderedDict[str, RawTransactionEvent] = OrderedDict()

    def get(self, transaction_id: str) -> RawTransactionEvent | None:
        value = self._items.get(transaction_id)
        if value is not None:
            self._items.move_to_end(transaction_id)
        return value

    def remember(self, raw: RawTransactionEvent) -> None:
        self._items[raw.transactionId] = raw
        self._items.move_to_end(raw.transactionId)
        while len(self._items) > self.max_size:
            self._items.popitem(last=False)


def _decode_payload(record: ConsumerRecord) -> Any:
    return json.loads(record.value.decode("utf-8"))


def _dlq_event(record: ConsumerRecord, code: str, message: str, payload: Any) -> DLQEvent:
    if isinstance(payload, dict):
        original_payload = payload
    else:
        original_payload = {"raw": payload}
    return DLQEvent(
        sourceTopic=record.topic,
        sourcePartition=record.partition,
        sourceOffset=record.offset,
        errorCode=code,
        errorMessage=message,
        failedAt=datetime.now(timezone.utc),
        originalPayload=original_payload,
    )


def _validate_predictions(
    accepted: list[AcceptedTransaction],
    predictions: Iterable[TransactionPredictionEvent],
) -> dict[str, TransactionPredictionEvent]:
    expected_ids = [transaction.raw.transactionId for transaction in accepted]
    expected = set(expected_ids)
    by_id: dict[str, TransactionPredictionEvent] = {}
    expected_steps = {item.raw.transactionId: item.raw.step for item in accepted}

    for prediction in predictions:
        if not isinstance(prediction, TransactionPredictionEvent):
            raise BatchInferenceError("engine output must be a TransactionPredictionEvent")
        try:
            prediction = TransactionPredictionEvent.model_validate(prediction.model_dump())
        except ValueError as exc:
            raise BatchInferenceError("invalid prediction schema") from exc
        if prediction.transactionId in expected_steps and prediction.step != expected_steps[prediction.transactionId]:
            raise BatchInferenceError(f"prediction step mismatch: {prediction.transactionId}")
        if prediction.transactionId in by_id:
            raise BatchInferenceError(f"duplicate prediction: {prediction.transactionId}")
        by_id[prediction.transactionId] = prediction

    actual = set(by_id)
    missing = expected - actual
    unexpected = actual - expected
    if missing or unexpected or len(by_id) != len(expected_ids):
        raise BatchInferenceError(
            f"prediction mapping mismatch; missing={sorted(missing)}, "
            f"unexpected={sorted(unexpected)}"
        )

    return by_id


def process_batch(
    records: list[ConsumerRecord],
    engine: InferenceEngine,
    publisher: EventPublisher,
    seen_transactions: SeenTransactionCache | None = None,
) -> tuple[int, int]:
    """Validate, infer and publish one batch.

    Returns ``(accepted_count, dlq_count)``. Any publisher or inference error
    propagates so the caller can leave the Kafka input offsets uncommitted.
    """

    accepted_by_id: dict[str, AcceptedTransaction] = {}
    dlq_count = 0

    for record in records:
        payload: Any = undecoded_payload(record.value)
        try:
            payload = _decode_payload(record)
            raw = parse_raw_transaction(payload)
            candidate = AcceptedTransaction(raw=raw, model_input=to_model_input(raw))
            previous = accepted_by_id.get(raw.transactionId)
            if previous is not None:
                if previous.raw.to_json_dict() != raw.to_json_dict():
                    raise BatchInferenceError(
                        f"conflicting duplicate transaction: {raw.transactionId}"
                    )
                continue
            if seen_transactions is not None:
                seen_raw = seen_transactions.get(raw.transactionId)
                if seen_raw is not None:
                    if seen_raw.to_json_dict() != raw.to_json_dict():
                        raise BatchInferenceError(
                            f"conflicting duplicate transaction: {raw.transactionId}"
                        )
                    continue
            accepted_by_id[raw.transactionId] = candidate
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            publisher.publish_dlq(_dlq_event(record, "INVALID_JSON", str(exc), payload))
            dlq_count += 1
        except BatchInferenceError:
            raise
        except Exception as exc:
            publisher.publish_dlq(_dlq_event(record, "INVALID_SCHEMA", str(exc), payload))
            dlq_count += 1

    accepted = list(accepted_by_id.values())
    if not accepted:
        return 0, dlq_count

    # The adapter owns ordering for the model batch. Output mapping still uses
    # transactionId rather than relying on the engine's list order.
    accepted.sort(key=lambda item: (item.raw.step, item.raw.sourceRowIndex))
    predictions = engine.predict([item.model_input for item in accepted])
    predictions_by_id = _validate_predictions(accepted, predictions)

    for item in accepted:
        publisher.publish_prediction(predictions_by_id[item.raw.transactionId])

    if seen_transactions is not None:
        for item in accepted:
            seen_transactions.remember(item.raw)

    return len(accepted), dlq_count
