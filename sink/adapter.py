"""Validation and persistence adapter for MongoDB sink consumers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

from contracts.events import (
    DLQEvent,
    RawTransactionEvent,
    TransactionPredictionEvent,
    parse_prediction,
    parse_raw_transaction,
)
from consumer.adapter import ConsumerRecord
from runtime.payload import undecoded_payload

SinkKind = Literal["raw", "prediction", "alert"]


class SinkStore(Protocol):
    def upsert_raw(self, event: RawTransactionEvent) -> None:
        ...

    def upsert_prediction(self, event: TransactionPredictionEvent) -> None:
        ...

    def upsert_alert(self, event: TransactionPredictionEvent) -> None:
        ...


class DLQPublisher(Protocol):
    def publish_dlq(self, event: DLQEvent) -> None:
        ...


def _decode_payload(record: ConsumerRecord) -> Any:
    return json.loads(record.value.decode("utf-8"))


def _dlq_event(record: ConsumerRecord, code: str, message: str, payload: Any) -> DLQEvent:
    original_payload = payload if isinstance(payload, dict) else {"raw": payload}
    return DLQEvent(
        sourceTopic=record.topic,
        sourcePartition=record.partition,
        sourceOffset=record.offset,
        errorCode=code,
        errorMessage=message,
        failedAt=datetime.now(timezone.utc),
        originalPayload=original_payload,
    )


def process_batch(
    records: list[ConsumerRecord],
    kind: SinkKind,
    store: SinkStore,
    dlq_publisher: DLQPublisher,
    metrics=None,
) -> tuple[int, int]:
    """Persist a batch; database errors propagate so offsets stay uncommitted."""

    stored = 0
    dlq_count = 0
    for record in records:
        payload: Any = undecoded_payload(record.value)
        try:
            payload = _decode_payload(record)
            if kind == "raw":
                event = parse_raw_transaction(payload)
            else:
                event = parse_prediction(payload)
                if kind == "alert":
                    if not event.isAnomaly:
                        raise ValueError("anomaly topic event must have isAnomaly=true")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            dlq_publisher.publish_dlq(_dlq_event(record, "INVALID_JSON", str(exc), payload))
            dlq_count += 1
            continue
        except (ValueError, TypeError) as exc:
            dlq_publisher.publish_dlq(_dlq_event(record, "INVALID_SCHEMA", str(exc), payload))
            dlq_count += 1
            continue

        # Only decode/validation failures belong in the schema DLQ. Persistence
        # failures must reach the runtime retry loop without committing input.
        if kind == "raw":
            store.upsert_raw(event)
        elif kind == "prediction":
            store.upsert_prediction(event)
        else:
            store.upsert_alert(event)
        stored += 1
        if metrics is not None:
            metrics.stored.inc()
    return stored, dlq_count
