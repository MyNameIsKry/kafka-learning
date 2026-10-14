"""Kafka publisher for prediction and DLQ events."""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from contracts.events import DLQEvent, TransactionPredictionEvent


class KafkaEventPublisher:
    """Publish an event and wait for Kafka delivery acknowledgement."""

    def __init__(self, bootstrap_servers: list[str]) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            retries=5,
            linger_ms=0,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8"),
        )

    def publish_prediction(self, prediction: TransactionPredictionEvent) -> None:
        self._publish(
            "transactions.scored",
            prediction.transactionId,
            prediction.to_json_dict(),
        )
        if prediction.isAnomaly:
            self._publish(
                "transactions.anomalies",
                prediction.transactionId,
                prediction.to_json_dict(),
            )

    def publish_dlq(self, event: DLQEvent) -> None:
        self._publish(
            "transactions.dlq",
            f"{event.sourceTopic}-{event.sourcePartition}-{event.sourceOffset}",
            event.to_json_dict(),
        )

    def _publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        future = self._producer.send(topic, key=key, value=value)
        future.get(timeout=30)

    def close(self) -> None:
        self._producer.flush(timeout=30)
        self._producer.close()
