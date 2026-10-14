"""Kafka publisher for prediction and DLQ events."""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from contracts.events import DLQEvent, TransactionPredictionEvent


class KafkaEventPublisher:
    """Publish an event and wait for Kafka delivery acknowledgement."""

    def __init__(self, bootstrap_servers: list[str], *, metrics=None,
                 scored_topic="transactions.scored", anomalies_topic="transactions.anomalies",
                 dlq_topic="transactions.dlq") -> None:
        self.metrics = metrics
        self.scored_topic = scored_topic
        self.anomalies_topic = anomalies_topic
        self.dlq_topic = dlq_topic
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
            self.scored_topic,
            prediction.transactionId,
            prediction.to_json_dict(),
        )
        if prediction.isAnomaly:
            self._publish(
                self.anomalies_topic,
                prediction.transactionId,
                prediction.to_json_dict(),
            )

    def publish_dlq(self, event: DLQEvent) -> None:
        self._publish(
            self.dlq_topic,
            f"{event.sourceTopic}-{event.sourcePartition}-{event.sourceOffset}",
            event.to_json_dict(),
        )

    def _publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        future = self._producer.send(topic, key=key, value=value)
        future.get(timeout=30)
        if self.metrics is not None:
            self.metrics.published_record(topic)

    def close(self) -> None:
        self._producer.flush(timeout=30)
        self._producer.close()
