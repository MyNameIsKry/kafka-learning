"""Kafka publisher with delivery acknowledgement for raw PaySim events."""

from __future__ import annotations

import json
from typing import Any

from kafka import KafkaProducer

from contracts.events import RawTransactionEvent


class KafkaRawPublisher:
    """Publish one raw event and wait for its broker acknowledgement."""

    def __init__(self, bootstrap_servers: list[str], topic: str = "transactions.raw") -> None:
        self.topic = topic
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

    def publish(self, event: RawTransactionEvent) -> Any:
        future = self._producer.send(
            self.topic,
            key=event.nameOrig,
            value=event.to_json_dict(),
        )
        return future.get(timeout=30)

    def close(self) -> None:
        self._producer.flush(timeout=30)
        self._producer.close()
