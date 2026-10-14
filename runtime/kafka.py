"""Shared polling and manual-commit helpers for Kafka consumers."""

from __future__ import annotations

from time import monotonic

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

from consumer.adapter import ConsumerRecord


def collect_batch(consumer: KafkaConsumer, batch_size: int, max_wait_ms: int) -> list[ConsumerRecord]:
    if batch_size <= 0 or max_wait_ms < 0:
        raise ValueError("batch_size must be positive and max_wait_ms non-negative")

    records: list[ConsumerRecord] = []
    deadline = monotonic() + max_wait_ms / 1000
    while len(records) < batch_size:
        remaining_ms = max(0, int((deadline - monotonic()) * 1000))
        polled = consumer.poll(
            timeout_ms=remaining_ms,
            max_records=batch_size - len(records),
        )
        for topic_partition, messages in polled.items():
            for message in messages:
                records.append(
                    ConsumerRecord(
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        offset=message.offset,
                        value=message.value,
                        timestamp_ms=message.timestamp,
                    )
                )
        if not polled or max_wait_ms == 0 or monotonic() >= deadline:
            break

    records.sort(key=lambda record: (record.partition, record.offset))
    return records


def commit_batch(consumer: KafkaConsumer, records: list[ConsumerRecord]) -> None:
    offsets: dict[TopicPartition, OffsetAndMetadata] = {}
    for record in records:
        topic_partition = TopicPartition(record.topic, record.partition)
        next_offset = record.offset + 1
        current = offsets.get(topic_partition)
        if current is None or next_offset > current.offset:
            offsets[topic_partition] = OffsetAndMetadata(next_offset, None, -1)
    if offsets:
        consumer.commit(offsets=offsets)


def seek_batch_start(consumer: KafkaConsumer, records: list[ConsumerRecord]) -> None:
    starts: dict[TopicPartition, int] = {}
    for record in records:
        topic_partition = TopicPartition(record.topic, record.partition)
        starts[topic_partition] = min(starts.get(topic_partition, record.offset), record.offset)
    for topic_partition, offset in starts.items():
        consumer.seek(topic_partition, offset)
