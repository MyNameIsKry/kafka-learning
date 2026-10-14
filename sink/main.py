"""Run one of the MongoDB sink consumer groups."""

from __future__ import annotations

import argparse
import logging
import time

from kafka import KafkaConsumer

from consumer.publisher import KafkaEventPublisher
from runtime.kafka import collect_batch, commit_batch
from .adapter import SinkKind, process_batch
from .mongo_store import MongoStore

LOGGER = logging.getLogger("mongo-sink")

KIND_CONFIG: dict[SinkKind, tuple[str, str]] = {
    "raw": ("transactions.raw", "mongodb-raw-sink-group"),
    "prediction": ("transactions.scored", "mongodb-sink-group"),
    "alert": ("transactions.anomalies", "alert-group"),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a PaySim MongoDB sink")
    parser.add_argument("--kind", choices=KIND_CONFIG, required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--mongo-db", default="financial_anomaly")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-wait-ms", type=int, default=250)
    parser.add_argument("--max-retries", type=int, default=3)
    return parser


def run(args: argparse.Namespace) -> int:
    if args.max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if args.batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if args.max_wait_ms < 0:
        raise ValueError("max_wait_ms must be non-negative")

    topic, group_id = KIND_CONFIG[args.kind]
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=args.bootstrap_servers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=args.batch_size,
        value_deserializer=lambda value: value,
    )
    dlq_publisher = KafkaEventPublisher(args.bootstrap_servers.split(","))
    store: MongoStore | None = None

    try:
        connection_attempts = 0
        while store is None:
            try:
                store = MongoStore(args.mongo_uri, args.mongo_db)
            except Exception:
                connection_attempts += 1
                if connection_attempts > args.max_retries:
                    LOGGER.exception(
                        "unable to connect to MongoDB after %d retries; stopping",
                        connection_attempts - 1,
                    )
                    return 1
                delay = min(2**connection_attempts, 30)
                LOGGER.exception(
                    "MongoDB connection failed; retry %d/%d in %ds",
                    connection_attempts,
                    args.max_retries,
                    delay,
                )
                time.sleep(delay)

        while True:
            records = collect_batch(consumer, args.batch_size, args.max_wait_ms)
            if not records:
                continue

            attempts = 0
            while True:
                try:
                    stored, dlq_count = process_batch(records, args.kind, store, dlq_publisher)
                    commit_batch(consumer, records)
                    LOGGER.info(
                        "%s sink batch complete: input=%d stored=%d dlq=%d",
                        args.kind,
                        len(records),
                        stored,
                        dlq_count,
                    )
                    break
                except Exception:
                    attempts += 1
                    if attempts > args.max_retries:
                        LOGGER.exception(
                            "%s sink failed after %d retries; stopping without commit",
                            args.kind,
                            attempts - 1,
                        )
                        return 1
                    delay = min(2**attempts, 30)
                    LOGGER.exception(
                        "%s sink failed; retry %d/%d in %ds",
                        args.kind,
                        attempts,
                        args.max_retries,
                        delay,
                    )
                    time.sleep(delay)
    except KeyboardInterrupt:
        LOGGER.info("%s sink stopped", args.kind)
        return 0
    finally:
        consumer.close()
        dlq_publisher.close()
        if store is not None:
            store.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
