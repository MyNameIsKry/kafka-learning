"""Manual-commit Kafka consumer for the inference adapter."""

from __future__ import annotations

import argparse
import logging
import time

from kafka import KafkaConsumer

from .adapter import SeenTransactionCache, process_batch
from .publisher import KafkaEventPublisher
from inference.mock_engine import MockInferenceEngine
from runtime.kafka import collect_batch, commit_batch

LOGGER = logging.getLogger("inference-consumer")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the PaySim inference consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions.raw")
    parser.add_argument("--group-id", default="gatv2-inference-group")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--max-wait-ms", type=int, default=250)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--dedupe-cache-size", type=int, default=100_000)
    parser.add_argument("--mock-score", type=float, default=0.1)
    parser.add_argument("--mock-threshold", type=float, default=0.8)
    parser.add_argument("--model-version", default="mock-v1")
    return parser


def run(args: argparse.Namespace) -> int:
    if args.max_retries < 0:
        raise ValueError("max_retries must be non-negative")
    if args.batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if args.max_wait_ms < 0:
        raise ValueError("max_wait_ms must be non-negative")
    if args.dedupe_cache_size <= 0:
        raise ValueError("dedupe_cache_size must be positive")

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers.split(","),
        group_id=args.group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=args.batch_size,
        value_deserializer=lambda value: value,
    )
    publisher = KafkaEventPublisher(args.bootstrap_servers.split(","))
    engine = MockInferenceEngine(
        score=args.mock_score,
        threshold=args.mock_threshold,
        model_version=args.model_version,
    )

    try:
        seen_transactions = SeenTransactionCache(args.dedupe_cache_size)
        while True:
            records = collect_batch(consumer, args.batch_size, args.max_wait_ms)
            if not records:
                continue

            attempts = 0
            while True:
                try:
                    accepted, dlq_count = process_batch(
                        records,
                        engine,
                        publisher,
                        seen_transactions,
                    )
                    commit_batch(consumer, records)
                    LOGGER.info(
                        "batch complete: input=%d accepted=%d dlq=%d",
                        len(records),
                        accepted,
                        dlq_count,
                    )
                    break
                except Exception:
                    attempts += 1
                    if attempts > args.max_retries:
                        LOGGER.exception("batch failed after %d retries; stopping without commit", attempts - 1)
                        return 1
                    delay = min(2**attempts, 30)
                    LOGGER.exception("batch failed; retry %d/%d in %ds", attempts, args.max_retries, delay)
                    time.sleep(delay)
    except KeyboardInterrupt:
        LOGGER.info("consumer stopped")
        return 0
    finally:
        consumer.close()
        publisher.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
