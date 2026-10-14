"""CLI for replaying PaySim CSV records to Kafka."""

from __future__ import annotations

import argparse
import csv
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol

from .kafka_publisher import KafkaRawPublisher
from .paysim import PreparedTransaction, prepare_paysim_csv, write_ground_truth

LOGGER = logging.getLogger("paysim-producer")


class RawPublisher(Protocol):
    def publish(self, event):  # type: ignore[no-untyped-def]
        ...


@dataclass
class ReplayStats:
    attempted: int = 0
    published: int = 0
    failed: int = 0


class ReplayPublishError(RuntimeError):
    """A delivery failure stopped ordered replay."""

    def __init__(self, transaction_id: str, stats: ReplayStats, cause: Exception) -> None:
        super().__init__(f"publish failed for {transaction_id}")
        self.transaction_id = transaction_id
        self.stats = stats
        self.__cause__ = cause


def replay(
    records: Iterable[PreparedTransaction],
    publisher: RawPublisher,
    *,
    ground_truth_path: str,
    events_per_second: float = 0,
    continue_on_error: bool = False,
) -> ReplayStats:
    """Persist labels and publish validated raw events in order.

    Delivery errors stop replay by default so a missing event cannot be hidden
    by later events. ``continue_on_error`` is an explicit recovery option.
    """

    if events_per_second < 0:
        raise ValueError("events_per_second must be zero or positive")

    stats = ReplayStats()
    next_send_at = time.monotonic()
    interval = 1 / events_per_second if events_per_second > 0 else 0
    output_path = Path(ground_truth_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None

    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as csv_file:
            temporary_path = Path(csv_file.name)
            writer = csv.DictWriter(
                csv_file,
                fieldnames=("transactionId", "step", "isFraud", "isFlaggedFraud"),
            )
            writer.writeheader()

            for record in records:
                stats.attempted += 1
                writer.writerow(record.ground_truth.to_json_dict())

                if interval:
                    delay = next_send_at - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    next_send_at = max(next_send_at + interval, time.monotonic())

                try:
                    publisher.publish(record.raw)
                except Exception as exc:
                    stats.failed += 1
                    if not continue_on_error:
                        raise ReplayPublishError(record.raw.transactionId, stats, exc) from exc
                    LOGGER.exception("publish failed for %s", record.raw.transactionId)
                    continue
                stats.published += 1

            csv_file.flush()
            os.fsync(csv_file.fileno())
        os.replace(temporary_path, output_path)
        return stats
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay PaySim CSV into Kafka")
    parser.add_argument("--csv", required=True, help="path to PaySim CSV")
    parser.add_argument(
        "--ground-truth-path",
        default="data/ground_truth.csv",
        help="offline label CSV output path",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions.raw")
    parser.add_argument("--events-per-second", type=float, default=0)
    parser.add_argument("--dataset-version")
    parser.add_argument("--max-events", type=int)
    parser.add_argument(
        "--sort-chunk-size",
        type=int,
        default=10_000,
        help="rows kept in memory per external-sort chunk",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="continue after a delivery failure; disabled by default",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate and write ground truth without connecting to Kafka",
    )
    return parser


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args()
    prepared = prepare_paysim_csv(
        args.csv,
        dataset_version=args.dataset_version,
        max_events=args.max_events,
        chunk_size=args.sort_chunk_size,
    )

    try:
        if args.dry_run:
            write_ground_truth(prepared.iter_records(), args.ground_truth_path)
            LOGGER.info("validated %d records; no Kafka publish (--dry-run)", prepared.count)
            return 0

        publisher = KafkaRawPublisher(args.bootstrap_servers.split(","), args.topic)
        try:
            stats = replay(
                prepared.iter_records(),
                publisher,
                ground_truth_path=args.ground_truth_path,
                events_per_second=args.events_per_second,
                continue_on_error=args.continue_on_error,
            )
        except ReplayPublishError as exc:
            LOGGER.error(
                "replay aborted at %s: attempted=%d published=%d failed=%d",
                exc.transaction_id,
                exc.stats.attempted,
                exc.stats.published,
                exc.stats.failed,
            )
            return 1
        finally:
            publisher.close()

        LOGGER.info(
            "replay finished: attempted=%d published=%d failed=%d",
            stats.attempted,
            stats.published,
            stats.failed,
        )
        return 0 if stats.failed == 0 else 1
    finally:
        prepared.close()


if __name__ == "__main__":
    raise SystemExit(main())
