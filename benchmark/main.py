"""Measure target versus achieved producer rates and raw-send to scored latency."""

import argparse
import csv
import json
import math
import platform
import time
import uuid
from pathlib import Path

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from contracts.events import parse_prediction
from producer.kafka_publisher import KafkaRawPublisher
from producer.main import replay
from producer.paysim import PAYSIM_COLUMNS, prepare_paysim_csv


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def latency_summary(values: list[float]) -> dict:
    return {"samples": len(values), "p50": percentile(values, .5),
            "p95": percentile(values, .95), "p99": percentile(values, .99)}


class TrackingPublisher:
    def __init__(self, publisher):
        self.publisher = publisher
        self.sent_at_ms = {}
        self.delivery_seconds = []

    def publish(self, event):
        self.sent_at_ms[event.transactionId] = time.time() * 1000
        started = time.monotonic()
        self.publisher.publish(event)
        self.delivery_seconds.append(time.monotonic() - started)


def synthetic_csv(path: Path, count: int) -> None:
    """Synthetic wiring data only; never presented as real PaySim experiment data."""
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=PAYSIM_COLUMNS)
        writer.writeheader()
        for index in range(count):
            writer.writerow(dict(step=1 + index // 100, type="TRANSFER", amount=10,
                nameOrig=f"C{index % 100}", nameDest=f"C{(index + 1) % 100}",
                oldbalanceOrg=100, newbalanceOrig=90, oldbalanceDest=0,
                newbalanceDest=10, isFraud=0, isFlaggedFraud=0))


def run_scenario(args, directory: Path, run_id: str, rate: int) -> dict:
    version = f"bench-{run_id}-{rate}"
    source = Path(args.csv) if args.csv else directory / "synthetic.csv"
    with prepare_paysim_csv(source, dataset_version=version, max_events=args.max_events) as prepared:
        if prepared.count == 0:
            raise ValueError("benchmark requires a non-empty dataset")
        observer = KafkaConsumer(bootstrap_servers=args.bootstrap_servers.split(","),
            enable_auto_commit=False, group_id=None, auto_offset_reset="latest")
        publisher = None
        try:
            partitions = observer.partitions_for_topic(args.scored_topic)
            if not partitions:
                raise ValueError("scored topic must exist before benchmarking")
            tps = [TopicPartition(args.scored_topic, p) for p in sorted(partitions)]
            observer.assign(tps)
            observer.seek_to_end(*tps)
            # Resolve positions before any sends so the observer cannot skip this run.
            for tp in tps:
                observer.position(tp)
            publisher = KafkaRawPublisher(args.bootstrap_servers.split(","), args.raw_topic)
            tracking = TrackingPublisher(publisher)
            started = time.monotonic()
            stats = replay(prepared.iter_records(), tracking,
                ground_truth_path=str(directory / f"ground-truth-{rate}.csv"), events_per_second=rate)
            producer_seconds = time.monotonic() - started
            deadline = time.monotonic() + args.drain_timeout
            seen = set()
            latencies = []
            invalid_timestamps = 0
            while len(seen) < stats.published and time.monotonic() < deadline:
                polled = observer.poll(timeout_ms=250, max_records=1000)
                for messages in polled.values():
                    for message in messages:
                        payload = json.loads(message.value)
                        transaction_id = payload.get("transactionId")
                        if transaction_id not in tracking.sent_at_ms or transaction_id in seen:
                            continue
                        prediction = parse_prediction(payload)
                        if prediction.modelVersion != args.model_version:
                            raise ValueError("benchmark prediction modelVersion mismatch")
                        seen.add(transaction_id)
                        if message.timestamp is None or message.timestamp < 0:
                            invalid_timestamps += 1
                        else:
                            latency_ms = message.timestamp - tracking.sent_at_ms[transaction_id]
                            if latency_ms < 0:
                                invalid_timestamps += 1
                                continue
                            latencies.append(latency_ms / 1000)
            return dict(target_events_per_second=rate, published=stats.published,
                producer_seconds=producer_seconds,
                achieved_producer_events_per_second=stats.published / producer_seconds,
                observed_scored_events_per_second=len(seen) / (time.monotonic() - started),
                scored_received=len(seen), missing_scored=stats.published - len(seen),
                complete=len(seen) == stats.published, invalid_timestamp_samples=invalid_timestamps,
                delivery_seconds=latency_summary(tracking.delivery_seconds),
                raw_send_to_scored_publication_seconds=latency_summary(latencies))
        finally:
            observer.close()
            if publisher is not None:
                publisher.close()


def build_parser():
    parser = argparse.ArgumentParser(description="Benchmark an already running isolated mock pipeline")
    parser.add_argument("--csv", help="real PaySim CSV; omitted means synthetic wiring data")
    parser.add_argument("--raw-topic", required=True)
    parser.add_argument("--scored-topic", required=True)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--rates", type=int, nargs="+", default=[100, 500, 1000])
    parser.add_argument("--max-events", type=int, default=1000)
    parser.add_argument("--drain-timeout", type=float, default=60)
    parser.add_argument("--model-version", default="mock-v1")
    parser.add_argument("--output-dir", default="benchmark-results")
    return parser


def main():
    args = build_parser().parse_args()
    if not 1 <= args.max_events <= 100_000 or any(rate <= 0 for rate in args.rates):
        raise ValueError("max-events must be 1..100000 and rates positive")
    if not math.isfinite(args.drain_timeout) or args.drain_timeout <= 0 or len(set(args.rates)) != len(args.rates):
        raise ValueError("drain-timeout must be finite and positive; rates must be unique")
    run_id = uuid.uuid4().hex[:12]
    directory = Path(args.output_dir) / run_id
    directory.mkdir(parents=True, exist_ok=False)
    if not args.csv:
        synthetic_csv(directory / "synthetic.csv", args.max_events)
    report = dict(run_id=run_id, dataset="paysim" if args.csv else "synthetic_wiring",
        source=args.csv, model_version=args.model_version, max_events=args.max_events,
        python=platform.python_version(), platform=platform.platform(), processor=platform.processor(),
        raw_topic=args.raw_topic, scored_topic=args.scored_topic,
        latency_definition="local raw send wall clock to scored Kafka record timestamp; excludes DB persistence; requires synchronized clocks",
        scenarios=[])
    try:
        for rate in args.rates:
            scenario = run_scenario(args, directory, run_id, rate)
            report["scenarios"].append(scenario)
            print(json.dumps(scenario), flush=True)
    finally:
        (directory / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Report: {directory / 'report.json'}")
    return 0 if all(s["complete"] for s in report["scenarios"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
