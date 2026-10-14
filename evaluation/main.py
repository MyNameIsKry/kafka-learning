"""CLI for offline model evaluation; labels never enter the Kafka consumer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .io import join_inputs, load_ground_truth, load_predictions
from .metrics import evaluate


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate scored predictions against offline PaySim labels")
    parser.add_argument(
        "--ground-truth",
        type=Path,
        required=True,
        help="offline CSV with transactionId,step,isFraud",
    )
    parser.add_argument("--predictions", type=Path, required=True, help="scored JSONL, not a Kafka topic dump with non-JSON metadata")
    parser.add_argument("--threshold", type=float, required=True, help="fixed threshold chosen outside this test report")
    parser.add_argument("--output", type=Path, help="optional JSON report path")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    records = join_inputs(load_ground_truth(args.ground_truth), load_predictions(args.predictions))
    report = evaluate(records, args.threshold).to_dict()
    document = {"evaluationType": "offline", "thresholdSource": "explicit-cli-argument", "report": report}
    rendered = json.dumps(document, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
