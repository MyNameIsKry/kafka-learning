"""Strict readers for offline labels and scored prediction JSONL files."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from contracts.events import parse_prediction

from .metrics import ScoredLabel


def _read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        rows = []
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at line {line_number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"JSONL line {line_number} must be an object")
            rows.append(value)
        return rows
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as stream:
            return list(csv.DictReader(stream))
    raise ValueError("supported files are .jsonl and .csv")


def load_ground_truth(path: Path) -> dict[str, int]:
    rows = _read_rows(path)
    result: dict[str, int] = {}
    for row in rows:
        transaction_id = str(row.get("transactionId", "")).strip()
        try:
            label = int(row["isFraud"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("ground truth requires integer isFraud") from exc
        if not transaction_id or label not in (0, 1) or transaction_id in result:
            raise ValueError("ground truth IDs must be unique and labels binary")
        result[transaction_id] = label
    if not result:
        raise ValueError("ground truth is empty")
    return result


def load_predictions(path: Path) -> dict[str, tuple[float, bool]]:
    result: dict[str, tuple[float, bool]] = {}
    for row in _read_rows(path):
        event = parse_prediction(row)
        if event.transactionId in result:
            raise ValueError(f"duplicate prediction: {event.transactionId}")
        result[event.transactionId] = (event.anomalyScore, event.isAnomaly)
    if not result:
        raise ValueError("predictions are empty")
    return result


def join_inputs(ground_truth: dict[str, int], predictions: dict[str, tuple[float, bool]]) -> list[ScoredLabel]:
    missing = sorted(set(ground_truth) - set(predictions))
    unexpected = sorted(set(predictions) - set(ground_truth))
    if missing or unexpected:
        raise ValueError(
            f"ground truth/prediction IDs do not match; missing={missing[:5]}, unexpected={unexpected[:5]}"
        )
    return [
        ScoredLabel(transaction_id=transaction_id, score=predictions[transaction_id][0],
                    label=ground_truth[transaction_id], published_is_anomaly=predictions[transaction_id][1])
        for transaction_id in sorted(ground_truth)
    ]
