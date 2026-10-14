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


def _strict_integer(value: Any, field: str) -> int:
    """Parse CSV integer strings without coercing JSON floats or booleans."""
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError as exc:
            raise ValueError(f"{field} must be an integer") from exc
    raise ValueError(f"{field} must be an integer")


def load_ground_truth(path: Path) -> dict[str, tuple[int, int]]:
    rows = _read_rows(path)
    result: dict[str, tuple[int, int]] = {}
    for row in rows:
        unknown_fields = set(row) - {"transactionId", "step", "isFraud", "isFlaggedFraud"}
        if None in row or unknown_fields:
            raise ValueError(f"ground truth contains unsupported fields: {sorted(unknown_fields, key=str)}")
        raw_transaction_id = row.get("transactionId", "")
        if not isinstance(raw_transaction_id, str):
            raise ValueError("ground truth transactionId must be a string")
        transaction_id = raw_transaction_id.strip()
        try:
            step = _strict_integer(row["step"], "step")
            label = _strict_integer(row["isFraud"], "isFraud")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("ground truth requires integer step and isFraud") from exc
        if not transaction_id or step < 1 or label not in (0, 1) or transaction_id in result:
            raise ValueError("ground truth IDs must be unique, steps positive and labels binary")
        result[transaction_id] = (step, label)
    if not result:
        raise ValueError("ground truth is empty")
    return result


def load_predictions(path: Path) -> dict[str, tuple[int, float, bool]]:
    result: dict[str, tuple[int, float, bool]] = {}
    for row in _read_rows(path):
        event = parse_prediction(row)
        if event.transactionId in result:
            raise ValueError(f"duplicate prediction: {event.transactionId}")
        result[event.transactionId] = (event.step, event.anomalyScore, event.isAnomaly)
    if not result:
        raise ValueError("predictions are empty")
    return result


def join_inputs(ground_truth: dict[str, tuple[int, int]], predictions: dict[str, tuple[int, float, bool]]) -> list[ScoredLabel]:
    missing = sorted(set(ground_truth) - set(predictions))
    unexpected = sorted(set(predictions) - set(ground_truth))
    if missing or unexpected:
        raise ValueError(
            f"ground truth/prediction IDs do not match; missing={missing[:5]}, unexpected={unexpected[:5]}"
        )
    mismatched_steps = sorted(
        transaction_id for transaction_id in ground_truth
        if ground_truth[transaction_id][0] != predictions[transaction_id][0]
    )
    if mismatched_steps:
        raise ValueError(f"ground truth/prediction steps do not match: {mismatched_steps[:5]}")
    return [
        ScoredLabel(transaction_id=transaction_id, score=predictions[transaction_id][1],
                    label=ground_truth[transaction_id][1], published_is_anomaly=predictions[transaction_id][2])
        for transaction_id in sorted(ground_truth)
    ]
