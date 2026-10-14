"""Leakage-safe classification metrics for offline PaySim evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import isfinite
from typing import Iterable


@dataclass(frozen=True)
class ScoredLabel:
    transaction_id: str
    score: float
    label: int
    published_is_anomaly: bool | None = None


@dataclass(frozen=True)
class EvaluationReport:
    threshold: float
    sample_count: int
    positive_count: int
    negative_count: int
    predicted_positive_count: int
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    f1: float
    pr_auc: float | None
    published_flag_mismatches: int

    def to_dict(self) -> dict:
        return asdict(self)


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _average_precision(records: list[ScoredLabel]) -> float | None:
    positives = sum(record.label for record in records)
    if positives == 0:
        return None
    ordered = sorted(records, key=lambda record: (-record.score, record.transaction_id))
    seen_positive = 0
    precision_sum = 0.0
    for index, record in enumerate(ordered, start=1):
        if record.label:
            seen_positive += 1
            precision_sum += seen_positive / index
    return precision_sum / positives


def evaluate(records: Iterable[ScoredLabel], threshold: float) -> EvaluationReport:
    if not isfinite(threshold):
        raise ValueError("threshold must be finite")
    materialized = list(records)
    if not materialized:
        raise ValueError("evaluation requires at least one joined record")
    ids = [record.transaction_id for record in materialized]
    if any(not transaction_id for transaction_id in ids) or len(set(ids)) != len(ids):
        raise ValueError("evaluation records must have unique non-empty transaction IDs")
    if any(record.label not in (0, 1) or not isfinite(record.score) for record in materialized):
        raise ValueError("evaluation labels must be binary and scores finite")

    predicted = [record.score >= threshold for record in materialized]
    true_positive = sum(prediction and record.label == 1 for prediction, record in zip(predicted, materialized))
    false_positive = sum(prediction and record.label == 0 for prediction, record in zip(predicted, materialized))
    true_negative = sum(not prediction and record.label == 0 for prediction, record in zip(predicted, materialized))
    false_negative = sum(not prediction and record.label == 1 for prediction, record in zip(predicted, materialized))
    mismatches = sum(
        record.published_is_anomaly is not None and record.published_is_anomaly != prediction
        for record, prediction in zip(materialized, predicted)
    )
    precision = _ratio(true_positive, true_positive + false_positive)
    recall = _ratio(true_positive, true_positive + false_negative)
    return EvaluationReport(
        threshold=threshold,
        sample_count=len(materialized),
        positive_count=sum(record.label for record in materialized),
        negative_count=sum(record.label == 0 for record in materialized),
        predicted_positive_count=sum(predicted),
        true_positive=true_positive,
        false_positive=false_positive,
        true_negative=true_negative,
        false_negative=false_negative,
        precision=precision,
        recall=recall,
        f1=_ratio(2 * precision * recall, precision + recall),
        pr_auc=_average_precision(materialized),
        published_flag_mismatches=mismatches,
    )
