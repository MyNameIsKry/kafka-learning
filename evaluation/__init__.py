"""Offline evaluation of scored predictions against PaySim ground truth."""

from .metrics import EvaluationReport, evaluate

__all__ = ["EvaluationReport", "evaluate"]
