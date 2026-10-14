"""Inference engine interfaces and the Phase 4 mock implementation."""

from .interface import InferenceEngine
from .mock_engine import MockInferenceEngine

__all__ = ["InferenceEngine", "MockInferenceEngine"]
