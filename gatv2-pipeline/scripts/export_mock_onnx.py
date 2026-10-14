"""Build a deterministic ONNX mock for wiring Kafka to Go inference."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import numpy as np
import onnx
from onnx import TensorProto, helper, numpy_helper


FEATURES = (
    "log1p_amount",
    "step_fraction",
    "type_cash_in",
    "type_cash_out",
    "type_debit",
    "type_payment",
    "type_transfer",
)


def create_mock_model(output_path: Path) -> Path:
    """Write a mock scoring graph with illustrative, untrained weights."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    input_info = helper.make_tensor_value_info("features", TensorProto.FLOAT, ["batch", len(FEATURES)])
    output_info = helper.make_tensor_value_info("scores", TensorProto.FLOAT, ["batch", 1])
    weights = np.asarray([[0.9], [0.1], [0.0], [0.1], [0.0], [0.0], [0.6]], dtype=np.float32)
    bias = np.asarray([-4.0], dtype=np.float32)
    graph = helper.make_graph(
        [
            helper.make_node("MatMul", ["features", "weights"], ["linear"]),
            helper.make_node("Add", ["linear", "bias"], ["logits"]),
            helper.make_node("Sigmoid", ["logits"], ["scores"]),
        ],
        "paysim_mock_score",
        [input_info],
        [output_info],
        initializer=[numpy_helper.from_array(weights, "weights"), numpy_helper.from_array(bias, "bias")],
    )
    model = helper.make_model(graph, producer_name="kafka-learning-mock", opset_imports=[helper.make_opsetid("", 17)])
    model.ir_version = 10
    onnx.checker.check_model(model)
    try:
        onnx.save_model(model, temporary_path)
        os.replace(temporary_path, output_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("gatv2-pipeline/artifacts/mock/anomaly_score.onnx"))
    args = parser.parse_args()
    print(create_mock_model(args.output.resolve()))


if __name__ == "__main__":
    main()
