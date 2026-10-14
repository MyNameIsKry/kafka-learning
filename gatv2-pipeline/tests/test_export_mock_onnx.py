import tempfile
import unittest
from pathlib import Path

import onnx

from export_mock_onnx import FEATURES, create_mock_model


class MockONNXExportTests(unittest.TestCase):
    def test_exports_checkable_model_with_dynamic_batch_and_fixed_features(self):
        with tempfile.TemporaryDirectory() as directory:
            path = create_mock_model(Path(directory) / "mock.onnx")
            model = onnx.load(path)

        onnx.checker.check_model(model)
        self.assertEqual(model.graph.input[0].name, "features")
        dimensions = [dimension.dim_param or dimension.dim_value for dimension in model.graph.input[0].type.tensor_type.shape.dim]
        self.assertEqual(dimensions, ["batch", len(FEATURES)])
        self.assertEqual(model.graph.output[0].name, "scores")
        self.assertEqual([node.op_type for node in model.graph.node], ["MatMul", "Add", "Sigmoid"])


if __name__ == "__main__":
    unittest.main()
