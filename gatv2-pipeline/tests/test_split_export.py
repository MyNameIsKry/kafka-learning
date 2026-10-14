import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from paysim_pipeline.schema import MODEL_INPUT_COLUMNS
from paysim_pipeline.stages import chronological_split, export
from support import paysim_record


class SplitAndExportTests(unittest.TestCase):
    def test_chronological_split_keeps_each_step_in_one_partition(self):
        frame = pd.DataFrame({
            "step": [step for step in range(1, 11) for _ in range(2)],
            "sourceRowIndex": range(20),
        })

        splits, boundaries = chronological_split.run(frame, train_fraction=0.6, validation_fraction=0.2)

        self.assertLess(splits["train"]["step"].max(), splits["validation"]["step"].min())
        self.assertLess(splits["validation"]["step"].max(), splits["test"]["step"].min())
        for partition in splits.values():
            self.assertTrue(partition.groupby("step").size().eq(2).all())
        self.assertEqual(boundaries["train_step_max"], 6)
        self.assertEqual(boundaries["validation_step_max"], 8)

    def test_chronological_split_rejects_too_few_distinct_steps(self):
        with self.assertRaisesRegex(ValueError, "At least three distinct step"):
            chronological_split.run(pd.DataFrame({"step": [1, 1, 2]}), 0.7, 0.15)

    def test_export_separates_safe_features_and_ground_truth(self):
        rows = [
            {"sourceRowIndex": source_row, **paysim_record(step=step, isFraud=source_row % 2)}
            for source_row, step in enumerate(range(1, 7))
        ]
        frame = pd.DataFrame(rows)
        splits, boundaries = chronological_split.run(frame, 0.5, 0.25)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.csv"
            source.write_text("paysim-source", encoding="utf-8")
            output = root / "artifacts"
            manifest = export.run(splits, frame.iloc[0:0], output, "test-dataset", source, boundaries)

            features = pd.read_parquet(output / "features/train.parquet")
            labels = pd.read_parquet(output / "ground_truth/train.parquet")
            feature_schema = json.loads((output / "feature_schema.json").read_text(encoding="utf-8"))
            manifest_file = json.loads((output / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(features.columns.tolist(), list(MODEL_INPUT_COLUMNS))
        forbidden = {"oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"}
        self.assertFalse(forbidden.intersection(features.columns))
        self.assertEqual(labels.columns.tolist(), ["transactionId", "sourceRowIndex", "step", "isFraud", "isFlaggedFraud"])
        self.assertEqual(features["transactionId"].tolist(), labels["transactionId"].tolist())
        self.assertEqual(len(manifest["sourceSha256"]), 64)
        self.assertEqual(feature_schema["datasetVersion"], "test-dataset")
        self.assertEqual(manifest_file["splitBoundaries"], boundaries)


if __name__ == "__main__":
    unittest.main()
