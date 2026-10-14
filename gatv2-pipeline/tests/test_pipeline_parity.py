import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from contracts.events import ModelInputDTO
from paysim_pipeline.schema import BALANCE_COLUMNS, LABEL_COLUMNS, MODEL_INPUT_COLUMNS, assert_model_input_columns
from paysim_pipeline.stages import chronological_split, export, validate_clean


class PipelineParityTests(unittest.TestCase):
    def test_offline_columns_match_online_model_input_dto(self) -> None:
        self.assertEqual(tuple(ModelInputDTO.model_fields), MODEL_INPUT_COLUMNS)
        assert_model_input_columns(ModelInputDTO.model_fields)
        self.assertTrue(set(BALANCE_COLUMNS).isdisjoint(MODEL_INPUT_COLUMNS))
        self.assertTrue(set(LABEL_COLUMNS).isdisjoint(MODEL_INPUT_COLUMNS))

    def test_validation_quarantines_bad_rows_without_balance_consistency_rule(self) -> None:
        frame = pd.DataFrame([
            {"sourceRowIndex": 0, "step": 1, "type": "PAYMENT", "amount": 1.0, "nameOrig": "C1", "nameDest": "M1", "oldbalanceOrg": 100.0, "newbalanceOrig": 999.0, "oldbalanceDest": 0.0, "newbalanceDest": 1.0, "isFraud": 0, "isFlaggedFraud": 0},
            {"sourceRowIndex": 1, "step": 0, "type": "UNKNOWN", "amount": -1.0, "nameOrig": "", "nameDest": "M2", "oldbalanceOrg": 0.0, "newbalanceOrig": 0.0, "oldbalanceDest": 0.0, "newbalanceDest": 0.0, "isFraud": 2, "isFlaggedFraud": 0},
        ])
        accepted, rejected = validate_clean.run(frame)
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 1)
        self.assertIn("step:invalid_value", rejected.iloc[0]["rejectReason"])
        self.assertNotIn("balance", rejected.iloc[0]["rejectReason"])

    def test_validation_rejects_negative_balances_without_consistency_rule(self) -> None:
        frame = pd.DataFrame([{
            "sourceRowIndex": 0, "step": 1, "type": "PAYMENT", "amount": 1.0,
            "nameOrig": "C1", "nameDest": "M1", "oldbalanceOrg": -1.0,
            "newbalanceOrig": 0.0, "oldbalanceDest": 0.0, "newbalanceDest": 1.0,
            "isFraud": 0, "isFlaggedFraud": 0,
        }])
        accepted, rejected = validate_clean.run(frame)
        self.assertTrue(accepted.empty)
        self.assertIn("oldbalanceOrg:invalid_value", rejected.iloc[0]["rejectReason"])

    def test_export_manifest_and_artifacts_are_parity_safe(self) -> None:
        rows = [{"sourceRowIndex": step - 1, "step": step, "type": "TRANSFER", "amount": float(step), "nameOrig": f"C{step}", "nameDest": f"C{step + 1}", "oldbalanceOrg": 100.0, "newbalanceOrig": 90.0, "oldbalanceDest": 0.0, "newbalanceDest": 10.0, "isFraud": step == 6, "isFlaggedFraud": 0} for step in range(1, 7)]
        accepted, rejected = validate_clean.run(pd.DataFrame(rows))
        splits, boundaries = chronological_split.run(accepted, .5, .25)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.csv"
            source.write_text("source-for-hash", encoding="utf-8")
            output = Path(directory) / "artifacts"
            manifest = export.run(splits, rejected, output, "test-v1", source, boundaries)
            schema = json.loads((output / "feature_schema.json").read_text(encoding="utf-8"))
            self.assertEqual(schema["modelInputColumns"], list(MODEL_INPUT_COLUMNS))
            self.assertEqual(manifest["rowsInput"], 6)
            self.assertEqual(manifest["rowsAccepted"], 6)
            self.assertEqual(manifest["rowsRejected"], 0)
            features = pd.read_parquet(output / "features/train.parquet")
            self.assertEqual(tuple(features.columns), MODEL_INPUT_COLUMNS)
            self.assertTrue(set(BALANCE_COLUMNS + LABEL_COLUMNS).isdisjoint(features.columns))

    def test_split_manifest_uses_actual_step_when_steps_have_gaps(self) -> None:
        frame = pd.DataFrame([
            {"sourceRowIndex": index, "step": step}
            for index, step in enumerate((1, 2, 10, 20))
        ])
        splits, boundaries = chronological_split.run(frame, .5, .25)
        self.assertEqual(boundaries["test_step_min"], int(splits["test"]["step"].min()))
        self.assertEqual(boundaries["test_step_min"], 20)

    def test_split_rejects_invalid_fractions(self) -> None:
        frame = pd.DataFrame({"step": [1, 2, 3]})
        with self.assertRaisesRegex(ValueError, "fractions"):
            chronological_split.run(frame, 0.7, 0.3)

    def test_export_removes_stale_rejected_artifact(self) -> None:
        rows = [{"sourceRowIndex": index, "step": step, "type": "TRANSFER", "amount": 1.0,
                 "nameOrig": f"C{index}", "nameDest": f"C{index + 1}", "oldbalanceOrg": 1.0,
                 "newbalanceOrig": 0.0, "oldbalanceDest": 0.0, "newbalanceDest": 1.0,
                 "isFraud": 0, "isFlaggedFraud": 0} for index, step in enumerate((1, 2, 3, 4, 5, 6))]
        accepted, rejected = validate_clean.run(pd.DataFrame(rows))
        splits, boundaries = chronological_split.run(accepted, .5, .25)
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.csv"
            source.write_text("source-for-hash", encoding="utf-8")
            output = Path(directory) / "artifacts"
            rejected_frame = rejected.copy()
            rejected_frame = pd.concat([rejected_frame, pd.DataFrame([rows[0]])], ignore_index=True)
            rejected_frame["rejectReason"] = "test:reject;"
            manifest = export.run(splits, rejected_frame, output, "test-v1", source, boundaries)
            self.assertTrue((output / "rejected.parquet").exists())
            self.assertIn("rejected.parquet", manifest["artifacts"])
            export.run(splits, rejected, output, "test-v1", source, boundaries)
            self.assertFalse((output / "rejected.parquet").exists())


if __name__ == "__main__":
    unittest.main()
