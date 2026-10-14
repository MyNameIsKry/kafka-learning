import tempfile
import unittest
from pathlib import Path

import pandas as pd

from paysim_pipeline.schema import PAYSIM_COLUMNS
from paysim_pipeline.stages import load, validate_clean
from support import paysim_record


class LoadAndCleanTests(unittest.TestCase):
    def test_load_assigns_stable_source_row_index_and_trims_headers(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "paysim.csv"
            pd.DataFrame([paysim_record(), paysim_record(step=2)]).rename(
                columns={column: f" {column} " for column in PAYSIM_COLUMNS}
            ).to_csv(csv_path, index=False)

            result = load.run(csv_path)

        self.assertEqual(result.columns.tolist(), ["sourceRowIndex", *PAYSIM_COLUMNS])
        self.assertEqual(result["sourceRowIndex"].tolist(), [0, 1])
        self.assertEqual(result["step"].tolist(), [1, 2])

    def test_load_rejects_missing_or_extra_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "bad.csv"
            pd.DataFrame([paysim_record()]).assign(unexpected=1).to_csv(csv_path, index=False)
            with self.assertRaisesRegex(ValueError, "Unexpected PaySim schema"):
                load.run(csv_path)

    def test_load_rejects_duplicate_columns_after_trimming(self):
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "duplicate.csv"
            csv_path.write_text(
                "step, step,type,amount,nameOrig,nameDest,oldbalanceOrg,newbalanceOrig,"
                "oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate column"):
                load.run(csv_path)

    def test_clean_quarantines_invalid_rows_and_keeps_balances(self):
        frame = pd.DataFrame([
            {"sourceRowIndex": 0, **paysim_record(oldbalanceOrg=999.0)},
            {"sourceRowIndex": 1, **paysim_record(type="UNKNOWN")},
            {"sourceRowIndex": 2, **paysim_record(amount=-1.0)},
            {"sourceRowIndex": 3, **paysim_record(amount=float("inf"))},
            {"sourceRowIndex": 4, **paysim_record(isFraud=0.5)},
        ])

        accepted, rejected = validate_clean.run(frame)

        self.assertEqual(accepted["sourceRowIndex"].tolist(), [0])
        self.assertEqual(accepted.loc[0, "oldbalanceOrg"], 999.0)
        self.assertEqual(rejected["sourceRowIndex"].tolist(), [1, 2, 3, 4])
        self.assertIn("type:invalid_value", rejected.loc[1, "rejectReason"])
        self.assertIn("amount:invalid_value", rejected.loc[2, "rejectReason"])
        self.assertIn("amount:missing_or_non_finite", rejected.loc[3, "rejectReason"])
        self.assertIn("isFraud_integer", rejected.loc[4, "rejectReason"])


if __name__ == "__main__":
    unittest.main()
