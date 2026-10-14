import json
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from paysim_pipeline.schema import PAYSIM_COLUMNS
from paysim_pipeline.stages import eda
from paysim_pipeline import cli
from support import paysim_record


class EdaTests(unittest.TestCase):
    def test_preclean_eda_writes_report_without_mutating_source(self):
        frame = pd.DataFrame([
            paysim_record(step=2, type="PAYMENT", isFraud=0),
            paysim_record(step=1, amount="not-a-number", type="UNKNOWN", isFraud=1),
        ])
        original = frame.copy(deep=True)

        with tempfile.TemporaryDirectory() as directory:
            result = eda.run(frame, Path(directory), "paysim-test", "source.csv")
            report = json.loads(Path(result["reportPath"]).read_text(encoding="utf-8"))

        self.assertTrue(frame.equals(original))
        self.assertEqual(report["stage"], "before_validate_clean")
        self.assertEqual(report["rows"], 2)
        self.assertEqual(report["sourceRowOrderStepDecreases"], 1)
        self.assertEqual(report["columns"]["amount"]["parse_failed_count"], 1)
        self.assertEqual(report["columns"]["type"]["unexpected_type_count"], 1)
        self.assertEqual(report["labelUsage"]["counts"]["isFraud"], {"0": 1, "1": 1})
        self.assertIn("Full pre-clean dataset", report["labelUsage"]["scope"])
        self.assertIn("Do not use full-dataset labels", report["labelUsage"]["restriction"])
        self.assertTrue(set(PAYSIM_COLUMNS).issubset(report["columns"]))

    def test_cli_runs_eda_before_cleaning_and_saves_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "configs").mkdir()
            (root / "data").mkdir()
            source = root / "data" / "input.csv"
            pd.DataFrame([
                {column: value for column, value in paysim_record(step=step).items()}
                for step in range(1, 7)
            ], columns=PAYSIM_COLUMNS).to_csv(source, index=False)
            config = root / "configs" / "pipeline.toml"
            config.write_text(
                '[dataset]\ninput_csv = "data/input.csv"\noutput_dir = "artifacts"\ndataset_version = "v1"\n'
                '[split]\ntrain_fraction = 0.5\nvalidation_fraction = 0.25\ntest_fraction = 0.25\n'
                '[pipeline]\nstages = ["load", "eda", "validate_clean", "chronological_split", "export"]\n',
                encoding="utf-8",
            )
            order = []
            original_eda = cli.eda.run
            original_clean = cli.validate_clean.run

            def run_eda(*args, **kwargs):
                order.append("eda")
                return original_eda(*args, **kwargs)

            def run_clean(*args, **kwargs):
                order.append("clean")
                return original_clean(*args, **kwargs)

            with patch("sys.argv", ["paysim-pipeline", "--config", str(config)]):
                with patch.object(cli.eda, "run", side_effect=run_eda), patch.object(
                    cli.validate_clean, "run", side_effect=run_clean
                ), redirect_stdout(io.StringIO()):
                    cli.main()

            self.assertEqual(order, ["eda", "clean"])
            report_path = root / "artifacts" / "eda" / "raw_dataset_report.json"
            self.assertTrue(report_path.is_file())
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["rows"], 6)

    def test_eda_only_stops_before_cleaning_and_export(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "configs").mkdir()
            (root / "data").mkdir()
            source = root / "data" / "input.csv"
            pd.DataFrame([paysim_record(step=step) for step in range(1, 4)], columns=PAYSIM_COLUMNS).to_csv(
                source, index=False
            )
            config = root / "configs" / "pipeline.toml"
            config.write_text(
                '[dataset]\ninput_csv = "data/input.csv"\noutput_dir = "artifacts"\ndataset_version = "v1"\n'
                '[split]\ntrain_fraction = 0.34\nvalidation_fraction = 0.33\ntest_fraction = 0.33\n'
                '[pipeline]\nstages = ["load", "eda", "validate_clean", "chronological_split", "export"]\n',
                encoding="utf-8",
            )

            with patch("sys.argv", ["paysim-pipeline", "--config", str(config), "--eda-only"]):
                with patch.object(cli.validate_clean, "run", side_effect=AssertionError("cleaning must not run")), redirect_stdout(
                    io.StringIO()
                ):
                    cli.main()

            self.assertTrue((root / "artifacts" / "eda" / "raw_dataset_report.json").is_file())
            self.assertFalse((root / "artifacts" / "features").exists())


if __name__ == "__main__":
    unittest.main()
