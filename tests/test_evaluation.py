import json
import tempfile
import unittest
from pathlib import Path

from evaluation.io import join_inputs, load_ground_truth, load_predictions
from evaluation.metrics import ScoredLabel, evaluate


class EvaluationTests(unittest.TestCase):
    def test_confusion_metrics_and_average_precision(self) -> None:
        report = evaluate([
            ScoredLabel("a", .9, 1, True),
            ScoredLabel("b", .8, 0, True),
            ScoredLabel("c", .2, 1, False),
            ScoredLabel("d", .1, 0, False),
        ], .5)
        self.assertEqual((report.true_positive, report.false_positive, report.true_negative, report.false_negative), (1, 1, 1, 1))
        self.assertEqual(report.precision, .5)
        self.assertEqual(report.recall, .5)
        self.assertEqual(report.f1, .5)
        self.assertAlmostEqual(report.pr_auc, 5 / 6)
        self.assertEqual(report.published_flag_mismatches, 0)

    def test_mismatch_between_published_flag_and_explicit_threshold_is_reported(self) -> None:
        report = evaluate([ScoredLabel("a", .4, 1, True)], .5)
        self.assertEqual(report.published_flag_mismatches, 1)

    def test_join_is_strict_and_readers_validate_contract(self) -> None:
        with self.assertRaisesRegex(ValueError, "do not match"):
            join_inputs({"a": 1}, {"b": (.9, True)})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            labels = root / "labels.csv"
            labels.write_text("transactionId,step,isFraud\na,1,1\nb,1,0\n", encoding="utf-8")
            predictions = root / "predictions.jsonl"
            predictions.write_text("\n".join(json.dumps({
                "schemaVersion": 1, "transactionId": item, "step": 1,
                "anomalyScore": score, "isAnomaly": flag, "modelVersion": "test-v1",
                "processedAt": "2026-10-14T00:00:00Z", "reasonCodes": [],
            }) for item, score, flag in (("a", .9, True), ("b", .1, False))) + "\n", encoding="utf-8")
            self.assertEqual(load_ground_truth(labels), {"a": (1, 1), "b": (1, 0)})
            self.assertEqual(set(load_predictions(predictions)), {"a", "b"})

    def test_join_rejects_step_mismatch(self) -> None:
        with self.assertRaisesRegex(ValueError, "steps do not match"):
            join_inputs({"a": (2, 1)}, {"a": (1, .9, True)})

    def test_evaluation_rejects_empty_and_non_finite_scores(self) -> None:
        with self.assertRaises(ValueError):
            evaluate([], .5)
        with self.assertRaises(ValueError):
            evaluate([ScoredLabel("a", float("nan"), 1)], .5)

    def test_ground_truth_rejects_non_integer_json_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "labels.jsonl"
            path.write_text(
                json.dumps({"transactionId": "a", "step": 1.5, "isFraud": 1}) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "integer"):
                load_ground_truth(path)


if __name__ == "__main__":
    unittest.main()
