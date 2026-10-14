import csv
import tempfile
import unittest
from pathlib import Path

from producer.main import ReplayPublishError, replay
from producer.paysim import (
    PAYSIM_COLUMNS,
    PaySimInputError,
    load_paysim_csv,
    prepare_paysim_csv,
)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=PAYSIM_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def row(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "step": 2,
        "type": "PAYMENT",
        "amount": "100.50",
        "nameOrig": "C123",
        "oldbalanceOrg": "1000.0",
        "newbalanceOrig": "899.5",
        "nameDest": "M456",
        "oldbalanceDest": "0.0",
        "newbalanceDest": "100.5",
        "isFraud": "0",
        "isFlaggedFraud": "0",
    }
    value.update(overrides)
    return value


class FakePublisher:
    def __init__(self) -> None:
        self.events = []

    def publish(self, event):  # type: ignore[no-untyped-def]
        self.events.append(event)


class ProducerTests(unittest.TestCase):
    def test_load_sorts_by_step_then_original_row_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "paysim.csv"
            write_csv(path, [row(step=2), row(step=1), row(step=1, nameOrig="C789")])

            records = load_paysim_csv(path)

            self.assertEqual([r.raw.step for r in records], [1, 1, 2])
            self.assertEqual([r.raw.sourceRowIndex for r in records], [1, 2, 0])
            self.assertEqual([r.raw.transactionId for r in records], ["paysim-1", "paysim-2", "paysim-0"])

    def test_external_sort_uses_bounded_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "paysim.csv"
            write_csv(path, [row(step=3), row(step=1), row(step=2)])

            with prepare_paysim_csv(path, chunk_size=1) as prepared:
                self.assertEqual(prepared.count, 3)
                self.assertEqual(
                    [record.raw.step for record in prepared.iter_records()],
                    [1, 2, 3],
                )

    def test_dataset_version_changes_id_without_changing_source_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "paysim.csv"
            write_csv(path, [row()])

            record = load_paysim_csv(path, dataset_version="v1")[0]

            self.assertEqual(record.raw.transactionId, "paysim-v1-0")
            self.assertEqual(record.raw.sourceRowIndex, 0)

    def test_ground_truth_is_written_separately_and_labels_are_not_published(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            csv_path = directory_path / "paysim.csv"
            ground_truth_path = directory_path / "offline" / "ground_truth.csv"
            write_csv(csv_path, [row(isFraud="1", isFlaggedFraud="0")])
            publisher = FakePublisher()

            records = load_paysim_csv(csv_path)
            stats = replay(records, publisher, ground_truth_path=str(ground_truth_path))

            self.assertEqual((stats.attempted, stats.published, stats.failed), (1, 1, 0))
            self.assertTrue(ground_truth_path.exists())
            self.assertEqual(publisher.events[0].transactionId, "paysim-0")
            self.assertNotIn("isFraud", publisher.events[0].model_dump())
            self.assertNotIn("isFlaggedFraud", publisher.events[0].model_dump())
            self.assertIn("oldbalanceOrg", publisher.events[0].model_dump())
            self.assertIn("newbalanceDest", publisher.events[0].model_dump())
            self.assertIn("isFraud", ground_truth_path.read_text(encoding="utf-8"))

    def test_invalid_csv_data_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "paysim.csv"
            write_csv(path, [row(type="NOT_A_TYPE")])

            with self.assertRaises(PaySimInputError):
                load_paysim_csv(path)

    def test_publish_failure_is_counted(self) -> None:
        class FailingPublisher:
            def publish(self, _event):  # type: ignore[no-untyped-def]
                raise RuntimeError("broker unavailable")

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "paysim.csv"
            ground_truth_path = Path(directory) / "ground_truth.csv"
            write_csv(path, [row()])

            with self.assertRaises(ReplayPublishError) as raised:
                replay(
                    load_paysim_csv(path),
                    FailingPublisher(),
                    ground_truth_path=str(ground_truth_path),
                )

            self.assertEqual(raised.exception.stats.attempted, 1)
            self.assertEqual(raised.exception.stats.published, 0)
            self.assertEqual(raised.exception.stats.failed, 1)
            self.assertFalse(ground_truth_path.exists())


if __name__ == "__main__":
    unittest.main()
