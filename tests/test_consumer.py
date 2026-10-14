import json
from base64 import b64decode
import unittest
from datetime import datetime, timezone

from contracts.events import TransactionPredictionEvent
from consumer.adapter import (
    BatchInferenceError,
    ConsumerRecord,
    SeenTransactionCache,
    process_batch,
)
from consumer.main import commit_batch
from kafka.structs import TopicPartition


def raw_payload(**overrides):  # type: ignore[no-untyped-def]
    payload = {
        "schemaVersion": 2,
        "transactionId": "paysim-0",
        "sourceRowIndex": 0,
        "step": 1,
        "type": "PAYMENT",
        "amount": 100.0,
        "nameOrig": "C1",
        "oldbalanceOrg": 1000.0,
        "newbalanceOrig": 900.0,
        "nameDest": "M1",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 100.0,
    }
    payload.update(overrides)
    return payload


def record(value, offset=0):  # type: ignore[no-untyped-def]
    return ConsumerRecord(
        topic="transactions.raw",
        partition=0,
        offset=offset,
        value=value if isinstance(value, bytes) else json.dumps(value).encode(),
    )


class FakePublisher:
    def __init__(self) -> None:
        self.predictions = []
        self.dlq = []

    def publish_prediction(self, prediction: TransactionPredictionEvent) -> None:
        self.predictions.append(prediction)

    def publish_dlq(self, event) -> None:  # type: ignore[no-untyped-def]
        self.dlq.append(event)


class ReorderingEngine:
    def __init__(self, mismatch: bool = False) -> None:
        self.inputs = []
        self.mismatch = mismatch

    def predict(self, transactions):  # type: ignore[no-untyped-def]
        self.inputs = transactions
        selected = transactions if not self.mismatch else transactions[:-1]
        return [
            TransactionPredictionEvent(
                schemaVersion=1,
                transactionId=item.transactionId,
                step=item.step,
                anomalyScore=0.2,
                isAnomaly=False,
                modelVersion="test-v1",
                processedAt=datetime.now(timezone.utc),
                reasonCodes=[],
            )
            for item in reversed(selected)
        ]


class ConsumerAdapterTests(unittest.TestCase):
    def test_wrong_step_prediction_is_rejected_before_publishing(self) -> None:
        class WrongStepEngine(ReorderingEngine):
            def predict(self, transactions):
                predictions = super().predict(transactions)
                predictions[0].step = 999
                return predictions

        publisher = FakePublisher()
        with self.assertRaises(BatchInferenceError):
            process_batch([record(raw_payload())], WrongStepEngine(), publisher)
        self.assertEqual(publisher.predictions, [])

    def test_constructed_invalid_prediction_is_revalidated(self) -> None:
        class InvalidEngine(ReorderingEngine):
            def predict(self, transactions):
                prediction = super().predict(transactions)[0]
                return [prediction.model_copy(update={"anomalyScore": float("nan")})]

        publisher = FakePublisher()
        with self.assertRaises(BatchInferenceError):
            process_batch([record(raw_payload())], InvalidEngine(), publisher)
        self.assertEqual(publisher.predictions, [])

    def test_decode_failure_dlq_preserves_original_bytes(self) -> None:
        for value in (b"not-json", b"\xff\x00"):
            with self.subTest(value=value):
                publisher = FakePublisher()
                self.assertEqual(process_batch([record(value)], ReorderingEngine(), publisher), (0, 1))
                event = publisher.dlq[0]
                self.assertEqual(event.errorCode, "INVALID_JSON")
                self.assertEqual(b64decode(event.originalPayload["rawBase64"]), value)

    def test_commit_batch_commits_next_offset_per_partition(self) -> None:
        class FakeConsumer:
            def __init__(self) -> None:
                self.offsets = None

            def commit(self, offsets):  # type: ignore[no-untyped-def]
                self.offsets = offsets

        consumer = FakeConsumer()
        commit_batch(
            consumer,
            [record(raw_payload(), offset=4), record(raw_payload(transactionId="paysim-1"), offset=6)],
        )

        self.assertEqual(consumer.offsets[TopicPartition("transactions.raw", 0)].offset, 7)

    def test_valid_batch_uses_model_input_and_maps_by_transaction_id(self) -> None:
        publisher = FakePublisher()
        engine = ReorderingEngine()

        accepted, dlq_count = process_batch(
            [record(raw_payload(transactionId="paysim-1", sourceRowIndex=1), offset=1), record(raw_payload())],
            engine,
            publisher,
        )

        self.assertEqual((accepted, dlq_count), (2, 0))
        self.assertEqual([item.transactionId for item in engine.inputs], ["paysim-0", "paysim-1"])
        self.assertEqual(
            [prediction.transactionId for prediction in publisher.predictions],
            ["paysim-0", "paysim-1"],
        )
        self.assertNotIn("oldbalanceOrg", engine.inputs[0].model_dump())

    def test_invalid_json_and_schema_are_sent_to_dlq(self) -> None:
        publisher = FakePublisher()
        engine = ReorderingEngine()

        accepted, dlq_count = process_batch(
            [record(b"not-json"), record(raw_payload(isFraud=1), offset=1)],
            engine,
            publisher,
        )

        self.assertEqual((accepted, dlq_count), (0, 2))
        self.assertEqual(len(publisher.dlq), 2)
        self.assertEqual(
            {event.errorCode for event in publisher.dlq},
            {"INVALID_JSON", "INVALID_SCHEMA"},
        )

    def test_prediction_mapping_mismatch_fails_without_publishing_predictions(self) -> None:
        publisher = FakePublisher()
        with self.assertRaises(BatchInferenceError):
            process_batch([record(raw_payload()), record(raw_payload(transactionId="paysim-1"), 1)], ReorderingEngine(True), publisher)

        self.assertEqual(publisher.predictions, [])

    def test_duplicate_transaction_is_published_once_and_committed_as_input(self) -> None:
        publisher = FakePublisher()
        engine = ReorderingEngine()
        seen = SeenTransactionCache(max_size=1)

        accepted, dlq_count = process_batch(
            [record(raw_payload()), record(raw_payload(), offset=1)],
            engine,
            publisher,
            seen,
        )

        self.assertEqual((accepted, dlq_count), (1, 0))
        self.assertEqual(len(publisher.predictions), 1)
        self.assertIsNotNone(seen.get("paysim-0"))

        accepted, dlq_count = process_batch(
            [record(raw_payload(), offset=2)],
            engine,
            publisher,
            seen,
        )
        self.assertEqual((accepted, dlq_count), (0, 0))
        self.assertEqual(len(publisher.predictions), 1)

    def test_conflicting_duplicate_transaction_fails(self) -> None:
        with self.assertRaises(BatchInferenceError):
            process_batch(
                [record(raw_payload()), record(raw_payload(amount=999), offset=1)],
                ReorderingEngine(),
                FakePublisher(),
            )

    def test_anomaly_prediction_is_still_published_by_adapter(self) -> None:
        class AnomalyEngine(ReorderingEngine):
            def predict(self, transactions):  # type: ignore[no-untyped-def]
                item = transactions[0]
                return [
                    TransactionPredictionEvent(
                        schemaVersion=1,
                        transactionId=item.transactionId,
                        step=item.step,
                        anomalyScore=0.95,
                        isAnomaly=True,
                        modelVersion="test-v1",
                        processedAt=datetime.now(timezone.utc),
                        reasonCodes=[],
                    )
                ]

        publisher = FakePublisher()
        accepted, dlq_count = process_batch([record(raw_payload())], AnomalyEngine(), publisher)

        self.assertEqual((accepted, dlq_count), (1, 0))
        self.assertTrue(publisher.predictions[0].isAnomaly)


if __name__ == "__main__":
    unittest.main()
