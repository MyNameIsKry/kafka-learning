import json
from base64 import b64decode
import unittest

from consumer.adapter import ConsumerRecord
from sink.adapter import process_batch
from runtime.metrics import PipelineMetrics


def raw_payload():
    return {
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


def prediction_payload(is_anomaly=True):  # type: ignore[no-untyped-def]
    return {
        "schemaVersion": 1,
        "transactionId": "paysim-0",
        "step": 1,
        "anomalyScore": 0.95,
        "isAnomaly": is_anomaly,
        "modelVersion": "mock-v1",
        "processedAt": "2026-10-14T00:00:00Z",
        "reasonCodes": [],
    }


def record(value, offset=0):  # type: ignore[no-untyped-def]
    return ConsumerRecord(
        topic="transactions.raw",
        partition=0,
        offset=offset,
        value=value if isinstance(value, bytes) else json.dumps(value).encode(),
    )


class FakeStore:
    def __init__(self) -> None:
        self.raw = []
        self.predictions = []
        self.alerts = []

    def upsert_raw(self, event) -> None:  # type: ignore[no-untyped-def]
        self.raw.append(event)

    def upsert_prediction(self, event) -> None:  # type: ignore[no-untyped-def]
        self.predictions.append(event)

    def upsert_alert(self, event) -> None:  # type: ignore[no-untyped-def]
        self.alerts.append(event)


class FakeDLQ:
    def __init__(self) -> None:
        self.events = []

    def publish_dlq(self, event) -> None:  # type: ignore[no-untyped-def]
        self.events.append(event)


class SinkTests(unittest.TestCase):
    def test_storage_metrics_count_only_acknowledged_upserts(self):
        metrics = PipelineMetrics("sink_raw")
        store = FakeStore()
        self.assertEqual(process_batch([record(raw_payload())], "raw", store, FakeDLQ(), metrics), (1, 0))
        labels = {"component": "sink_raw"}
        self.assertEqual(metrics.registry.get_sample_value("pipeline_stored_records_total", labels), 1)
        class FailingStore(FakeStore):
            def upsert_raw(self, _event):
                raise RuntimeError("no write acknowledgement")
        with self.assertRaises(RuntimeError):
            process_batch([record(raw_payload())], "raw", FailingStore(), FakeDLQ(), metrics)
        self.assertEqual(metrics.registry.get_sample_value("pipeline_stored_records_total", labels), 1)

    def test_decode_failure_dlq_preserves_original_bytes(self) -> None:
        for kind in ("raw", "prediction", "alert"):
            for value in (b"broken-json", b"\xff\x00"):
                with self.subTest(kind=kind, value=value):
                    dlq = FakeDLQ()
                    self.assertEqual(process_batch([record(value)], kind, FakeStore(), dlq), (0, 1))
                    event = dlq.events[0]
                    self.assertEqual(event.errorCode, "INVALID_JSON")
                    self.assertEqual(b64decode(event.originalPayload["rawBase64"]), value)

    def test_raw_sink_stores_balance_and_not_labels(self) -> None:
        store = FakeStore()
        dlq = FakeDLQ()

        result = process_batch([record(raw_payload())], "raw", store, dlq)

        self.assertEqual(result, (1, 0))
        self.assertEqual(len(store.raw), 1)
        self.assertIn("oldbalanceOrg", store.raw[0].model_dump())
        self.assertNotIn("isFraud", store.raw[0].model_dump())

    def test_prediction_and_alert_sinks_store_idempotent_contract_events(self) -> None:
        store = FakeStore()
        dlq = FakeDLQ()
        prediction = record(prediction_payload())

        self.assertEqual(process_batch([prediction], "prediction", store, dlq), (1, 0))
        self.assertEqual(process_batch([prediction], "prediction", store, dlq), (1, 0))
        self.assertEqual(len(store.predictions), 2)

        alert = record(prediction_payload())
        self.assertEqual(process_batch([alert], "alert", store, dlq), (1, 0))
        self.assertEqual(len(store.alerts), 1)

    def test_invalid_event_and_non_anomaly_alert_go_to_dlq(self) -> None:
        store = FakeStore()
        dlq = FakeDLQ()

        result = process_batch(
            [record(b"not-json"), record(prediction_payload(False), offset=1)],
            "alert",
            store,
            dlq,
        )

        self.assertEqual(result, (0, 2))
        self.assertEqual(len(dlq.events), 2)

    def test_database_error_propagates(self) -> None:
        class FailingStore(FakeStore):
            def upsert_raw(self, _event) -> None:  # type: ignore[no-untyped-def]
                raise RuntimeError("MongoDB unavailable")

        with self.assertRaises(RuntimeError):
            process_batch([record(raw_payload())], "raw", FailingStore(), FakeDLQ())

    def test_storage_errors_are_not_classified_as_schema_errors(self) -> None:
        for kind in ("raw", "prediction", "alert"):
            for error_type in (ValueError, TypeError, RuntimeError):
                with self.subTest(kind=kind, error=error_type.__name__):
                    class FailingStore(FakeStore):
                        def fail(self, _event):
                            raise error_type("storage failure")

                        upsert_raw = fail
                        upsert_prediction = fail
                        upsert_alert = fail

                    dlq = FakeDLQ()
                    payload = raw_payload() if kind == "raw" else prediction_payload()
                    with self.assertRaises(error_type):
                        process_batch([record(payload)], kind, FailingStore(), dlq)
                    self.assertEqual(dlq.events, [])


if __name__ == "__main__":
    unittest.main()
