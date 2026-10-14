import unittest
from datetime import datetime, timezone

from pydantic import ValidationError

from contracts.events import (
    DLQEvent,
    GroundTruthDTO,
    ModelInputDTO,
    RawTransactionEvent,
    TransactionPredictionEvent,
    parse_raw_transaction,
    to_model_input,
)


RAW_PAYLOAD = {
    "schemaVersion": 2,
    "transactionId": "paysim-0",
    "sourceRowIndex": 0,
    "step": 1,
    "type": "PAYMENT",
    "amount": 9839.64,
    "nameOrig": "C1231006815",
    "oldbalanceOrg": 170136.0,
    "newbalanceOrig": 160296.36,
    "nameDest": "M1979787155",
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
}


class ContractTests(unittest.TestCase):
    def test_raw_event_accepts_pay_sim_business_fields(self) -> None:
        event = RawTransactionEvent.model_validate(RAW_PAYLOAD)

        self.assertEqual(event.schemaVersion, 2)
        self.assertEqual(event.transactionId, "paysim-0")
        self.assertEqual(event.step, 1)

    def test_raw_event_rejects_labels_and_unknown_fields(self) -> None:
        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "isFraud": 1})

        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "timestamp": "2026-10-13T10:00:00Z"})

        with self.assertRaises(ValidationError):
            payload_without_version = dict(RAW_PAYLOAD)
            del payload_without_version["schemaVersion"]
            parse_raw_transaction(payload_without_version)

        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "schemaVersion": 1})

    def test_raw_event_rejects_invalid_values(self) -> None:
        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "step": 0})

        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "type": "UNKNOWN"})

        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "amount": -1})

        with self.assertRaises(ValidationError):
            parse_raw_transaction({**RAW_PAYLOAD, "amount": "1.25"})

    def test_whitelist_conversion_excludes_balance_and_labels(self) -> None:
        raw = RawTransactionEvent.model_validate(RAW_PAYLOAD)
        model_input = to_model_input(raw)

        self.assertEqual(
            set(model_input.model_dump()),
            {
                "transactionId",
                "sourceRowIndex",
                "step",
                "type",
                "amount",
                "nameOrig",
                "nameDest",
            },
        )
        self.assertNotIn("oldbalanceOrg", model_input.model_dump())
        self.assertNotIn("newbalanceOrig", model_input.model_dump())
        self.assertNotIn("oldbalanceDest", model_input.model_dump())
        self.assertNotIn("newbalanceDest", model_input.model_dump())
        self.assertNotIn("isFraud", model_input.model_dump())
        self.assertNotIn("isFlaggedFraud", model_input.model_dump())

    def test_model_input_rejects_balance_and_labels(self) -> None:
        valid = {
            "transactionId": "paysim-0",
            "sourceRowIndex": 0,
            "step": 1,
            "type": "PAYMENT",
            "amount": 9839.64,
            "nameOrig": "C1231006815",
            "nameDest": "M1979787155",
        }

        with self.assertRaises(ValidationError):
            ModelInputDTO(**{**valid, "oldbalanceOrg": 1})

        with self.assertRaises(ValidationError):
            ModelInputDTO(**{**valid, "isFraud": 1})

    def test_ground_truth_is_offline_contract(self) -> None:
        labels = GroundTruthDTO(
            transactionId="paysim-0",
            step=1,
            isFraud=1,
            isFlaggedFraud=0,
        )

        self.assertEqual(labels.isFraud, 1)
        self.assertEqual(set(labels.model_dump()), {"transactionId", "step", "isFraud", "isFlaggedFraud"})

    def test_prediction_and_dlq_contracts(self) -> None:
        prediction = TransactionPredictionEvent(
            schemaVersion=1,
            transactionId="paysim-0",
            step=1,
            anomalyScore=0.87,
            isAnomaly=True,
            modelVersion="gatv2-v1",
            processedAt=datetime.now(timezone.utc),
        )
        self.assertEqual(prediction.reasonCodes, [])
        self.assertTrue(prediction.to_json_dict()["processedAt"].endswith("Z"))

        with self.assertRaises(ValidationError):
            TransactionPredictionEvent(
                schemaVersion=1,
                transactionId="paysim-0",
                step=1,
                anomalyScore="0.87",
                isAnomaly=True,
                modelVersion="gatv2-v1",
                processedAt=datetime.now(timezone.utc),
            )

        dlq = DLQEvent(
            sourceTopic="transactions.raw",
            sourcePartition=0,
            sourceOffset=125,
            errorCode="INVALID_SCHEMA",
            errorMessage="Missing transactionId",
            failedAt=datetime.now(timezone.utc),
            originalPayload={},
        )
        self.assertEqual(dlq.sourceOffset, 125)


if __name__ == "__main__":
    unittest.main()
