"""MongoDB persistence for raw transactions, predictions and alerts."""

from __future__ import annotations

from pymongo import ASCENDING, MongoClient

from contracts.events import RawTransactionEvent, TransactionPredictionEvent


class MongoStore:
    """Idempotent MongoDB repository used by the three sink processes."""

    def __init__(self, uri: str, database: str) -> None:
        self._client = MongoClient(uri, serverSelectionTimeoutMS=5_000)
        self._client.admin.command("ping")
        db = self._client[database]
        self._transactions = db["transactions"]
        self._predictions = db["transaction_predictions"]
        self._alerts = db["transaction_alerts"]
        self.ensure_indexes()

    def ensure_indexes(self) -> None:
        self._transactions.create_index([("transactionId", ASCENDING)], unique=True)
        self._transactions.create_index([("step", ASCENDING)])
        self._transactions.create_index([
            ("step", ASCENDING), ("sourceRowIndex", ASCENDING), ("transactionId", ASCENDING)
        ])
        self._predictions.create_index(
            [("transactionId", ASCENDING), ("modelVersion", ASCENDING)],
            unique=True,
        )
        self._predictions.create_index([("step", ASCENDING), ("isAnomaly", ASCENDING)])
        self._predictions.create_index([
            ("transactionId", ASCENDING), ("processedAt", ASCENDING), ("modelVersion", ASCENDING)
        ])
        self._alerts.create_index(
            [("transactionId", ASCENDING), ("modelVersion", ASCENDING)],
            unique=True,
        )

    def upsert_raw(self, event: RawTransactionEvent) -> None:
        document = event.model_dump(mode="python")
        self._transactions.update_one(
            {"transactionId": event.transactionId},
            {"$set": document},
            upsert=True,
        )

    def upsert_prediction(self, event: TransactionPredictionEvent) -> None:
        document = event.model_dump(mode="python")
        self._predictions.update_one(
            {
                "transactionId": event.transactionId,
                "modelVersion": event.modelVersion,
            },
            {"$set": document},
            upsert=True,
        )

    def upsert_alert(self, event: TransactionPredictionEvent) -> None:
        document = event.model_dump(mode="python")
        self._alerts.update_one(
            {
                "transactionId": event.transactionId,
                "modelVersion": event.modelVersion,
            },
            {"$set": document},
            upsert=True,
        )

    def close(self) -> None:
        self._client.close()
