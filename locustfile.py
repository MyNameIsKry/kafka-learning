"""HTTP-only load generator. Labels never enter online requests."""
import itertools
import uuid

from locust import HttpUser, between, task


class TransactionUser(HttpUser):
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.sequence = itertools.count(1)
        self.run_id = uuid.uuid4().hex

    @task
    def submit_transaction(self):
        index = next(self.sequence)
        payload = {
            "schemaVersion": 2,
            "transactionId": f"locust-{self.run_id}-{index}",
            "sourceRowIndex": index,
            "step": index,
            "type": "TRANSFER",
            "amount": 20.0,
            "nameOrig": f"C{self.run_id}",
            "nameDest": "C-receiver",
            "oldbalanceOrg": 200.0,
            "newbalanceOrig": 180.0,
            "oldbalanceDest": 0.0,
            "newbalanceDest": 20.0,
        }
        with self.client.post(
            "/api/v1/transactions", json=payload, catch_response=True
        ) as response:
            if response.status_code != 202:
                response.failure(f"expected durable acceptance (202), got {response.status_code}")
            elif response.json().get("status") != "accepted":
                response.failure("missing acceptance receipt")
