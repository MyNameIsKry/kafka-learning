"""HTTP-only load generator. Labels never enter online requests."""
import itertools
import random
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
        amount = round(random.uniform(1.0, 1000.0), 2)
        sender_balance_after = 1000.0
        sender = f"sender-{index % 5}"
        receiver = f"receiver-{index % 5}"
        payload = {
            "schemaVersion": 2,
            "transactionId": f"locust-{self.run_id}-{index}",
            "sourceRowIndex": index,
            "step": index,
            "type": "TRANSFER",
            "amount": amount,
            "nameOrig": sender,
            "nameDest": receiver,
            "oldbalanceOrg": sender_balance_after + amount,
            "newbalanceOrig": sender_balance_after,
            "oldbalanceDest": 0.0,
            "newbalanceDest": amount,
        }
        with self.client.post(
            "/api/v1/transactions", json=payload, catch_response=True
        ) as response:
            if response.status_code != 202:
                response.failure(f"expected durable acceptance (202), got {response.status_code}")
            elif response.json().get("status") != "accepted":
                response.failure("missing acceptance receipt")
