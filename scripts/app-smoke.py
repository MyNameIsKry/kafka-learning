"""Non-destructive smoke test of the single-process HTTP runtime.

Requires running demo infrastructure; keeps unique streams/topics/database.
Uses an unreachable Kafka address to verify independent Mongo projection.
"""
import argparse
import json
import pathlib
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
RUN = "app-smoke-" + uuid.uuid4().hex
DIR = pathlib.Path("/tmp/opencode") / RUN
DIR.mkdir(parents=True)
BINARY = DIR / "app"
BASE = "http://localhost:18080"


def request(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def wait_for(predicate):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.5)
    raise AssertionError(f"timeout; inspect {DIR}")


def start(brokers):
    log = open(DIR / f"{brokers.replace(':', '-')}.log", "w")
    process = subprocess.Popen([
        str(BINARY), "--addr", ":18080", "--mongo-db", RUN,
        "--stream-prefix", RUN + "-event-", "--group-prefix", RUN,
        "--topic-prefix", RUN, "--bootstrap-servers", brokers,
        "--mock-score", "0.95", "--model-version", RUN,
    ], stdout=log, stderr=subprocess.STDOUT)
    log.close()
    return process


def stop(process):
    process.terminate()
    try:
        process.wait(timeout=40)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        raise AssertionError("server failed graceful shutdown")
    assert process.returncode == 0, f"server exit={process.returncode}; logs: {DIR}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--locust-bin", help="optionally exercise real Locust (absolute binary path)")
    args = parser.parse_args()
    subprocess.run(["go", "build", "-o", str(BINARY), "./cmd/app"], cwd=ROOT / "server", check=True)
    for kind in ("raw", "scored", "anomalies", "dlq"):
        subprocess.run([
            "docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092", "--create", "--topic", f"{RUN}.{kind}",
            "--partitions", "1", "--replication-factor", "1",
        ], cwd=ROOT, check=True)
    payload = dict(schemaVersion=2, transactionId=RUN + "-1", sourceRowIndex=1,
                   step=1, type="TRANSFER", amount=20, nameOrig="C1", nameDest="C2",
                   oldbalanceOrg=200, newbalanceOrig=180, oldbalanceDest=0, newbalanceDest=20)
    process = start("localhost:1")
    try:
        wait_for(lambda: request("/healthz")[0] == 200)
        assert request("/api/v1/transactions", payload)[0] == 202
        assert request("/api/v1/transactions", payload)[0] == 202  # same event, no duplicate
        assert request("/api/v1/transactions", dict(payload, amount=21))[0] == 409
        assert request("/api/v1/transactions", dict(payload, isFraud=1))[0] == 400
        wait_for(lambda: request("/api/v1/summary")[1]["totalTransactions"] == 1)
        assert request("/api/v1/summary")[1]["totalPredictions"] == 0
        assert request("/api/v1/transactions")[1]["items"][0]["status"] == "pending"
    finally:
        stop(process)
    process = start("localhost:9092")
    try:
        wait_for(lambda: request("/api/v1/summary")[1]["totalPredictions"] == 1)
        wait_for(lambda: len(request("/api/v1/alerts")[1]["items"]) == 1)
        summary = request("/api/v1/summary")[1]
        assert summary["totalTransactions"] == summary["totalPredictions"] == summary["totalAnomalies"] == 1
        assert request("/api/v1/transactions")[1]["items"][0]["status"] == "anomaly"
        if args.locust_bin:
            subprocess.run([
                args.locust_bin, "-f", "locustfile.py", "--host", BASE,
                "--headless", "--users", "2", "--spawn-rate", "2", "--run-time", "5s",
                "--stop-timeout", "30", "--only-summary",
            ], cwd=ROOT, check=True)
            def load_completed():
                summary = request("/api/v1/summary")[1]
                return summary["totalTransactions"] > 1 and summary["totalTransactions"] == summary["totalPredictions"]
            wait_for(load_completed)
            print("PASS: Locust HTTP requests accepted and scored")
    finally:
        stop(process)
    print("PASS: HTTP store-first, idempotency/conflict, labels rejected, direct Mongo projection while Kafka unavailable, restart recovery and scoring.")
    print(f"Test resources retained: {RUN}; logs: {DIR}")


if __name__ == "__main__":
    main()
