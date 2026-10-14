# PaySim Event Contracts

This is the executable contract for the Python pipeline. The source of truth
is `contracts/events.py`; this document describes the topic boundaries.

## `transactions.raw`

`RawTransactionEvent` uses `schemaVersion: 2` and contains the PaySim business
fields below:

```json
{
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
  "newbalanceDest": 0.0
}
```

The raw event does **not** contain `isFraud`, `isFlaggedFraud`, `timestamp`, or
`currency`. The first two belong to the offline ground-truth store.

## Model input boundary

`to_model_input(raw)` is a whitelist conversion. It passes only:

```text
transactionId, sourceRowIndex, step, type, amount, nameOrig, nameDest
```

It must never pass the four balance fields or either label. Both raw and model
DTOs reject unknown fields, so labels accidentally added to a production-like
raw event fail validation instead of being silently accepted.

## `transactions.scored` and `transactions.anomalies`

Both topics use `TransactionPredictionEvent`:

```json
{
  "schemaVersion": 1,
  "transactionId": "paysim-0",
  "step": 1,
  "anomalyScore": 0.87,
  "isAnomaly": true,
  "modelVersion": "gatv2-v1",
  "processedAt": "2026-10-13T15:00:01Z",
  "reasonCodes": []
}
```

`anomalyScore` must be finite. Whether it is interpreted as a probability or
another normalized score, and the threshold for `isAnomaly`, are decisions of
the AI pipeline.

## Offline ground truth

`GroundTruthDTO` contains `isFraud` and `isFlaggedFraud`. It is not consumed by
the inference service.

## `transactions.dlq`

`DLQEvent` records the source topic, partition, offset, validation error and
the original decoded payload. If JSON/UTF-8 decoding fails, runtime handlers
retain the exact original bytes as `originalPayload.rawBase64` with
`originalPayload.encoding="base64"`.
