"""Dataset and inference-safe feature contracts derived from design v2."""

PAYSIM_COLUMNS = (
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
)
TRANSACTION_TYPES = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
BALANCE_COLUMNS = ("oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest")
LABEL_COLUMNS = ("isFraud", "isFlaggedFraud")

# This is a contract for the graph builder. Account IDs are structural node keys,
# not numeric/vector features. Balance columns and both labels are excluded.
MODEL_INPUT_COLUMNS = (
    "transactionId", "sourceRowIndex", "step", "type", "amount", "nameOrig", "nameDest",
)
MODEL_VECTOR_CANDIDATES = ("amount", "type")


def assert_model_input_columns(columns) -> None:
    """Fail closed when offline features drift from the Kafka whitelist."""

    actual = tuple(columns)
    if actual != MODEL_INPUT_COLUMNS:
        raise ValueError(
            f"offline ModelInputDTO drift: expected {MODEL_INPUT_COLUMNS}, got {actual}"
        )
