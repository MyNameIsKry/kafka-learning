import pandas as pd

from paysim_pipeline.schema import LABEL_COLUMNS, TRANSACTION_TYPES

NUMERIC_COLUMNS = (
    "step", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud",
)


def run(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = frame.copy()
    reasons = pd.Series("", index=frame.index, dtype="string")
    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        bad = frame[column].isna() | ~frame[column].map(lambda value: pd.notna(value) and abs(value) != float("inf"))
        reasons = reasons.mask(bad, reasons + f"{column}:missing_or_non_finite;")
    for column in ("nameOrig", "nameDest", "type"):
        frame[column] = frame[column].astype("string").str.strip()
        bad = frame[column].isna() | frame[column].eq("")
        reasons = reasons.mask(bad, reasons + f"{column}:empty;")
    checks = {
        "step": frame["step"] < 1,
        "amount": frame["amount"] < 0,
        **{column: frame[column] < 0 for column in (
            "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"
        )},
        "type": ~frame["type"].isin(TRANSACTION_TYPES),
        "isFraud": ~frame["isFraud"].isin((0, 1)),
        "isFlaggedFraud": ~frame["isFlaggedFraud"].isin((0, 1)),
    }
    for column in ("step", "isFraud", "isFlaggedFraud"):
        checks[f"{column}_integer"] = frame[column].notna() & (frame[column] % 1 != 0)
    for column, invalid in checks.items():
        reasons = reasons.mask(invalid, reasons + f"{column}:invalid_value;")

    rejected_mask = reasons.ne("")
    rejected = frame.loc[rejected_mask].copy()
    rejected["rejectReason"] = reasons.loc[rejected_mask]
    accepted = frame.loc[~rejected_mask].copy()
    accepted["step"] = accepted["step"].astype("int64")
    for column in LABEL_COLUMNS:
        accepted[column] = accepted[column].astype("int8")
    # Keep duplicate rows: in PaySim they are separate events. IDs derive from source row.
    if accepted["sourceRowIndex"].duplicated().any():
        raise ValueError("sourceRowIndex must uniquely identify source rows")
    return accepted, rejected
