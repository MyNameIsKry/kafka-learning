"""Non-mutating exploratory summary of the loaded, pre-clean PaySim rows."""

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from paysim_pipeline.schema import LABEL_COLUMNS, PAYSIM_COLUMNS, TRANSACTION_TYPES

NUMERIC_COLUMNS = (
    "step", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud",
)


def _numeric_summary(values: pd.Series) -> dict:
    numeric = pd.to_numeric(values, errors="coerce")
    raw_non_null = values.notna()
    parse_failed = raw_non_null & numeric.isna()
    finite = pd.Series(np.isfinite(numeric.to_numpy(dtype="float64", na_value=np.nan)), index=values.index)
    clean = numeric.loc[finite]
    return {
        "numeric_count": int(finite.sum()),
        "missing_count": int(values.isna().sum()),
        "parse_failed_count": int(parse_failed.sum()),
        "non_finite_count": int((numeric.notna() & ~finite).sum()),
        "min": float(clean.min()) if not clean.empty else None,
        "max": float(clean.max()) if not clean.empty else None,
        "mean": float(clean.mean()) if not clean.empty else None,
        "std": float(clean.std()) if len(clean) > 1 else 0.0 if len(clean) == 1 else None,
    }


def run(frame: pd.DataFrame, output_dir: Path, dataset_version: str, source_file: str) -> dict:
    """Write a descriptive report without modifying or filtering ``frame``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    column_summary = {}
    for column in PAYSIM_COLUMNS:
        values = frame[column]
        summary = {
            "dtype": str(values.dtype),
            "row_count": int(len(values)),
            "missing_count": int(values.isna().sum()),
            "blank_count": int(values.astype("string").str.strip().eq("").fillna(False).sum()),
            "distinct_count": int(values.nunique(dropna=True)),
        }
        if column in NUMERIC_COLUMNS:
            summary.update(_numeric_summary(values))
        elif column == "type":
            summary["value_counts"] = {
                str(key): int(value)
                for key, value in values.value_counts(dropna=False).head(25).items()
            }
            summary["unexpected_type_count"] = int((~values.isin(TRANSACTION_TYPES)).sum())
        column_summary[column] = summary

    report = {
        "reportVersion": 1,
        "datasetVersion": dataset_version,
        "sourceFile": source_file,
        "createdAt": datetime.now(timezone.utc).isoformat(),
        "stage": "before_validate_clean",
        "rows": int(len(frame)),
        "sourceRowOrderStepDecreases": int((pd.to_numeric(frame["step"], errors="coerce").diff() < 0).sum()),
        "exactDuplicateSourceRows": int(frame.loc[:, PAYSIM_COLUMNS].duplicated().sum()),
        "columns": column_summary,
        "balanceConsistency": {
            "originMismatchCount": int(_balance_mismatch_count(frame, "oldbalanceOrg", -1, "amount", "newbalanceOrig")),
            "destinationMismatchCount": int(_balance_mismatch_count(frame, "oldbalanceDest", 1, "amount", "newbalanceDest")),
            "note": "Descriptive only; balance inconsistencies are not automatically corrected or rejected.",
        },
        "labelUsage": {
            "scope": "Full pre-clean dataset; exploratory description only.",
            "counts": {
                column: {str(key): int(value) for key, value in frame[column].value_counts(dropna=False).items()}
                for column in LABEL_COLUMNS
            },
            "restriction": "Do not use full-dataset labels for feature selection, fitting, threshold tuning, or model selection.",
        },
    }
    report_path = output_dir / "raw_dataset_report.json"
    report_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return {"reportPath": str(report_path), "rows": report["rows"], "duplicateRows": report["exactDuplicateSourceRows"]}


def _balance_mismatch_count(frame: pd.DataFrame, balance_column: str, amount_sign: int, amount_column: str, result_column: str) -> int:
    balance = pd.to_numeric(frame[balance_column], errors="coerce")
    amount = pd.to_numeric(frame[amount_column], errors="coerce")
    result = pd.to_numeric(frame[result_column], errors="coerce")
    expected = balance + amount_sign * amount
    comparable = expected.notna() & result.notna() & np.isfinite(expected) & np.isfinite(result)
    matches = np.isclose(expected.loc[comparable], result.loc[comparable], rtol=1e-5, atol=0.01)
    return int((~matches).sum())
