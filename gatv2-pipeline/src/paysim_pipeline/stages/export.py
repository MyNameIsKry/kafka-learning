import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from paysim_pipeline import __version__
from paysim_pipeline.schema import LABEL_COLUMNS, MODEL_INPUT_COLUMNS


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(
    splits: dict[str, pd.DataFrame], rejected: pd.DataFrame, output_dir: Path,
    dataset_version: str, input_csv: Path, boundaries: dict[str, int],
) -> dict:
    features_dir, labels_dir = output_dir / "features", output_dir / "ground_truth"
    features_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, frame in splits.items():
        ordered = frame.sort_values(["step", "sourceRowIndex"], kind="stable")
        features = ordered.assign(transactionId="paysim-" + ordered["sourceRowIndex"].astype("string"))
        features = features.loc[:, MODEL_INPUT_COLUMNS]
        labels = ordered.loc[:, ["sourceRowIndex", "step", *LABEL_COLUMNS]].copy()
        labels.insert(0, "transactionId", "paysim-" + labels["sourceRowIndex"].astype("string"))
        features.to_parquet(features_dir / f"{name}.parquet", index=False)
        labels.to_parquet(labels_dir / f"{name}.parquet", index=False)
        counts[name] = {
            "rows": int(len(ordered)),
            "fraud_labels": int(labels["isFraud"].sum()),
            "step_min": int(ordered["step"].min()),
            "step_max": int(ordered["step"].max()),
        }
    if not rejected.empty:
        rejected.to_parquet(output_dir / "rejected.parquet", index=False)
    schema = {
        "schemaVersion": 1,
        "datasetVersion": dataset_version,
        "modelInputColumns": list(MODEL_INPUT_COLUMNS),
        "vectorCandidates": ["amount", "type"],
        "structuralIdentifiers": ["transactionId", "sourceRowIndex", "step", "nameOrig", "nameDest"],
        "excludedColumns": ["oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"],
        "labelColumns": list(LABEL_COLUMNS),
        "note": "Account IDs and step are graph structure/ordering metadata, not numeric vector inputs.",
    }
    (output_dir / "feature_schema.json").write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    manifest = {
        "pipelineVersion": __version__, "datasetVersion": dataset_version,
        "sourceFile": input_csv.name, "sourceBytes": input_csv.stat().st_size,
        "sourceSha256": sha256_file(input_csv),
        "createdAt": datetime.now(timezone.utc).isoformat(), "rowsRejected": int(len(rejected)),
        "splitBoundaries": boundaries, "splits": counts,
        "artifacts": ["features/{train,validation,test}.parquet", "ground_truth/{train,validation,test}.parquet", "feature_schema.json"],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest

