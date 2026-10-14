from pathlib import Path

import pandas as pd

from paysim_pipeline.schema import PAYSIM_COLUMNS


def run(input_csv: Path) -> pd.DataFrame:
    if not input_csv.is_file():
        raise FileNotFoundError(f"PaySim CSV not found: {input_csv}")
    frame = pd.read_csv(input_csv, dtype={"type": "string", "nameOrig": "string", "nameDest": "string"})
    frame.columns = [column.strip() for column in frame.columns]
    if len(frame.columns) != len(set(frame.columns)):
        raise ValueError("Unexpected PaySim schema; duplicate column names")
    missing = sorted(set(PAYSIM_COLUMNS) - set(frame.columns))
    extra = sorted(set(frame.columns) - set(PAYSIM_COLUMNS))
    if missing or extra:
        raise ValueError(f"Unexpected PaySim schema; missing={missing}, extra={extra}")
    frame = frame.loc[:, PAYSIM_COLUMNS].copy()
    # Stable identity refers to the original data row, before any filtering/sorting.
    frame.insert(0, "sourceRowIndex", range(len(frame)))
    return frame
