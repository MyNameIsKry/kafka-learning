#!/usr/bin/env python3
"""Download and safely extract the public PaySim Kaggle dataset."""

from __future__ import annotations

import argparse
import csv
import shutil
import tempfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


DATASET_URL = "https://www.kaggle.com/api/v1/datasets/download/ealaxi/paysim1"
EXPECTED_CSV = "PS_20174392719_1491204439457_log.csv"
EXPECTED_COLUMNS = {
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
}


def valid_csv(path: Path) -> bool:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            header = next(csv.reader(stream))
        return set(header) == EXPECTED_COLUMNS
    except (OSError, UnicodeError, StopIteration, csv.Error):
        return False


def safe_extract(archive: zipfile.ZipFile, destination: Path) -> list[Path]:
    root = destination.resolve()
    extracted = []
    for member in archive.infolist():
        if member.is_dir():
            continue
        target = (destination / member.filename).resolve()
        if not target.is_relative_to(root):
            raise ValueError(f"Unsafe path in dataset archive: {member.filename}")
        target.parent.mkdir(parents=True, exist_ok=True)
        with archive.open(member) as source, target.open("wb") as output:
            shutil.copyfileobj(source, output)
        extracted.append(target)
    return extracted


def download(output_dir: Path, force: bool) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_path = output_dir / EXPECTED_CSV
    if expected_path.exists() and valid_csv(expected_path) and not force:
        print(f"PaySim dataset already exists: {expected_path}")
        return expected_path

    request = urllib.request.Request(DATASET_URL, headers={"User-Agent": "kafka-learning-paysim-pipeline/1.0"})
    with tempfile.TemporaryDirectory(prefix="paysim-download-", dir=output_dir) as temporary:
        temp_dir = Path(temporary)
        archive_path = temp_dir / "paysim.zip"
        print(f"Downloading public Kaggle dataset: {DATASET_URL}")
        try:
            with urllib.request.urlopen(request, timeout=60) as response, archive_path.open("wb") as output:
                shutil.copyfileobj(response, output)
        except urllib.error.HTTPError as error:
            if error.code in (401, 403):
                raise RuntimeError(
                    "Kaggle denied the public dataset download. Check dataset availability or Kaggle access requirements."
                ) from error
            raise RuntimeError(f"Kaggle download failed with HTTP {error.code}") from error
        except urllib.error.URLError as error:
            raise RuntimeError(f"Cannot reach Kaggle: {error.reason}") from error

        try:
            with zipfile.ZipFile(archive_path) as archive:
                extracted = safe_extract(archive, temp_dir / "extracted")
        except (zipfile.BadZipFile, OSError) as error:
            raise RuntimeError("Kaggle response was not a valid ZIP dataset archive") from error

        csv_files = [path for path in extracted if path.suffix.lower() == ".csv"]
        selected = next((path for path in csv_files if path.name == EXPECTED_CSV), None)
        if selected is None:
            selected = next((path for path in csv_files if valid_csv(path)), None)
        if selected is None or not valid_csv(selected):
            raise RuntimeError("Downloaded archive does not contain a CSV with the expected PaySim columns")

        if expected_path.exists():
            expected_path.unlink()
        shutil.move(str(selected), expected_path)

    print(f"Dataset ready: {expected_path}")
    return expected_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("gatv2-pipeline/data"))
    parser.add_argument("--force", action="store_true", help="Download again even if the CSV already exists")
    args = parser.parse_args()
    download(args.output_dir.resolve(), args.force)


if __name__ == "__main__":
    main()
