"""PaySim CSV parsing and bounded-memory replay preparation.

PaySim is large enough that a full in-memory list is not a safe default. The
loader validates rows into bounded sorted chunks on disk, then merges those
chunks by ``(step, sourceRowIndex)`` while the producer publishes them.
"""

from __future__ import annotations

import csv
import heapq
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from contracts.events import GroundTruthDTO, RawTransactionEvent

PAYSIM_COLUMNS = (
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
)


@dataclass(frozen=True)
class PreparedTransaction:
    """Validated raw event and its offline-only labels."""

    raw: RawTransactionEvent
    ground_truth: GroundTruthDTO


class PaySimInputError(ValueError):
    """Raised when a PaySim CSV row violates the input contract."""


class PreparedPaySim:
    """External-sort output that can be iterated without retaining all rows."""

    def __init__(self, temp_dir, chunks: list[Path], count: int) -> None:  # type: ignore[no-untyped-def]
        self._temp_dir = temp_dir
        self._chunks = chunks
        self.count = count

    def iter_records(self) -> Iterator[PreparedTransaction]:
        handles = [chunk.open("r", encoding="utf-8") for chunk in self._chunks]
        heap: list[tuple[int, int, int, PreparedTransaction]] = []

        def read_record(chunk_index: int) -> PreparedTransaction | None:
            line = handles[chunk_index].readline()
            if not line:
                return None
            payload = json.loads(line)
            return PreparedTransaction(
                raw=RawTransactionEvent.model_validate(payload["raw"]),
                ground_truth=GroundTruthDTO.model_validate(payload["ground_truth"]),
            )

        try:
            for chunk_index in range(len(handles)):
                record = read_record(chunk_index)
                if record is not None:
                    heapq.heappush(
                        heap,
                        (record.raw.step, record.raw.sourceRowIndex, chunk_index, record),
                    )

            while heap:
                _, _, chunk_index, record = heapq.heappop(heap)
                yield record
                next_record = read_record(chunk_index)
                if next_record is not None:
                    heapq.heappush(
                        heap,
                        (
                            next_record.raw.step,
                            next_record.raw.sourceRowIndex,
                            chunk_index,
                            next_record,
                        ),
                    )
        finally:
            for handle in handles:
                handle.close()

    def close(self) -> None:
        self._temp_dir.cleanup()

    def __enter__(self) -> "PreparedPaySim":
        return self

    def __exit__(self, _type, _value, _traceback) -> None:  # type: ignore[no-untyped-def]
        self.close()


def make_transaction_id(source_row_index: int, dataset_version: str | None = None) -> str:
    """Create the stable transaction ID used by the replay contract."""

    if source_row_index < 0:
        raise ValueError("source_row_index must be non-negative")
    if dataset_version:
        return f"paysim-{dataset_version}-{source_row_index}"
    return f"paysim-{source_row_index}"


def _required_headers(fieldnames: list[str] | None) -> None:
    if fieldnames is None:
        raise PaySimInputError("CSV is missing a header row")

    expected = set(PAYSIM_COLUMNS)
    actual = set(fieldnames)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    duplicate_headers = len(fieldnames) != len(actual)
    if missing or extra or duplicate_headers or len(fieldnames) != len(PAYSIM_COLUMNS):
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if extra:
            details.append(f"unexpected: {', '.join(extra)}")
        if duplicate_headers:
            details.append("duplicate header names")
        raise PaySimInputError("invalid PaySim headers (" + "; ".join(details) + ")")


def _parse_int(row: dict[str, str], field: str, row_index: int) -> int:
    value = row.get(field, "").strip()
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise PaySimInputError(f"row {row_index}: {field} must be an integer") from exc


def _parse_float(row: dict[str, str], field: str, row_index: int) -> float:
    value = row.get(field, "").strip()
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise PaySimInputError(f"row {row_index}: {field} must be a number") from exc


def _parse_row(
    row: dict[str, str], source_row_index: int, dataset_version: str | None
) -> PreparedTransaction:
    try:
        is_fraud = _parse_int(row, "isFraud", source_row_index)
        is_flagged_fraud = _parse_int(row, "isFlaggedFraud", source_row_index)
        raw = RawTransactionEvent(
            schemaVersion=2,
            transactionId=make_transaction_id(source_row_index, dataset_version),
            sourceRowIndex=source_row_index,
            step=_parse_int(row, "step", source_row_index),
            type=row.get("type", "").strip(),
            amount=_parse_float(row, "amount", source_row_index),
            nameOrig=row.get("nameOrig", "").strip(),
            oldbalanceOrg=_parse_float(row, "oldbalanceOrg", source_row_index),
            newbalanceOrig=_parse_float(row, "newbalanceOrig", source_row_index),
            nameDest=row.get("nameDest", "").strip(),
            oldbalanceDest=_parse_float(row, "oldbalanceDest", source_row_index),
            newbalanceDest=_parse_float(row, "newbalanceDest", source_row_index),
        )
        ground_truth = GroundTruthDTO(
            transactionId=raw.transactionId,
            step=raw.step,
            isFraud=is_fraud,
            isFlaggedFraud=is_flagged_fraud,
        )
    except Exception as exc:
        if isinstance(exc, PaySimInputError):
            raise
        raise PaySimInputError(f"row {source_row_index}: invalid PaySim data: {exc}") from exc

    return PreparedTransaction(raw=raw, ground_truth=ground_truth)


def _write_chunk(records: list[PreparedTransaction], path: Path) -> None:
    records.sort(key=lambda record: (record.raw.step, record.raw.sourceRowIndex))
    with path.open("w", encoding="utf-8") as chunk_file:
        for record in records:
            json.dump(
                {
                    "raw": record.raw.to_json_dict(),
                    "ground_truth": record.ground_truth.to_json_dict(),
                },
                chunk_file,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            chunk_file.write("\n")


def prepare_paysim_csv(
    path: str | Path,
    *,
    dataset_version: str | None = None,
    max_events: int | None = None,
    chunk_size: int = 10_000,
) -> PreparedPaySim:
    """Validate the complete CSV and create bounded external-sort chunks."""

    if max_events is not None and max_events <= 0:
        raise ValueError("max_events must be positive when provided")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    temp_dir = tempfile.TemporaryDirectory(prefix="paysim-sort-")
    chunks: list[Path] = []
    pending: list[PreparedTransaction] = []
    count = 0

    try:
        with Path(path).open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            _required_headers(reader.fieldnames)
            for source_row_index, row in enumerate(reader):
                if max_events is not None and source_row_index >= max_events:
                    break
                if None in row:
                    raise PaySimInputError(f"row {source_row_index}: unexpected extra columns")
                pending.append(_parse_row(row, source_row_index, dataset_version))
                count += 1
                if len(pending) >= chunk_size:
                    chunk_path = Path(temp_dir.name) / f"chunk-{len(chunks):08d}.jsonl"
                    _write_chunk(pending, chunk_path)
                    chunks.append(chunk_path)
                    pending = []

        if pending:
            chunk_path = Path(temp_dir.name) / f"chunk-{len(chunks):08d}.jsonl"
            _write_chunk(pending, chunk_path)
            chunks.append(chunk_path)
    except FileNotFoundError as exc:
        temp_dir.cleanup()
        raise PaySimInputError(f"PaySim CSV not found: {path}") from exc
    except Exception:
        temp_dir.cleanup()
        raise

    return PreparedPaySim(temp_dir, chunks, count)


def load_paysim_csv(
    path: str | Path,
    *,
    dataset_version: str | None = None,
    max_events: int | None = None,
) -> list[PreparedTransaction]:
    """Compatibility helper for small tests; the CLI uses external sorting."""

    with prepare_paysim_csv(
        path, dataset_version=dataset_version, max_events=max_events
    ) as prepared:
        return list(prepared.iter_records())


def write_ground_truth(records: Iterable[PreparedTransaction], output: str | Path) -> None:
    """Atomically write labels to an offline CSV."""

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=output_path.parent,
            prefix=f".{output_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as csv_file:
            temporary_path = Path(csv_file.name)
            writer = csv.DictWriter(
                csv_file,
                fieldnames=("transactionId", "step", "isFraud", "isFlaggedFraud"),
            )
            writer.writeheader()
            for record in records:
                writer.writerow(record.ground_truth.to_json_dict())
            csv_file.flush()
            os.fsync(csv_file.fileno())
        os.replace(temporary_path, output_path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise
