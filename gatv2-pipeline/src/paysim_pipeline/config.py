from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    input_csv: Path
    output_dir: Path
    dataset_version: str
    train_fraction: float
    validation_fraction: float
    test_fraction: float
    stages: tuple[str, ...]


def load_config(path: Path) -> PipelineConfig:
    with path.open("rb") as stream:
        raw = tomllib.load(stream)
    base = path.resolve().parent.parent
    dataset = raw["dataset"]
    split = raw["split"]
    pipeline = raw["pipeline"]
    fractions = tuple(float(split[key]) for key in ("train_fraction", "validation_fraction", "test_fraction"))
    if any(value <= 0 or value >= 1 for value in fractions) or abs(sum(fractions) - 1.0) > 1e-9:
        raise ValueError("train/validation/test fractions must be positive and sum to 1")
    stages = tuple(pipeline["stages"])
    if stages != ("load", "validate_clean", "chronological_split", "export"):
        raise ValueError("pipeline stages must be load -> validate_clean -> chronological_split -> export")
    return PipelineConfig(
        input_csv=(base / dataset["input_csv"]).resolve(),
        output_dir=(base / dataset["output_dir"]).resolve(),
        dataset_version=str(dataset["dataset_version"]),
        train_fraction=fractions[0], validation_fraction=fractions[1], test_fraction=fractions[2],
        stages=stages,
    )

