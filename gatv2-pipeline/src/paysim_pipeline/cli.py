import argparse
import json
from pathlib import Path

from paysim_pipeline.config import load_config
from paysim_pipeline.stages import chronological_split, eda, export, load, validate_clean


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare leakage-aware PaySim train/validation/test artifacts")
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline.toml"))
    parser.add_argument("--input-csv", type=Path, help="Override dataset.input_csv from TOML")
    parser.add_argument("--output-dir", type=Path, help="Override dataset.output_dir from TOML")
    parser.add_argument("--eda-only", action="store_true", help="Write the pre-clean EDA report and stop")
    args = parser.parse_args()
    config = load_config(args.config.resolve())
    if args.input_csv:
        config = config.__class__(**{**config.__dict__, "input_csv": args.input_csv.resolve()})
    if args.output_dir:
        config = config.__class__(**{**config.__dict__, "output_dir": args.output_dir.resolve()})
    print(f"[load] {config.input_csv}")
    source = load.run(config.input_csv)
    print("[eda] pre-clean dataset exploration")
    eda_result = eda.run(source, config.output_dir / "eda", config.dataset_version, config.input_csv.name)
    print(json.dumps(eda_result, indent=2))
    if args.eda_only:
        return
    print(f"[validate_clean] {len(source):,} rows")
    accepted, rejected = validate_clean.run(source)
    print(f"[chronological_split] accepted={len(accepted):,}, rejected={len(rejected):,}")
    splits, boundaries = chronological_split.run(accepted, config.train_fraction, config.validation_fraction)
    print("[export]")
    manifest = export.run(splits, rejected, config.output_dir, config.dataset_version, config.input_csv, boundaries)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()

