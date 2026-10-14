import pandas as pd


def run(frame: pd.DataFrame, train_fraction: float, validation_fraction: float) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    steps = sorted(frame["step"].unique().tolist())
    if len(steps) < 3:
        raise ValueError("At least three distinct step values are required for chronological splitting")
    train_end_index = max(0, min(len(steps) - 3, int(len(steps) * train_fraction) - 1))
    validation_end_index = max(train_end_index + 1, min(len(steps) - 2, int(len(steps) * (train_fraction + validation_fraction)) - 1))
    train_end, validation_end = steps[train_end_index], steps[validation_end_index]
    split_frames = {
        "train": frame.loc[frame["step"] <= train_end].copy(),
        "validation": frame.loc[(frame["step"] > train_end) & (frame["step"] <= validation_end)].copy(),
        "test": frame.loc[frame["step"] > validation_end].copy(),
    }
    boundaries = {"train_step_max": int(train_end), "validation_step_max": int(validation_end), "test_step_min": int(validation_end + 1)}
    if any(part.empty for part in split_frames.values()):
        raise ValueError("Chronological split produced an empty partition")
    return split_frames, boundaries

