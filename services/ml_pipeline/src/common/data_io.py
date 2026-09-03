"""Shared GCS partition loading used by local jobs and ETL adapters."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone

import pandas as pd

REQUIRED_FREQUENCIES = {"1s", "5s", "1m", "5m", "10m", "30m", "1h"}
REQUIRED_TARGETS = {"target_rv_1m", "target_rv_5m", "target_rv_15m", "target_rv_30m", "target_rv_1h"}


def dates(start: date, end: date) -> Iterable[date]:
    if end < start:
        raise ValueError("end date must not precede start date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def gcs_uri(path: str) -> str:
    return path if path.startswith("gs://") else f"gs://{path}"


def validate_training_table(table: pd.DataFrame) -> None:
    """Reject incomplete or malformed joined data before model training."""
    missing = REQUIRED_TARGETS.difference(table.columns)
    if missing:
        raise ValueError(f"training data missing targets: {sorted(missing)}")
    if not REQUIRED_FREQUENCIES.issubset(set(table["frequency"].dropna().unique())):
        raise ValueError("training data is missing one or more required frequencies")
    if table.empty or table["timestamp"].isna().any():
        raise ValueError("training data is empty or contains null timestamps")
    if table[list(REQUIRED_TARGETS)].notna().sum().min() == 0:
        raise ValueError("training data has no usable target rows")


def load_training_table(fs, feature_root: str, target_root: str,
                        start: date, end: date) -> pd.DataFrame:
    """Load point-in-time-safe training data through yesterday's UTC cutoff.

    The one-hour target requires a complete future window, so samples after
    23:00 UTC on the eligible end date are excluded.
    """
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end > yesterday:
        raise ValueError(f"training end date {end} exceeds policy cutoff {yesterday}")
    cutoff = pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(hours=1)
    features = [
        pd.read_parquet(path, filesystem=fs)
        for day in dates(start, end)
        for path in [f"{feature_root.rstrip('/')}/date={day}/features.parquet"]
        if fs.exists(path)
    ]
    targets = [
        pd.read_parquet(gcs_uri(path), filesystem=fs)
        for day in dates(start, end)
        for path in fs.glob(f"{target_root.rstrip('/')}/date={day}/hour=*/targets.parquet")
    ]
    if not features or not targets:
        raise FileNotFoundError("feature or target partitions are missing")
    left, right = pd.concat(features, ignore_index=True), pd.concat(targets, ignore_index=True)
    left["timestamp"] = pd.to_datetime(left["timestamp"], utc=True)
    right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True)
    left = left[left["timestamp"] <= cutoff]
    right = right[right["timestamp"] <= cutoff]
    joined = left.merge(right, on=["timestamp", "frequency"], how="inner", suffixes=("", "_target"))
    if joined.empty:
        raise ValueError(f"no eligible feature/target rows at or before {cutoff.isoformat()}")
    validate_training_table(joined)
    joined.attrs["training_cutoff"] = cutoff.isoformat()
    return joined
