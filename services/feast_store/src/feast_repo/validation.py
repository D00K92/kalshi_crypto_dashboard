"""Feature-store contract and freshness validation hooks."""

from __future__ import annotations

import pandas as pd

from registry import resolve_feature_spec


def validate_feature_schema(frame: pd.DataFrame, *, version: str) -> None:
    """Validate the immutable v1 source shape before materialization or training."""
    if frame.empty:
        raise ValueError("feature frame is empty")
    spec = resolve_feature_spec("market_features", version)
    required = {"asset", "event_timestamp", *spec.fields}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"feature frame missing columns: {missing}")
    if frame["asset"].isna().any() or (frame["asset"].astype(str).str.strip() == "").any():
        raise ValueError("feature frame has an empty asset entity")
    for name in spec.required_fields:
        if frame[name].isna().any():
            raise ValueError(f"feature frame has null required values: {name}")


def validate_feature_freshness(frame: pd.DataFrame, *, as_of) -> None:
    """Reject null, naive, or future event timestamps before historical use."""
    if "event_timestamp" not in frame:
        raise ValueError("feature frame missing event_timestamp")
    timestamps = pd.to_datetime(frame["event_timestamp"], utc=True, errors="coerce")
    if timestamps.isna().any():
        raise ValueError("feature frame has invalid event timestamps")
    cutoff = pd.Timestamp(as_of)
    if cutoff.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    if (timestamps > cutoff).any():
        raise ValueError("feature frame contains data after the requested as_of time")
