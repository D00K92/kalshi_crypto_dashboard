"""Normalize and validate batch_etl frames before feature computation."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from kalshi_crypto_batch_etl.features.v1_features import _require


def prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a sorted, de-duplicated UTC frame with numeric market columns."""
    if "timestamp" not in frame:
        raise ValueError("resampled frame must contain timestamp")
    result = frame.copy()
    result["timestamp"] = pd.to_datetime(result["timestamp"], utc=True, errors="coerce")
    result = result.dropna(subset=["timestamp"]).sort_values("timestamp")
    result = result.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    for column in result.columns:
        if column not in {"timestamp", "venue", "asset"}:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    _require(result)
    return result


def prepare_venue_frames(frames: Mapping[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Normalize each venue independently while retaining venue keys."""
    if not frames:
        raise ValueError("at least one venue frame is required")
    return {venue: prepare_frame(frame) for venue, frame in frames.items()}
