"""Deterministic volatility benchmarks used for model evaluation."""

from __future__ import annotations

import numpy as np
import pandas as pd

SECONDS_PER_YEAR = 365 * 24 * 60 * 60


def ewma_annualized_volatility(table: pd.DataFrame, horizon: str, decay: float = 0.96) -> np.ndarray:
    """Forecast annualized volatility from prior same-frequency returns."""
    horizon_seconds = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}[horizon]
    required = {"timestamp", "frequency_seconds", "trade_log_return"}
    missing = required.difference(table.columns)
    if missing:
        raise ValueError(f"benchmark data missing columns: {sorted(missing)}")
    frame = table[["timestamp", "frequency_seconds", "trade_log_return"]].copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame["trade_log_return"] = pd.to_numeric(frame["trade_log_return"], errors="coerce").fillna(0.0)
    frame["_variance"] = frame.groupby("frequency_seconds", sort=False)["trade_log_return"].transform(
        lambda values: values.pow(2).ewm(alpha=1 - decay, adjust=False, min_periods=1).mean().shift(1)
    )
    frame["_variance"] = frame["_variance"].fillna(frame["trade_log_return"].pow(2))
    periods = np.maximum(1.0, horizon_seconds / frame["frequency_seconds"].astype(float))
    variance = frame["_variance"].to_numpy() * periods.to_numpy() * (SECONDS_PER_YEAR / horizon_seconds)
    return np.sqrt(np.maximum(variance, 1e-18))
