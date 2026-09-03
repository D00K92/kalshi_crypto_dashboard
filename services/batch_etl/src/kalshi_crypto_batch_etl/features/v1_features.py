"""Venue-local v1 features and equal-weight synthetic-price targets.

This module deliberately excludes cross-venue predictors.  It operates on one
venue's resampled frame at a time; callers can combine the resulting rows by
``timestamp`` and ``frequency``.
"""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

WINDOWS_SECONDS = (30, 60, 300, 900, 1800, 3600)
TARGET_HORIZONS_SECONDS = (60, 300, 900, 1800, 3600)
BOOK_LEVELS = (1, 5, 10)
FREQUENCY_LABELS = {1: "1s", 5: "5s", 60: "1m", 300: "5m", 600: "10m", 1800: "30m", 3600: "1h"}


def _ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.div(denominator.replace(0, np.nan))


def _periods(seconds: int, bar_seconds: int) -> int:
    return max(1, int(np.ceil(seconds / bar_seconds)))


def _require(frame: pd.DataFrame) -> None:
    required = {
        "timestamp", "p_trade", "p_open", "p_close", "p_high", "p_low",
        "p_trade_mean", "v_trade", "v_buy", "v_sell",
        *(f"p_{side}_{level}" for side in ("bid", "ask") for level in range(1, 11)),
        *(f"q_{side}_{level}" for side in ("bid", "ask") for level in range(1, 11)),
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"missing v1 input columns: {missing}")


def compute_v1_features(frame: pd.DataFrame, bar_seconds: int, venue: str) -> pd.DataFrame:
    """Compute non-cross-venue v1 features for one venue."""
    _require(frame)
    if bar_seconds <= 0:
        raise ValueError("bar_seconds must be positive")
    pdf = frame.sort_values("timestamp").reset_index(drop=True).copy()
    out = pdf[["timestamp"]].copy()
    out["venue"] = venue
    out["asset"] = "BTC"
    out["frequency_seconds"] = bar_seconds

    price = pdf["p_trade_mean"].fillna(pdf["p_trade"])
    returns = np.log(price / price.shift(1)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["trade_log_return"] = returns
    out["aggressor_imbalance"] = _ratio(pdf["v_buy"] - pdf["v_sell"], pdf["v_buy"] + pdf["v_sell"])

    for level in BOOK_LEVELS:
        bp, ap = pdf[f"p_bid_{level}"], pdf[f"p_ask_{level}"]
        bq, aq = pdf[f"q_bid_{level}"], pdf[f"q_ask_{level}"]
        out[f"wap_{level}"] = _ratio(bp * aq + ap * bq, bq + aq)
        out[f"microprice_{level}"] = out[f"wap_{level}"]
        out[f"obi_{level}"] = _ratio(bq - aq, bq + aq)

    out["spread"] = pdf["p_ask_1"] - pdf["p_bid_1"]
    out["relative_spread"] = _ratio(out["spread"], out["wap_1"])
    out["book_slope_bid"] = _ratio(pdf[[f"q_bid_{i}" for i in range(1, 11)]].sum(axis=1), pdf["p_bid_1"] - pdf["p_bid_10"])
    out["book_slope_ask"] = _ratio(pdf[[f"q_ask_{i}" for i in range(1, 11)]].sum(axis=1), pdf["p_ask_10"] - pdf["p_ask_1"])
    out["liquidity_consumption"] = _ratio(pdf["v_trade"], pdf["q_bid_1"] + pdf["q_ask_1"])

    bid_delta = pdf["p_bid_1"].diff()
    ask_delta = pdf["p_ask_1"].diff()
    bid_flow = np.where(bid_delta > 0, pdf["q_bid_1"], np.where(bid_delta == 0, pdf["q_bid_1"].diff(), 0.0))
    ask_flow = np.where(ask_delta > 0, 0.0, np.where(ask_delta == 0, pdf["q_ask_1"].diff(), pdf["q_ask_1"]))
    out["ofi"] = pd.Series(bid_flow - ask_flow).fillna(0.0)

    log_hl = np.log(pdf["p_high"] / pdf["p_low"]).replace([np.inf, -np.inf], np.nan)
    log_co = np.log(pdf["p_close"] / pdf["p_open"]).replace([np.inf, -np.inf], np.nan)
    log_ho = np.log(pdf["p_high"] / pdf["p_open"]).replace([np.inf, -np.inf], np.nan)
    log_lo = np.log(pdf["p_low"] / pdf["p_open"]).replace([np.inf, -np.inf], np.nan)
    for window in WINDOWS_SECONDS:
        n = _periods(window, bar_seconds)
        squared = returns.pow(2)
        rv2 = squared.rolling(n, min_periods=max(2, n // 5)).sum()
        bv2 = (np.pi / 2 * returns.abs() * returns.abs().shift(1)).rolling(n, min_periods=max(2, n // 5)).sum()
        out[f"rv_{window}s"] = np.sqrt(rv2)
        out[f"bv_{window}s"] = np.sqrt(bv2)
        out[f"jump_component_{window}s"] = (rv2 - bv2).clip(lower=0)
        gk = 0.511 * log_hl.pow(2) - 0.019 * (log_co * np.log((pdf["p_high"] * pdf["p_low"]) / pdf["p_open"].pow(2)) - 2 * log_ho * log_lo) - 0.383 * log_co.pow(2)
        out[f"gk_vol_{window}s"] = np.sqrt(gk.clip(lower=0).rolling(n, min_periods=max(2, n // 5)).mean())
    return out


def compute_synthetic_targets(frames: Mapping[str, pd.DataFrame], bar_seconds: int) -> pd.DataFrame:
    """Build future volatility labels from equal-weight venue trade means."""
    if not frames:
        raise ValueError("at least one venue frame is required")
    prices = [df.set_index("timestamp")["p_trade_mean"].rename(venue) for venue, df in frames.items()]
    synthetic = pd.concat(prices, axis=1).mean(axis=1, skipna=True).sort_index()
    returns = np.log(synthetic / synthetic.shift(1)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    result = pd.DataFrame({"timestamp": synthetic.index, "synthetic_price": synthetic})
    for horizon in TARGET_HORIZONS_SECONDS:
        n = _periods(horizon, bar_seconds)
        result[f"target_vol_{horizon}s"] = np.sqrt(returns.pow(2).rolling(n, min_periods=n).sum().shift(-n))
    return result.reset_index(drop=True)


def build_v1_dataset(frames: Mapping[str, pd.DataFrame], bar_seconds: int) -> pd.DataFrame:
    """Build one venue-agnostic training table from completed venue frames.

    Venue-local measurements are equal-weight averaged by timestamp.  The
    synthetic-price targets use the same venue set, so the output is one row
    per timestamp/frequency rather than one file or column family per venue.
    """
    if not frames:
        raise ValueError("at least one venue frame is required")
    feature_frames = [
        compute_v1_features(frame, bar_seconds, venue).drop(columns="venue")
        for venue, frame in frames.items()
    ]
    stacked = pd.concat(feature_frames, ignore_index=True)
    numeric = stacked.select_dtypes(include=[np.number]).columns
    features = stacked.groupby("timestamp", as_index=False)[list(numeric)].mean()
    features["asset"] = "BTC"
    features["frequency_seconds"] = bar_seconds
    features["frequency"] = FREQUENCY_LABELS.get(bar_seconds, f"{bar_seconds}s")
    targets = compute_synthetic_targets(frames, bar_seconds)
    return features.merge(targets, on="timestamp", how="left").sort_values("timestamp").reset_index(drop=True)


def build_v1_dataset_by_frequency(
    frames_by_frequency: Mapping[int, Mapping[str, pd.DataFrame]],
) -> pd.DataFrame:
    """Combine all requested frequencies into the single v1 output table."""
    if not frames_by_frequency:
        raise ValueError("at least one frequency is required")
    tables = [build_v1_dataset(frames, seconds) for seconds, frames in frames_by_frequency.items()]
    return pd.concat(tables, ignore_index=True).sort_values(
        ["timestamp", "frequency_seconds"]
    ).reset_index(drop=True)
