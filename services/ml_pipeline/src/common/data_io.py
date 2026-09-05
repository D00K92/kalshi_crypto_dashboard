"""Shared training-data loading for legacy GCS and Feast/BigQuery paths."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone

import pandas as pd

DEFAULT_TARGET_TABLE = "kalshi-crypto-506614.training_labels.future_realized_volatility_v1"

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


def validate_training_table(table: pd.DataFrame, *, require_all_frequencies: bool = True) -> None:
    """Reject incomplete or malformed joined data before model training."""
    missing = REQUIRED_TARGETS.difference(table.columns)
    if missing:
        raise ValueError(f"training data missing targets: {sorted(missing)}")
    if require_all_frequencies and not REQUIRED_FREQUENCIES.issubset(set(table["frequency"].dropna().unique())):
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


def load_training_table_from_feast(
    *, project: str, feast_repo: str, start: date, end: date,
    target_table: str = DEFAULT_TARGET_TABLE,
) -> pd.DataFrame:
    """Load BigQuery labels and attach point-in-time Feast features.

    Labels are keyed by ``prediction_timestamp``.  Feast receives that same
    timestamp as ``event_timestamp`` so no feature after the prediction cutoff
    can enter the training row.
    """
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end > yesterday:
        raise ValueError(f"training end date {end} exceeds policy cutoff {yesterday}")
    from google.cloud import bigquery
    from feast import FeatureStore

    client = bigquery.Client(project=project, location="asia-northeast3")
    query = f"""
      SELECT market_id, prediction_timestamp, label_window_end,
             target_rv_1m, target_rv_5m, target_rv_15m,
             target_rv_30m, target_rv_1h, label_version
      FROM `{target_table}`
      WHERE DATE(prediction_timestamp) BETWEEN @start_date AND @end_date
        AND target_rv_1h IS NOT NULL
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
    ])
    labels = client.query(query, job_config=config).to_dataframe()
    if labels.empty:
        raise FileNotFoundError("BigQuery target table has no usable labels for the requested range")
    labels["prediction_timestamp"] = pd.to_datetime(labels["prediction_timestamp"], utc=True)
    entities = labels.rename(columns={"market_id": "asset", "prediction_timestamp": "event_timestamp"})[
        ["asset", "event_timestamp"]
    ]
    features = FeatureStore(repo_path=feast_repo).get_historical_features(
        entity_df=entities.sort_values("event_timestamp"),
        features=[
            "v1_market_features:synthetic_price",
            "v1_market_features:log_return",
            "v1_market_features:venue_count",
            "v1_market_features:realized_vol_1h",
            "v1_market_features:realized_vol_3h",
        ],
    ).to_df()
    features["event_timestamp"] = pd.to_datetime(features["event_timestamp"], utc=True)
    joined = features.merge(
        labels, left_on=["asset", "event_timestamp"],
        right_on=["market_id", "prediction_timestamp"], how="inner",
    )
    if joined.empty:
        raise ValueError("Feast historical retrieval produced no label/feature matches")
    joined["frequency"] = "1m"
    joined["timestamp"] = joined["event_timestamp"]
    joined.attrs["training_cutoff"] = end.isoformat()
    validate_training_table(
        joined,
        require_all_frequencies=False,
    )
    return joined
