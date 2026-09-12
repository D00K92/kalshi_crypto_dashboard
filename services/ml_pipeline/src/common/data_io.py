"""Shared training-data loading for legacy GCS and Feast/BigQuery paths."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from src.common.contracts import CURRENT_CONTRACT_VERSION, resolve_contract

DEFAULT_TARGET_TABLE = "kalshi-crypto-506614.training_labels.future_realized_volatility_v2_10s"

REQUIRED_FREQUENCIES = {"10s"}
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
    feature_version: str = CURRENT_CONTRACT_VERSION,
) -> pd.DataFrame:
    """Load the BigQuery tables declared by the immutable Feast contract.

    This contract is exact-timestamp aligned at 10-second boundaries.  Joining
    the two partitioned tables directly avoids Feast's range join over the full
    feature-view TTL while preserving point-in-time correctness.
    """
    del feast_repo  # Retained in the public loader boundary for pipeline compatibility.
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end > yesterday:
        raise ValueError(f"training end date {end} exceeds policy cutoff {yesterday}")
    from google.cloud import bigquery

    contract = resolve_contract(feature_version)
    selected_features = tuple(dict.fromkeys(("log_return", "venue_count", *contract.feature_columns)))
    feature_select = ",\n        ".join(f"f.`{column}`" for column in selected_features)

    client = bigquery.Client(project=project, location="asia-northeast3")
    query = f"""
      SELECT
        f.asset,
        f.event_timestamp,
        f.synthetic_price,
        {feature_select},
        l.market_id,
        l.prediction_timestamp,
        l.label_window_end,
        l.target_rv_1m,
        l.target_rv_5m,
        l.target_rv_15m,
        l.target_rv_30m,
        l.target_rv_1h,
        l.label_version
      FROM `{target_table}` AS l
      INNER JOIN `{contract.offline_table}` AS f
        ON f.asset = l.market_id
       AND f.event_timestamp = l.prediction_timestamp
       AND f.feature_version = @feature_version
      WHERE DATE(l.prediction_timestamp) BETWEEN @start_date AND @end_date
        AND l.target_rv_1h IS NOT NULL
        AND l.label_window_end <= CURRENT_TIMESTAMP()
        AND l.label_version = @label_version
    """
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
        bigquery.ScalarQueryParameter("feature_version", "STRING", contract.feature_version),
        bigquery.ScalarQueryParameter("label_version", "STRING", contract.label_version),
    ])
    joined = client.query(query, job_config=config).to_dataframe()
    if joined.empty:
        raise FileNotFoundError("BigQuery contract tables have no usable feature/label matches")
    joined["event_timestamp"] = pd.to_datetime(joined["event_timestamp"], utc=True)
    joined["prediction_timestamp"] = pd.to_datetime(joined["prediction_timestamp"], utc=True)
    joined["frequency"] = "10s"
    joined["timestamp"] = joined["event_timestamp"]
    joined.attrs["feature_contract"] = contract.feature_version
    joined.attrs["training_cutoff"] = end.isoformat()
    validate_training_table(
        joined,
        require_all_frequencies=False,
    )
    return joined
