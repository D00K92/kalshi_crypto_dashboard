"""Load batch_etl resampled venue data from GCS."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, timedelta

import gcsfs
import pandas as pd

DEFAULT_BUCKET = "kalshi-crypto-tick-data"
DEFAULT_DATASET = "processed/resampled_market_data"
COMPLETED_VENUES = ("binance", "bitstamp", "coinbase", "crypto_com", "gemini")
FREQUENCIES = ("1s", "5s", "1m", "5m", "10m", "30m", "1h")


def _dates(start: date, end: date) -> Iterable[date]:
    if end < start:
        raise ValueError("end date must not precede start date")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _paths(dataset: str, venue: str, frequency: str, start: date, end: date,
           hour: str | None = None) -> Iterable[str]:
    for day in _dates(start, end):
        hour_part = hour if hour is not None else "*"
        yield f"{dataset.strip('/')}/frequency={frequency}/date={day}/hour={hour_part}/venue={venue}/*.parquet"


def load_venue_frame(fs: gcsfs.GCSFileSystem, *, bucket: str = DEFAULT_BUCKET,
                     dataset: str = DEFAULT_DATASET, venue: str, frequency: str,
                     start: date, end: date, hour: str | None = None) -> pd.DataFrame:
    """Load and concatenate one venue's resampled partitions."""
    if frequency not in FREQUENCIES:
        raise ValueError(f"unsupported frequency: {frequency}")
    files = [file for pattern in _paths(dataset, venue, frequency, start, end, hour)
             for file in fs.glob(f"{bucket}/{pattern}")]
    if not files:
        raise FileNotFoundError(f"no resampled files for venue={venue}, frequency={frequency}")
    paths = [file if file.startswith("gs://") else f"gs://{file}" for file in files]
    return pd.concat([pd.read_parquet(path, filesystem=fs) for path in paths], ignore_index=True)


def load_completed_venues(fs: gcsfs.GCSFileSystem, *, venues: Iterable[str] = COMPLETED_VENUES,
                          bucket: str = DEFAULT_BUCKET, dataset: str = DEFAULT_DATASET,
                          frequency: str, start: date, end: date,
                          hour: str | None = None) -> Mapping[str, pd.DataFrame]:
    """Load all selected venues; missing data fails explicitly."""
    return {venue: load_venue_frame(fs, bucket=bucket, dataset=dataset, venue=venue,
                                    frequency=frequency, start=start, end=end, hour=hour)
            for venue in venues}
