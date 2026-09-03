#!/usr/bin/env python3
"""Build target-only labels from batch_etl resampled market data.

This is intentionally a separate executable from feature generation, while
sharing the same resampled-data input boundary. It writes one daily parquet
containing all requested cadences and target horizons.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import gcsfs
import numpy as np
import pandas as pd

VENUES = ("binance", "bitstamp", "coinbase", "crypto_com", "gemini")
FREQUENCIES = {"1s": 1, "5s": 5, "1m": 60, "5m": 300, "10m": 600, "30m": 1800, "1h": 3600}
HORIZONS = (60, 300, 900, 1800, 3600)
HORIZON_LABELS = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h"}


def _days(start: date, end: date):
    while start <= end:
        yield start
        start += timedelta(days=1)


def _load(fs, bucket: str, dataset: str, venue: str, frequency: str, day: date, hour: str | None = None) -> pd.DataFrame:
    selected_hour = hour or "*"
    pattern = f"{bucket}/{dataset.strip('/')}/frequency={frequency}/date={day}/hour={selected_hour}/venue={venue}/*.parquet"
    files = fs.glob(pattern)
    if not files:
        raise FileNotFoundError(f"missing {frequency} data for {venue} on {day}")
    paths = [file if file.startswith("gs://") else f"gs://{file}" for file in files]
    return pd.concat([pd.read_parquet(path, filesystem=fs) for path in paths], ignore_index=True)


def build_hour(fs, bucket: str, dataset: str, target: datetime, venues: tuple[str, ...]) -> pd.DataFrame:
    end = target + timedelta(hours=1)
    tables = []
    for frequency, bar_seconds in FREQUENCIES.items():
        venue_prices = []
        for venue in venues:
            # One extra hour is required for the 1h forward label.
            try:
                frame = _load(fs, bucket, dataset, venue, frequency, target.date(), f"{target:%H}")
            except FileNotFoundError:
                print(f"skipping missing {frequency} data for {venue}", flush=True)
                continue
            next_hour = end.strftime("%H")
            try:
                next_frame = _load(fs, bucket, dataset, venue, frequency, end.date(), next_hour)
            except FileNotFoundError:
                print(f"skipping {venue}: missing look-ahead hour for {frequency}", flush=True)
                continue
            frame = pd.concat([frame, next_frame], ignore_index=True)
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
            venue_prices.append(frame.set_index("timestamp")["p_trade_mean"].rename(venue))
        if not venue_prices:
            raise FileNotFoundError(f"no {frequency} venue data available for {target.isoformat()}")
        synthetic = pd.concat(venue_prices, axis=1).mean(axis=1, skipna=True).sort_index()
        returns = np.log(synthetic / synthetic.shift(1)).replace([np.inf, -np.inf], np.nan)
        result = pd.DataFrame({"timestamp": synthetic.index, "synthetic_price": synthetic})
        for horizon in HORIZONS:
            periods = max(1, int(np.ceil(horizon / bar_seconds)))
            result[f"target_rv_{HORIZON_LABELS[horizon]}"] = np.sqrt(
                returns.pow(2).rolling(periods, min_periods=periods).sum().shift(-periods)
            )
        result["frequency"] = frequency
        tables.append(result[(result["timestamp"] >= target) & (result["timestamp"] < end)])
    return pd.concat(tables, ignore_index=True).sort_values(["timestamp", "frequency"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-hour", help="UTC hour; defaults to the hour two hours ago")
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--input-dataset", default="processed/resampled_market_data")
    parser.add_argument("--output", default="gs://kalshi-crypto-tick-data/processed/future_realized_volatility")
    parser.add_argument("--venues", default=",".join(VENUES))
    args = parser.parse_args()
    venues = tuple(v.strip() for v in args.venues.split(",") if v.strip())
    fs = gcsfs.GCSFileSystem()
    target = (datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc)
              if args.target_hour else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=2))
    if target.minute or target.second or target.microsecond:
        parser.error("--target-hour must be aligned to an hour")
    output = f"{args.output.rstrip('/')}/date={target.date().isoformat()}/hour={target:%H}/targets.parquet"
    build_hour(fs, args.bucket, args.input_dataset, target, venues).to_parquet(
            output, filesystem=fs, index=False, compression="snappy"
        )
    print(f"wrote {output}", flush=True)


if __name__ == "__main__":
    main()
