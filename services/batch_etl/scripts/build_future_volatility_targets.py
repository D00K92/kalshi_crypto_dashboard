"""Build target labels with one Dask graph across venues and frequencies."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import dask
import gcsfs
import numpy as np
import pandas as pd

VENUES = ("binance", "bitstamp", "coinbase", "crypto.com", "gemini", "kraken")
FREQUENCIES = {"1s": 1, "5s": 5, "1m": 60, "5m": 300, "10m": 600, "30m": 1800, "1h": 3600}
HORIZONS = (60, 300, 900, 1800, 3600)
LABELS = {60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h"}
SECONDS_PER_YEAR = 365 * 24 * 60 * 60  # crypto trades continuously

def _paths(fs, bucket, dataset, venue, frequency, hour):
    pattern = f"{bucket}/{dataset.strip('/')}/frequency={frequency}/date={hour.date()}/hour={hour:%H}/venue={venue}/*.parquet"
    return [p if p.startswith("gs://") else f"gs://{p}" for p in fs.glob(pattern)]

def _read_pair(paths, next_paths):
    return pd.concat([pd.read_parquet(path) for path in paths + next_paths], ignore_index=True)

@dask.delayed
def _build_frequency(frames, frequency, bar_seconds, target):
    end = target + timedelta(hours=1)
    prices = []
    for frame, venue in frames:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        prices.append(frame.set_index("timestamp")["p_trade_mean"].rename(venue))
    synthetic = pd.concat(prices, axis=1).mean(axis=1, skipna=True).sort_index()
    returns = np.log(synthetic / synthetic.shift(1)).replace([np.inf, -np.inf], np.nan)
    result = pd.DataFrame({"timestamp": synthetic.index, "synthetic_price": synthetic})
    for horizon in HORIZONS:
        periods = max(1, int(np.ceil(horizon / bar_seconds)))
        realized = np.sqrt(returns.pow(2).rolling(periods, min_periods=periods).sum().shift(-periods))
        result[f"target_rv_{LABELS[horizon]}"] = realized * np.sqrt(SECONDS_PER_YEAR / horizon)
    result["frequency"] = frequency
    return result[(result["timestamp"] >= target) & (result["timestamp"] < end)]

def build_hour(fs, bucket, dataset, target, venues):
    tasks, next_hour = [], target + timedelta(hours=1)
    for frequency, bar_seconds in FREQUENCIES.items():
        venue_tasks = []
        for venue in venues:
            paths, next_paths = _paths(fs, bucket, dataset, venue, frequency, target), _paths(fs, bucket, dataset, venue, frequency, next_hour)
            if not paths or not next_paths:
                print(f"skipping {frequency} data for {venue}: missing target/look-ahead partition", flush=True)
                continue
            venue_tasks.append((dask.delayed(_read_pair)(paths, next_paths), venue))
        if not venue_tasks:
            raise FileNotFoundError(f"no {frequency} venue data available for {target.isoformat()}")
        tasks.append(_build_frequency(venue_tasks, frequency, bar_seconds, target))
    return pd.concat(dask.compute(*tasks, scheduler="threads"), ignore_index=True).sort_values(["timestamp", "frequency"]).reset_index(drop=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-hour")
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--input-dataset", default="processed/resampled_market_data")
    parser.add_argument("--output", default="gs://kalshi-crypto-tick-data/processed/future_realized_volatility")
    parser.add_argument("--venues", default=','.join(VENUES))
    args = parser.parse_args()
    target = (datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc) if args.target_hour else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=2))
    if target.minute or target.second or target.microsecond:
        parser.error("--target-hour must be aligned to an hour")
    fs = gcsfs.GCSFileSystem()
    output = f"{args.output.rstrip('/')}/date={target.date()}/hour={target:%H}/targets.parquet"
    build_hour(fs, args.bucket, args.input_dataset, target, tuple(v for v in args.venues.split(',') if v)).to_parquet(output, filesystem=fs, index=False, compression="snappy")
    print(f"wrote {output}", flush=True)

if __name__ == "__main__":
    main()
