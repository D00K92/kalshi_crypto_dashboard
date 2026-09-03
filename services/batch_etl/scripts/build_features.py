"""Build one hourly, venue-agnostic v1 feature parquet partition."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import gcsfs
import pandas as pd

from kalshi_crypto_batch_etl.features.load_data import COMPLETED_VENUES, FREQUENCIES, load_venue_frame
from kalshi_crypto_batch_etl.features.preprocess import prepare_venue_frames
from kalshi_crypto_batch_etl.features.v1_features import build_v1_dataset_by_frequency


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-hour", help="UTC hour; defaults to the previous complete hour")
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--input-dataset", default="processed/resampled_market_data")
    parser.add_argument("--output", default="features/v1")
    parser.add_argument("--venues", default=','.join(COMPLETED_VENUES))
    parser.add_argument("--frequencies", default=','.join(FREQUENCIES))
    parser.add_argument("--lookback-hours", type=int, default=5)
    args = parser.parse_args()
    target = (datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc)
              if args.target_hour else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
    if not args.target_hour:
        target -= timedelta(hours=1)
    if args.lookback_hours < 1:
        parser.error("--lookback-hours must be positive")
    if target.minute or target.second or target.microsecond:
        parser.error("--target-hour must be aligned to an hour")
    fs = gcsfs.GCSFileSystem()
    venues, frequencies = tuple(args.venues.split(',')), tuple(args.frequencies.split(','))
    frames = {}
    for frequency in frequencies:
        seconds = int({'1s': 1, '5s': 5, '1m': 60, '5m': 300, '10m': 600, '30m': 1800, '1h': 3600}[frequency])
        loaded = {}
        for venue in venues:
            try:
                context = []
                for offset in range(args.lookback_hours, -1, -1):
                    hour = target - timedelta(hours=offset)
                    context.append(load_venue_frame(
                        fs, bucket=args.bucket, dataset=args.input_dataset, venue=venue,
                        frequency=frequency, start=hour.date(), end=hour.date(), hour=f"{hour:%H}"))
                loaded[venue] = pd.concat(context, ignore_index=True)
            except FileNotFoundError:
                print(f"skipping missing {frequency} data for {venue}", flush=True)
        if not loaded:
            raise FileNotFoundError(f"no {frequency} data available for {target:%Y-%m-%d} {target:%H}:00")
        frames[seconds] = prepare_venue_frames(loaded)
    result = build_v1_dataset_by_frequency(frames)
    end = target + timedelta(hours=1)
    result = result[(result["timestamp"] >= target) & (result["timestamp"] < end)]
    output = f"gs://{args.bucket}/{args.output.rstrip('/')}/date={target.date().isoformat()}/features.parquet"
    if fs.exists(output):
        existing = pd.read_parquet(output, filesystem=fs)
        result = pd.concat([existing, result], ignore_index=True).drop_duplicates(
            subset=["timestamp", "frequency"], keep="last").sort_values(["timestamp", "frequency"])
    result.to_parquet(output, filesystem=fs, index=False, compression="snappy")
    print(f"wrote {len(result)} rows to {output}", flush=True)


if __name__ == "__main__":
    main()
