"""Build one hourly, venue-agnostic v1 feature parquet partition."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

import gcsfs

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
    args = parser.parse_args()
    target = (datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc)
              if args.target_hour else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
    if not args.target_hour:
        from datetime import timedelta
        target -= timedelta(hours=1)
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
                loaded[venue] = load_venue_frame(fs, bucket=args.bucket, dataset=args.input_dataset,
                                                 venue=venue, frequency=frequency, start=target.date(),
                                                 end=target.date(), hour=f"{target:%H}")
            except FileNotFoundError:
                print(f"skipping missing {frequency} data for {venue}", flush=True)
        if not loaded:
            raise FileNotFoundError(f"no {frequency} data available for {target:%Y-%m-%d} {target:%H}:00")
        frames[seconds] = prepare_venue_frames(loaded)
    result = build_v1_dataset_by_frequency(frames)
    output = f"gs://{args.bucket}/{args.output.rstrip('/')}/date={target.date().isoformat()}/features.parquet"
    result.to_parquet(output, filesystem=fs, index=False, compression="snappy")
    print(f"wrote {len(result)} rows to {output}", flush=True)


if __name__ == "__main__":
    main()
