"""Backfill BigQuery bars, features, and labels over an hourly range."""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise argparse.ArgumentTypeError("hours must be aligned to UTC hour")
    return parsed


def _hours(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-hour", required=True, type=_hour)
    parser.add_argument("--end-hour", required=True, type=_hour)
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET_NAME", "kalshi-crypto-tick-data"))
    parser.add_argument("--venues", default=os.getenv("BATCH_ETL_VENUES"))
    parser.add_argument("--frequencies", default=os.getenv("BATCH_ETL_FREQUENCIES"))
    parser.add_argument("--parallelism", type=int, default=int(os.getenv("BATCH_ETL_PARALLELISM", "3")))
    phases = parser.add_mutually_exclusive_group()
    phases.add_argument("--resample-only", action="store_true")
    phases.add_argument("--features-only", action="store_true")
    phases.add_argument("--targets-only", action="store_true")
    args = parser.parse_args()
    if args.end_hour < args.start_hour:
        parser.error("end-hour must not precede start-hour")

    root = Path(__file__).resolve().parent
    resample = root / "run_bigquery_hourly.py"
    feature = root / "run_bigquery_features.py"
    target = root / "run_bigquery_targets.py"
    common = ["--project", args.project, "--parallelism", str(args.parallelism)]
    if args.bucket:
        common += ["--bucket", args.bucket]
    if args.venues:
        common += ["--venues", args.venues]
    if args.frequencies:
        common += ["--frequencies", args.frequencies]

    for current in _hours(args.start_hour, args.end_hour):
        stamp = current.strftime("%Y-%m-%dT%H:00:00Z")
        if not args.features_only and not args.targets_only:
            command = [sys.executable, str(resample), *common, "--target-hour", stamp]
            print("running", " ".join(command), flush=True)
            subprocess.run(command, check=True)
        if not args.resample_only and not args.targets_only:
            command = [sys.executable, str(feature), "--project", args.project, "--target-hour", stamp]
            print("running", " ".join(command), flush=True)
            subprocess.run(command, check=True)
        if not args.resample_only and not args.features_only:
            command = [sys.executable, str(target), "--project", args.project, "--target-hour", stamp]
            print("running", " ".join(command), flush=True)
            subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
