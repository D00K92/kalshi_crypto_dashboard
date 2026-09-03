"""Resumable hourly backfill for feature and future-volatility partitions."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import gcsfs


def hours(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-hour", required=True, help="UTC hour, inclusive")
    parser.add_argument("--end-hour", required=True, help="UTC hour, inclusive")
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--features-root", default="features/v1")
    parser.add_argument("--targets-root", default="processed/future_realized_volatility")
    parser.add_argument("--venues", default=None)
    parser.add_argument("--features-only", action="store_true")
    parser.add_argument("--targets-only", action="store_true")
    parser.add_argument("--resume", action="store_true", help="skip existing output partitions")
    args = parser.parse_args()
    if args.features_only and args.targets_only:
        parser.error("--features-only and --targets-only are mutually exclusive")
    parse = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    start, end = parse(args.start_hour), parse(args.end_hour)
    if start.minute or start.second or start.microsecond or end.minute or end.second or end.microsecond or end < start:
        parser.error("start/end must be aligned UTC hours and end must not precede start")
    fs = gcsfs.GCSFileSystem()
    for target in hours(start, end):
        stamp = target.strftime("%Y-%m-%dT%H:00:00Z")
        common = ["--target-hour", stamp, "--bucket", args.bucket]
        if args.venues:
            common += ["--venues", args.venues]
        feature_uri = f"{args.bucket}/{args.features_root.rstrip('/')}/date={target.date()}/features.parquet"
        target_uri = f"{args.bucket}/{args.targets_root.rstrip('/')}/date={target.date()}/hour={target:%H}/targets.parquet"
        commands = []
        if not args.targets_only and not (args.resume and fs.exists(feature_uri)):
            commands.append([sys.executable, "scripts/build_features.py", *common, "--output", args.features_root])
        if not args.features_only and not (args.resume and fs.exists(target_uri)):
            commands.append([sys.executable, "scripts/build_future_volatility_targets.py", *common, "--output", f"gs://{args.bucket}/{args.targets_root}"])
        for command in commands:
            print("running", " ".join(command), flush=True)
            subprocess.run(command, check=True)
        if not commands:
            print(f"skipping {stamp}: outputs already exist", flush=True)


if __name__ == "__main__":
    main()
