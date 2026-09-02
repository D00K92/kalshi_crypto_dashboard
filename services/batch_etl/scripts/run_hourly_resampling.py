#!/usr/bin/env python3
"""Resample the previous complete UTC hour into idempotent GCS partitions."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import os

import gcsfs
import pandas as pd

if __package__:
    from .build_resampled_market_data import (
        DEFAULT_BUCKET,
        DEFAULT_OUTPUT_DATASET,
        FREQUENCIES,
        VENUES,
        _resample_venue,
        write_hour_partition,
    )
else:
    from build_resampled_market_data import (
        DEFAULT_BUCKET,
        DEFAULT_OUTPUT_DATASET,
        FREQUENCIES,
        VENUES,
        _resample_venue,
        write_hour_partition,
    )


PRODUCTION_FREQUENCIES = ("1s", "5s", "1m", "5m", "10m", "30m", "1h")
EXPECTED_ROWS_PER_HOUR = {
    "1s": 3600,
    "5s": 720,
    "1m": 60,
    "5m": 12,
    "10m": 6,
    "30m": 2,
    "1h": 1,
}


def parse_target_hour(value: str | None, now: datetime | None = None) -> datetime:
    if value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("target hour must include a timezone")
        parsed = parsed.astimezone(timezone.utc)
        if parsed.minute or parsed.second or parsed.microsecond:
            raise ValueError("target hour must be aligned to an hour")
        return parsed

    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return current.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def parse_selection(value: str, allowed: tuple[str, ...], name: str) -> tuple[str, ...]:
    selected = tuple(item.strip() for item in value.split(",") if item.strip())
    invalid = sorted(set(selected) - set(allowed))
    if not selected or invalid:
        raise ValueError(f"invalid {name}: {invalid or value}")
    return selected


def run_hourly(
    fs: gcsfs.GCSFileSystem,
    bucket: str,
    output_dataset: str,
    target: datetime,
    venues: tuple[str, ...],
    frequencies: tuple[str, ...],
) -> None:
    previous = target - timedelta(hours=1)
    source_partitions = (
        (previous.date(), previous.strftime("%H")),
        (target.date(), target.strftime("%H")),
    )
    target_end = target + timedelta(hours=1)

    for frequency in frequencies:
        frequency_output = f"gs://{bucket}/{output_dataset.strip('/')}/frequency={frequency}"
        expected_rows = EXPECTED_ROWS_PER_HOUR[frequency]
        for venue in venues:
            print(f"resampling target={target.isoformat()} venue={venue} frequency={frequency}", flush=True)
            result = _resample_venue(
                fs=fs,
                bucket=bucket,
                venue=venue,
                start=target.date(),
                end=target.date(),
                freq=FREQUENCIES[frequency],
                hour=target.strftime("%H"),
                source_partitions=source_partitions,
            )
            target_result = result[
                (result["timestamp"] >= pd.Timestamp(target))
                & (result["timestamp"] < pd.Timestamp(target_end))
            ].persist()
            actual_rows = int(target_result.shape[0].compute())
            if actual_rows != expected_rows:
                raise RuntimeError(
                    f"incomplete output for {venue} {frequency}: "
                    f"expected {expected_rows} rows, got {actual_rows}"
                )
            destination = write_hour_partition(
                target_result,
                frequency_output,
                target.date(),
                target.strftime("%H"),
                venue,
            )
            print(f"wrote rows={actual_rows} destination={destination}", flush=True)
            del target_result, result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET_NAME", DEFAULT_BUCKET))
    parser.add_argument(
        "--output-dataset",
        default=os.getenv("BATCH_ETL_OUTPUT_DATASET", DEFAULT_OUTPUT_DATASET),
    )
    parser.add_argument("--target-hour", default=os.getenv("BATCH_ETL_TARGET_HOUR"))
    parser.add_argument("--venues", default=os.getenv("BATCH_ETL_VENUES", ",".join(VENUES)))
    parser.add_argument(
        "--frequencies",
        default=os.getenv("BATCH_ETL_FREQUENCIES", ",".join(PRODUCTION_FREQUENCIES)),
    )
    args = parser.parse_args()

    try:
        target = parse_target_hour(args.target_hour)
        venues = parse_selection(args.venues, tuple(VENUES), "venues")
        frequencies = parse_selection(args.frequencies, PRODUCTION_FREQUENCIES, "frequencies")
    except ValueError as exc:
        parser.error(str(exc))

    run_hourly(
        fs=gcsfs.GCSFileSystem(),
        bucket=args.bucket,
        output_dataset=args.output_dataset,
        target=target,
        venues=venues,
        frequencies=frequencies,
    )


if __name__ == "__main__":
    main()
