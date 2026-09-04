#!/usr/bin/env python3
"""Resample the previous complete UTC hour into idempotent GCS partitions."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import os

import dask
import gcsfs
import pandas as pd

from kalshi_crypto_batch_etl.bigquery.io import write_frame as write_bigquery_partition

if __package__:
    from .build_resampled_market_data import (
        DEFAULT_BUCKET,
        DEFAULT_OUTPUT_DATASET,
        FREQUENCIES,
        VENUES,
        _load_venue_events,
        _resample_events,
        write_hour_partition,
    )
else:
    from build_resampled_market_data import (
        DEFAULT_BUCKET,
        DEFAULT_OUTPUT_DATASET,
        FREQUENCIES,
        VENUES,
        _load_venue_events,
        _resample_events,
        write_hour_partition,
    )


PRODUCTION_FREQUENCIES = ("1s", "5s", "1m", "5m", "10m", "30m", "1h")
BIGQUERY_BAR_COLUMNS = (
    "event_timestamp", "created_timestamp", "venue", "instrument", "frequency",
    "p_open", "p_high", "p_low", "p_close", "p_trade", "p_trade_mean",
    "v_trade", "v_buy", "v_sell", "cnt_trade", "dt_fill_mean_ms", "dt_fill_max_ms", "dt_fill_min_ms",
    *[name for level in range(1, 11) for name in (
        f"p_bid_{level}", f"p_ask_{level}", f"q_bid_{level}", f"q_ask_{level}")],
)
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
    bigquery_table: str | None = None,
    bigquery_project: str = "kalshi-crypto-506614",
) -> None:
    previous = target - timedelta(hours=1)
    source_partitions = (
        (previous.date(), previous.strftime("%H")),
        (target.date(), target.strftime("%H")),
    )
    target_end = target + timedelta(hours=1)

    for venue in venues:
        print(f"loading target={target.isoformat()} venue={venue}", flush=True)
        tick_indexed, book_indexed = _load_venue_events(
            fs=fs,
            bucket=bucket,
            venue=venue,
            start=target.date(),
            end=target.date(),
            hour=target.strftime("%H"),
            source_partitions=source_partitions,
        )
        tick_indexed = tick_indexed.persist()
        book_indexed = book_indexed.persist()

        # Build one task graph per venue so all requested cadences can execute
        # concurrently against the same persisted raw inputs.
        def resample_frequency(frequency: str):
            # Keep persisted Dask collections in the closure. Passing them as
            # delayed arguments causes Dask to unwrap them into pandas frames.
            return _resample_events(
                tick_indexed,
                book_indexed,
                venue,
                FREQUENCIES[frequency],
                start=pd.Timestamp(previous),
                end=pd.Timestamp(target_end),
            )

        tasks = [dask.delayed(resample_frequency)(frequency) for frequency in frequencies]
        results = dask.compute(
            *tasks,
            scheduler="threads",
            num_workers=max(1, int(os.getenv("DASK_RESAMPLE_WORKERS", "4"))),
        )

        for frequency, result in zip(frequencies, results, strict=True):
            frequency_output = f"gs://{bucket}/{output_dataset.strip('/')}/frequency={frequency}"
            expected_rows = EXPECTED_ROWS_PER_HOUR[frequency]
            print(f"writing target={target.isoformat()} venue={venue} frequency={frequency}", flush=True)
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
            if bigquery_table:
                bq_frame = target_result.compute()
                bq_frame = bq_frame.rename_axis("event_timestamp").reset_index()
                bq_frame["created_timestamp"] = pd.Timestamp.now(tz="UTC")
                bq_frame["venue"] = venue
                bq_frame["instrument"] = "BTCUSDT" if venue == "binance" else "BTCUSD"
                bq_frame["frequency"] = frequency
                missing = sorted(set(BIGQUERY_BAR_COLUMNS) - set(bq_frame.columns))
                if missing:
                    raise RuntimeError(f"BigQuery bar output missing columns: {missing}")
                bq_frame["cnt_trade"] = pd.to_numeric(bq_frame["cnt_trade"], errors="raise").fillna(0).astype("int64")
                bq_frame = bq_frame[list(BIGQUERY_BAR_COLUMNS)]
                parts = bigquery_table.split(".")
                table = bigquery_table if len(parts) == 3 else f"{bigquery_project}.market_data.{parts[-1]}"
                written = write_bigquery_partition(
                    bq_frame,
                    table=table,
                    partition_date=target.date(),
                    time_start=target,
                    time_end=target_end,
                    filters={"venue": venue, "instrument": bq_frame["instrument"].iloc[0], "frequency": frequency},
                )
                print(f"wrote rows={written} destination=bigquery:{table}", flush=True)
            del target_result, result

        del tick_indexed, book_indexed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET_NAME", DEFAULT_BUCKET))
    parser.add_argument(
        "--output-dataset",
        default=os.getenv("BATCH_ETL_OUTPUT_DATASET", DEFAULT_OUTPUT_DATASET),
    )
    parser.add_argument(
        "--bigquery-table",
        default=os.getenv("BATCH_ETL_BIGQUERY_TABLE", "bars"),
        help="BigQuery canonical table (bars or project.dataset.table); set empty to disable.",
    )
    parser.add_argument("--bigquery-project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
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
        bigquery_table=args.bigquery_table,
        bigquery_project=args.bigquery_project,
    )


if __name__ == "__main__":
    main()
