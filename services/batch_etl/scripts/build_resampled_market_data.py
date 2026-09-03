#!/usr/bin/env python3
"""Build evenly sampled market data from the raw GCS lake.

The source data is the current exporter layout::

    gs://<bucket>/ticks/venue=<venue>/instrument=<instrument>/date=<date>/hour=<hour>/*.parquet
    gs://<bucket>/books/venue=<venue>/instrument=<instrument>/date=<date>/hour=<hour>/*.parquet

Raw trade and book events are converted to the requested regular grid. Source
objects are never modified.

Example::

    UV_CACHE_DIR=/tmp/kalshi-batch-etl-uv-cache uv run \
      python scripts/build_resampled_market_data.py \
      --start-date 2026-09-01 --end-date 2026-09-01 \
      --venue binance --frequency 1s --overwrite
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import json
import os
from pathlib import Path
from typing import Iterable

os.environ.setdefault("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")
os.environ.setdefault("USE_EXPERIMENTAL_ADAPTIVE_PREFETCHING", "false")

import dask.dataframe as dd
import gcsfs
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


DEFAULT_BUCKET = "kalshi-crypto-tick-data"
DEFAULT_OUTPUT_DATASET = "processed/resampled_market_data"
BOOK_LEVELS = 10
VENUES = {
    "binance": "BTCUSDT",
    "bitstamp": "BTCUSD",
    "coinbase": "BTC-USD",
    "crypto.com": "BTCUSD",
    "gemini": "BTCUSD",
    "kraken": "BTC_USD",
}
FREQUENCIES = {
    "1s": "1s",
    "5s": "5s",
    "1m": "1min",
    "1min": "1min",
    "5m": "5min",
    "5min": "5min",
    "10m": "10min",
    "10min": "10min",
    "30m": "30min",
    "30min": "30min",
    "1h": "1h",
}


def hive_partitioning() -> ds.Partitioning:
    """Describe the Hive columns that exist in the path, not the files."""
    return ds.partitioning(
        schema=pa.schema(
            [
                ("venue", pa.string()),
                ("instrument", pa.string()),
                ("date", pa.string()),
                ("hour", pa.string()),
            ]
        ),
        flavor="hive",
    )


def dates_between(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def source_paths(bucket: str, dataset: str, venue: str, start: date, end: date, hour: str | None = None) -> list[str]:
    hours = [hour] if hour is not None else [f"{value:02d}" for value in range(24)]
    partitions = [(day, selected_hour) for day in dates_between(start, end) for selected_hour in hours]
    return source_partition_paths(bucket, dataset, venue, partitions)


def source_partition_paths(
    bucket: str,
    dataset: str,
    venue: str,
    partitions: Iterable[tuple[date, str]],
) -> list[str]:
    """Build raw paths for an explicit set of UTC hour partitions."""
    instrument = VENUES[venue]
    return [
        f"gs://{bucket}/{dataset}/venue={venue}/instrument={instrument}/"
        f"date={day.isoformat()}/hour={hour}/**/*.parquet"
        for day, hour in partitions
    ]


def _parse_levels(value: object, levels: int = BOOK_LEVELS) -> list[tuple[float, float]]:
    parsed: list[tuple[float, float]] = []
    try:
        raw_levels = json.loads(value) if isinstance(value, str) else value
        for level in raw_levels[:levels]:
            parsed.append((float(level["price"]), float(level["quantity"])))
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        pass
    while len(parsed) < levels:
        parsed.append((np.nan, np.nan))
    return parsed


def _decode_books(pdf: pd.DataFrame) -> pd.DataFrame:
    rows: dict[str, list[object]] = {"timestamp": []}
    for level in range(1, BOOK_LEVELS + 1):
        rows[f"p_bid_{level}"] = []
        rows[f"p_ask_{level}"] = []
        rows[f"q_bid_{level}"] = []
        rows[f"q_ask_{level}"] = []
    for row in pdf.itertuples(index=False):
        bids = _parse_levels(row.bids)
        asks = _parse_levels(row.asks)
        rows["timestamp"].append(row.exchange_ts_ms)
        for level, ((bid_p, bid_q), (ask_p, ask_q)) in enumerate(zip(bids, asks), start=1):
            rows[f"p_bid_{level}"].append(bid_p)
            rows[f"p_ask_{level}"].append(ask_p)
            rows[f"q_bid_{level}"].append(bid_q)
            rows[f"q_ask_{level}"].append(ask_q)
    result = pd.DataFrame(rows)
    result["timestamp"] = pd.to_datetime(result["timestamp"], unit="ms", utc=True)
    return result


def _trade_events(pdf: pd.DataFrame) -> pd.DataFrame:
    quantity = pd.to_numeric(pdf["quantity"], errors="coerce")
    result = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(pdf["exchange_ts_ms"], unit="ms", utc=True),
            "p_trade": pd.to_numeric(pdf["price"], errors="coerce"),
            "v_trade": quantity,
            "v_buy": np.where(pdf["taker_side"].eq("buy"), quantity, 0.0),
            "v_sell": np.where(pdf["taker_side"].eq("sell"), quantity, 0.0),
        }
    )
    return result


def _set_time_index(frame: dd.DataFrame) -> dd.DataFrame:
    indexed = frame.set_index("timestamp", shuffle_method="tasks")
    return indexed.compute_current_divisions(set_divisions=True)


def _load_venue_events(
    fs: gcsfs.GCSFileSystem,
    bucket: str,
    venue: str,
    start: date,
    end: date,
    hour: str | None,
    source_partitions: Iterable[tuple[date, str]] | None = None,
) -> tuple[dd.DataFrame, dd.DataFrame]:
    partitioning = hive_partitioning()
    explicit_partitions = tuple(source_partitions) if source_partitions is not None else None
    tick_paths = (
        source_partition_paths(bucket, "ticks", venue, explicit_partitions)
        if explicit_partitions is not None
        else source_paths(bucket, "ticks", venue, start, end, hour)
    )
    book_paths = (
        source_partition_paths(bucket, "books", venue, explicit_partitions)
        if explicit_partitions is not None
        else source_paths(bucket, "books", venue, start, end, hour)
    )
    ticks = dd.read_parquet(
        tick_paths,
        engine="pyarrow",
        filesystem=fs,
        dataset={"partitioning": partitioning},
        columns=["price", "quantity", "taker_side", "exchange_ts_ms"],
        read={"open_file_options": {"cache_type": "none"}, "pre_buffer": False},
    )
    books = dd.read_parquet(
        book_paths,
        engine="pyarrow",
        filesystem=fs,
        dataset={"partitioning": partitioning},
        columns=["bids", "asks", "exchange_ts_ms"],
        read={"open_file_options": {"cache_type": "none"}, "pre_buffer": False},
    )

    tick_events = ticks.map_partitions(_trade_events, meta={
        "timestamp": "datetime64[ns, UTC]", "p_trade": "f8", "v_trade": "f8",
        "v_buy": "f8", "v_sell": "f8",
    })
    book_meta = {"timestamp": "datetime64[ns, UTC]"}
    for level in range(1, BOOK_LEVELS + 1):
        book_meta[f"p_bid_{level}"] = "f8"
        book_meta[f"p_ask_{level}"] = "f8"
        book_meta[f"q_bid_{level}"] = "f8"
        book_meta[f"q_ask_{level}"] = "f8"
    book_events = books.map_partitions(_decode_books, meta=book_meta)

    return _set_time_index(tick_events), _set_time_index(book_events)


def _resample_events(
    tick_indexed: dd.DataFrame,
    book_indexed: dd.DataFrame,
    venue: str,
    freq: str,
    start: pd.Timestamp | None = None,
    end: pd.Timestamp | None = None,
) -> dd.DataFrame:
    # Dask's resample uses an overlap window. Raw exporter files can create
    # adjacent divisions narrower than that window (notably Kraken), which
    # makes even a 1-second resample fail before the output is materialized.
    # The runner loads at most two source hours, so hourly divisions preserve
    # bounded memory while guaranteeing a safe partition width.
    if tick_indexed.npartitions > 1:
        tick_indexed = tick_indexed.repartition(freq="1h")
    if book_indexed.npartitions > 1:
        book_indexed = book_indexed.repartition(freq="1h")

    price_series = tick_indexed["p_trade"]
    positive_trades = tick_indexed[tick_indexed["v_trade"].gt(0)]
    dt_fill_ms = positive_trades.index.to_series().diff().dt.total_seconds() * 1000.0
    tick_bars = dd.concat(
        [
            price_series.resample(freq).first().rename("p_open"),
            price_series.resample(freq).last().rename("p_trade"),
            price_series.resample(freq).last().rename("p_close"),
            price_series.resample(freq).mean().rename("p_trade_mean"),
            price_series.resample(freq).max().rename("p_high"),
            price_series.resample(freq).min().rename("p_low"),
            dt_fill_ms.resample(freq).mean().rename("dt_fill_mean_ms"),
            dt_fill_ms.resample(freq).max().rename("dt_fill_max_ms"),
            dt_fill_ms.resample(freq).min().rename("dt_fill_min_ms"),
        ],
        axis=1,
    )
    tick_bars["v_trade"] = tick_indexed["v_trade"].resample(freq).sum()
    tick_bars["v_buy"] = tick_indexed["v_buy"].resample(freq).sum()
    tick_bars["v_sell"] = tick_indexed["v_sell"].resample(freq).sum()
    tick_bars["cnt_trade"] = tick_indexed["v_trade"].gt(0).resample(freq).sum()

    book_price_columns = [f"p_{side}_{level}" for level in range(1, BOOK_LEVELS + 1) for side in ("bid", "ask")]
    book_quote_columns = [f"q_{side}_{level}" for level in range(1, BOOK_LEVELS + 1) for side in ("bid", "ask")]
    book_bars = dd.concat(
        [
            book_indexed[book_price_columns].resample(freq).last(),
            # Quantities describe the book state, so retain the latest
            # snapshot. Summing them across updates makes depth-based
            # features (WAP, OBI, slope, HHI) physically meaningless.
            book_indexed[book_quote_columns].resample(freq).last(),
        ],
        axis=1,
    )
    result = tick_bars.join(book_bars, how="outer")
    # Dask cannot forward-fill through a partition that is entirely null for a
    # column. The resampled frame is small, and a single partition also carries
    # quote state correctly across source-hour boundaries.
    result = result.repartition(npartitions=1).compute()
    if not result.empty:
        grid_start = (start if start is not None else result.index.min()).floor(freq)
        grid_end = (
            end
            if end is not None
            else result.index.max() + pd.tseries.frequencies.to_offset(freq)
        ).ceil(freq)
        full_index = pd.date_range(grid_start, grid_end, freq=freq, inclusive="left")
        full_index.name = result.index.name or "timestamp"
        result = result.reindex(full_index)
    state_columns = ["p_open", "p_trade", "p_close", "p_high", "p_low", *book_price_columns, *book_quote_columns]
    flow_columns = ["v_trade", "v_buy", "v_sell", "cnt_trade"]
    result[state_columns] = result[state_columns].ffill()
    result[flow_columns] = result[flow_columns].fillna(0.0)
    result = result.reset_index()
    result["venue"] = venue
    return dd.from_pandas(result, npartitions=1)


def _resample_venue(
    fs: gcsfs.GCSFileSystem,
    bucket: str,
    venue: str,
    start: date,
    end: date,
    freq: str,
    hour: str | None,
    source_partitions: Iterable[tuple[date, str]] | None = None,
) -> dd.DataFrame:
    tick_indexed, book_indexed = _load_venue_events(
        fs,
        bucket,
        venue,
        start,
        end,
        hour,
        source_partitions,
    )
    return _resample_events(tick_indexed, book_indexed, venue, freq)


def build_dataset(fs: gcsfs.GCSFileSystem, args: argparse.Namespace) -> dd.DataFrame:
    venues = list(VENUES) if args.venue == "all" else [args.venue]
    venue_frames = [_resample_venue(fs, args.bucket, venue, args.start_date, args.end_date, args.frequency, args.hour) for venue in venues]
    resampled = dd.concat(venue_frames, axis=0)
    resampled["date"] = resampled["timestamp"].dt.strftime("%Y-%m-%d")
    resampled["hour"] = resampled["timestamp"].dt.strftime("%H")
    return resampled


def write_dataset(result: dd.DataFrame, output: str, overwrite: bool) -> None:
    """Write Hive partitions without duplicating their keys in file payloads."""
    result.to_parquet(
        output,
        engine="pyarrow",
        compression="snappy",
        write_index=False,
        partition_on=["date", "hour", "venue"],
        overwrite=overwrite,
    )


def hour_partition_path(output: str, day: date, hour: str, venue: str) -> str:
    """Return the final path for one idempotently replaceable hour."""
    return f"{output.rstrip('/')}/date={day.isoformat()}/hour={hour}/venue={venue}"


def write_hour_partition(
    result: dd.DataFrame,
    output: str,
    day: date,
    hour: str,
    venue: str,
    overwrite: bool = True,
) -> str:
    """Replace one Hive partition without deleting sibling hours or venues."""
    partition_columns = {"date", "hour", "venue"}
    payload_columns = [column for column in result.columns if column not in partition_columns]
    destination = hour_partition_path(output, day, hour, venue)
    result[payload_columns].repartition(npartitions=1).to_parquet(
        destination,
        engine="pyarrow",
        compression="snappy",
        write_index=False,
        overwrite=overwrite,
    )
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--venue", choices=(*VENUES, "all"), default="all")
    parser.add_argument("--hour", help="optional UTC hour (00-23) for bounded validation/backfills")
    parser.add_argument("--frequency", choices=FREQUENCIES, default="1s")
    parser.add_argument(
        "--output",
        help=(
            "Local path or gs:// output root. Defaults to "
            f"gs://<bucket>/{DEFAULT_OUTPUT_DATASET}/frequency=<frequency>."
        ),
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    frequency_label = args.frequency
    args.frequency = FREQUENCIES[args.frequency]
    output = args.output or f"gs://{args.bucket}/{DEFAULT_OUTPUT_DATASET}/frequency={frequency_label}"
    if args.end_date < args.start_date:
        parser.error("--end-date must be on or after --start-date")
    if args.hour is not None and (len(args.hour) != 2 or not args.hour.isdigit() or not 0 <= int(args.hour) <= 23):
        parser.error("--hour must be a UTC hour formatted 00-23")
    if not args.overwrite and (output.startswith("/") and Path(output).exists()):
        parser.error("output exists; pass --overwrite to replace it")

    fs = gcsfs.GCSFileSystem()
    result = build_dataset(fs, args)
    write_dataset(result, output, args.overwrite)
    print(f"Wrote resampled market data to {output}")


if __name__ == "__main__":
    main()
