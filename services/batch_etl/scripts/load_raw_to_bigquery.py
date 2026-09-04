"""Load one GCS raw-data hour into normalized BigQuery landing tables.

This is the first step of the SQL-resampling migration. It is deliberately
bounded and partition-scoped so retries cannot affect sibling hours.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import PurePosixPath

import gcsfs
import pandas as pd
from google.cloud import bigquery


def _paths(fs, bucket: str, dataset: str, venue: str, instrument: str, day: str, hour: str) -> list[str]:
    pattern = f"{bucket}/{dataset}/venue={venue}/instrument={instrument}/date={day}/hour={hour}/*.parquet"
    return [p if p.startswith("gs://") else f"gs://{p}" for p in fs.glob(pattern)]


def _read(paths: list[str], fs) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    frames = [pd.read_parquet(path, filesystem=fs) for path in paths]
    return pd.concat(frames, ignore_index=True)


def normalize_trades(frame: pd.DataFrame, *, venue: str, instrument: str, source_object: str) -> pd.DataFrame:
    """Normalize exporter trade rows into the raw_trades BigQuery schema."""
    required = {"price", "quantity", "taker_side", "exchange_ts_ms"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"trade input missing columns: {sorted(missing)}")
    result = pd.DataFrame({
        "event_timestamp": pd.to_datetime(frame["exchange_ts_ms"], unit="ms", utc=True),
        "received_timestamp": pd.to_datetime(frame.get("received_ts_ms", frame["exchange_ts_ms"]), unit="ms", utc=True),
        "venue": venue,
        "instrument": instrument,
        "trade_id": frame.get("event_id", frame.get("redis_id")),
        "price": pd.to_numeric(frame["price"], errors="raise"),
        "quantity": pd.to_numeric(frame["quantity"], errors="raise"),
        "taker_side": frame["taker_side"].astype(str),
        "source_object": source_object,
        "ingested_at": pd.Timestamp.now(tz="UTC"),
    })
    return result


def _levels(value) -> list[tuple[float, float]]:
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, list):
        raise ValueError("book levels must be a JSON array")
    return [(float(row["price"]), float(row["quantity"])) for row in parsed[:10]]


def normalize_books(frame: pd.DataFrame, *, venue: str, instrument: str, source_object: str) -> pd.DataFrame:
    """Expand JSON bid/ask arrays into one normalized row per book level."""
    required = {"bids", "asks", "exchange_ts_ms"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"book input missing columns: {sorted(missing)}")
    rows: list[dict] = []
    now = pd.Timestamp.now(tz="UTC")
    for record in frame.to_dict("records"):
        event_ts = pd.to_datetime(record["exchange_ts_ms"], unit="ms", utc=True)
        received_ts = pd.to_datetime(record.get("received_ts_ms", record["exchange_ts_ms"]), unit="ms", utc=True)
        for side in ("bids", "asks"):
            for level, (price, quantity) in enumerate(_levels(record[side]), start=1):
                rows.append({
                    "event_timestamp": event_ts,
                    "received_timestamp": received_ts,
                    "venue": venue,
                    "instrument": instrument,
                    "side": side[:-1],
                    "level": level,
                    "price": price,
                    "quantity": quantity,
                    "source_object": source_object,
                    "ingested_at": now,
                })
    return pd.DataFrame(rows)


def replace_partition(client: bigquery.Client, frame: pd.DataFrame, *, table: str, day: str, venue: str, instrument: str) -> int:
    """Replace one venue/day partition, returning rows written."""
    if frame.empty:
        return 0
    client.query(
        f"DELETE FROM `{table}` WHERE DATE(event_timestamp) = @day AND venue = @venue AND instrument = @instrument",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("day", "DATE", day),
            bigquery.ScalarQueryParameter("venue", "STRING", venue),
            bigquery.ScalarQueryParameter("instrument", "STRING", instrument),
        ]),
    ).result()
    load = client.load_table_from_dataframe(frame, table, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"))
    load.result()
    return len(frame)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="UTC date, YYYY-MM-DD")
    parser.add_argument("--hour", required=True, help="UTC hour, 00-23")
    parser.add_argument("--venue", required=True)
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--types", default="trades,books")
    parser.add_argument("--project", default="kalshi-crypto-506614")
    args = parser.parse_args()
    datetime.fromisoformat(f"{args.date}T{args.hour}:00:00+00:00")
    fs = gcsfs.GCSFileSystem()
    client = bigquery.Client(project=args.project, location="asia-northeast3")
    kinds = {item.strip() for item in args.types.split(",") if item.strip()}
    if "trades" in kinds:
        paths = _paths(fs, args.bucket, "ticks", args.venue, args.instrument, args.date, args.hour)
        frame = normalize_trades(_read(paths, fs), venue=args.venue, instrument=args.instrument, source_object=str(PurePosixPath(paths[0]).parent) if paths else "")
        print(f"trades source_files={len(paths)} rows={replace_partition(client, frame, table=f'{args.project}.market_data.raw_trades', day=args.date, venue=args.venue, instrument=args.instrument)}", flush=True)
    if "books" in kinds:
        paths = _paths(fs, args.bucket, "books", args.venue, args.instrument, args.date, args.hour)
        frame = normalize_books(_read(paths, fs), venue=args.venue, instrument=args.instrument, source_object=str(PurePosixPath(paths[0]).parent) if paths else "")
        print(f"books source_files={len(paths)} rows={replace_partition(client, frame, table=f'{args.project}.market_data.raw_book_levels', day=args.date, venue=args.venue, instrument=args.instrument)}", flush=True)


if __name__ == "__main__":
    main()
