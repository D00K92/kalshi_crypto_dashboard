"""Bulk-load GCS Parquet into BigQuery landing tables.

This replaces the per-file pandas upload bottleneck. It intentionally lands
source-shaped Parquet first; normalization into ``market_data.raw_*`` remains
a separate set-based SQL step.
"""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from datetime import date, timedelta

from google.cloud import bigquery, storage

VENUES = ("binance", "bitstamp", "coinbase", "crypto.com", "gemini", "kraken")
KINDS = ("ticks", "books")
INSTRUMENTS = {"binance": "BTCUSDT", "coinbase": "BTC-USD", "kraken": "BTC_USD"}


def _days(start: date, end: date):
    while start <= end:
        yield start
        start += timedelta(days=1)


def discover_parquet_uris(bucket: str, *, start: date, end: date, venues=VENUES, kinds=KINDS, project: str | None = None):
    """Return source URIs grouped by source kind and venue."""
    client = storage.Client(project=project)
    grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
    allowed = {(kind, venue) for kind in kinds for venue in venues}
    pattern = re.compile(r"^(ticks|books)/venue=([^/]+)/instrument=([^/]+)/date=(\d{4}-\d{2}-\d{2})/")
    for kind in kinds:
        for venue in venues:
            if (kind, venue) not in allowed:
                continue
            instrument = INSTRUMENTS.get(venue, "BTCUSD")
            for day in _days(start, end):
                prefix = f"{kind}/venue={venue}/instrument={instrument}/date={day.isoformat()}/"
                for blob in client.list_blobs(bucket, prefix=prefix):
                    if blob.name.endswith(".parquet"):
                        match = pattern.match(blob.name)
                        if match:
                            grouped[(kind, venue)].append(f"gs://{bucket}/{blob.name}")
    return {key: sorted(values) for key, values in grouped.items()}


def staging_table(project: str, dataset: str, kind: str, venue: str) -> str:
    safe_venue = venue.replace(".", "_").replace("-", "_")
    return f"{project}.{dataset}.{kind}_{safe_venue}"


def load_group(client: bigquery.Client, uris: list[str], destination: str, *, execute: bool) -> int:
    """Bulk-load one venue/kind using a single BigQuery load job."""
    if not uris:
        return 0
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                    autodetect=True,
                                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    if execute:
        client.load_table_from_uri(uris, destination, job_config=config).result()
    return len(uris)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--bucket", default="kalshi-crypto-tick-data")
    parser.add_argument("--project", default="kalshi-crypto-506614")
    parser.add_argument("--dataset", default="market_data_landing")
    parser.add_argument("--venues", default=",".join(VENUES))
    parser.add_argument("--types", default=",".join(KINDS))
    parser.add_argument("--execute", action="store_true", help="submit load jobs; default is discovery only")
    args = parser.parse_args()
    if args.end_date < args.start_date:
        parser.error("end-date must not precede start-date")
    venues = tuple(v.strip() for v in args.venues.split(",") if v.strip())
    kinds = tuple(k.strip() for k in args.types.split(",") if k.strip())
    grouped = discover_parquet_uris(args.bucket, start=args.start_date, end=args.end_date,
                                    venues=venues, kinds=kinds, project=args.project)
    client = bigquery.Client(project=args.project, location="asia-northeast3")
    for (kind, venue), uris in sorted(grouped.items()):
        table = staging_table(args.project, args.dataset, kind, venue)
        count = load_group(client, uris, table, execute=args.execute)
        print(f"{'loaded' if args.execute else 'would_load'} kind={kind} venue={venue} files={count} table={table}", flush=True)


if __name__ == "__main__":
    main()
