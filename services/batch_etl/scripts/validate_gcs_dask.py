#!/usr/bin/env python3
"""Validate Hive-partitioned Parquet data in GCS with Dask.

Authentication uses Google Application Default Credentials. For local use,
configure them first with ``gcloud auth application-default login`` or set
``GOOGLE_APPLICATION_CREDENTIALS`` to a service-account JSON file.

Example:
    uv run --with 'dask[dataframe]' --with gcsfs --with pyarrow \
        python scripts/validate_gcs_dask.py --date 2026-09-01
"""

from __future__ import annotations

import argparse
from datetime import date
import os

os.environ.setdefault("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")
os.environ.setdefault("USE_EXPERIMENTAL_ADAPTIVE_PREFETCHING", "false")

import dask
import dask.dataframe as dd
import gcsfs
import pyarrow as pa
import pyarrow.dataset as ds


DEFAULT_BUCKET = "kalshi-crypto-tick-data"
VENUES = {
    "binance": "BTCUSDT",
    "bitstamp": "BTCUSD",
    "coinbase": "BTC-USD",
    "crypto.com": "BTCUSD",
    "gemini": "BTCUSD",
    "kraken": "BTC_USD",
}
DATASETS = ("ticks", "books")


def hive_partitioning() -> ds.Partitioning:
    """Describe the partition columns stored in the GCS directory path."""
    schema = pa.schema(
        [
            ("venue", pa.string()),
            ("instrument", pa.string()),
            ("date", pa.string()),
            ("hour", pa.string()),
        ]
    )
    return ds.partitioning(schema=schema, flavor="hive")


def validate_dataset(
    fs: gcsfs.GCSFileSystem,
    *,
    bucket: str,
    dataset: str,
    target_date: date,
    venues: list[str],
    sample_rows: int,
    count_rows: bool,
) -> None:
    partitioning = hive_partitioning()

    for venue in venues:
        instrument = VENUES[venue]
        path = (
            f"gs://{bucket}/{dataset}/venue={venue}/instrument={instrument}/"
            f"date={target_date.isoformat()}/**/*.parquet"
        )
        ddf = dd.read_parquet(
            path,
            engine="pyarrow",
            filesystem=fs,
            dataset={"partitioning": partitioning},
            read={"open_file_options": {"cache_type": "none"}, "pre_buffer": False},
        )

        # head(compute=True) verifies that Dask can open and decode an object,
        # rather than only constructing a lazy expression graph.
        with dask.config.set(scheduler="synchronous"):
            sample = ddf.head(sample_rows, compute=True)
        print(
            f"{dataset:5} venue={venue:10} partitions={ddf.npartitions:4} "
            f"sample_rows={len(sample):2} columns={list(ddf.columns)}"
        )
        if count_rows:
            rows = int(ddf.shape[0].compute())
            print(f"{dataset:5} venue={venue:10} rows={rows}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--dataset", choices=(*DATASETS, "all"), default="all")
    parser.add_argument("--venue", choices=(*VENUES, "all"), default="all")
    parser.add_argument("--sample-rows", type=int, default=1)
    parser.add_argument(
        "--count-rows",
        action="store_true",
        help="force a full row-count computation; this can be expensive",
    )
    args = parser.parse_args()
    if args.sample_rows <= 0:
        parser.error("--sample-rows must be positive")

    datasets = DATASETS if args.dataset == "all" else (args.dataset,)
    venues = list(VENUES) if args.venue == "all" else [args.venue]
    fs = gcsfs.GCSFileSystem()
    try:
        for dataset in datasets:
            validate_dataset(
                fs,
                bucket=args.bucket,
                dataset=dataset,
                target_date=args.date,
                venues=venues,
                sample_rows=args.sample_rows,
                count_rows=args.count_rows,
            )
    finally:
        fs.invalidate_cache()


if __name__ == "__main__":
    main()
