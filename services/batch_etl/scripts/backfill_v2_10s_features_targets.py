"""Backfill v2_10s features and labels with bounded concurrent BigQuery jobs."""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery


SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def run_day(target_date: date, *, project: str, location: str, sql: str, dry_run: bool, max_bytes: int | None):
    target = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_start", "TIMESTAMP", target),
            bigquery.ScalarQueryParameter("target_end", "TIMESTAMP", target + timedelta(days=1)),
        ],
        dry_run=dry_run,
        use_query_cache=False,
    )
    if max_bytes is not None:
        config.maximum_bytes_billed = max_bytes
    job = bigquery.Client(project=project, location=location).query(sql, job_config=config)
    bytes_processed = int(job.total_bytes_processed or 0)
    if not dry_run:
        job.result()
    return target_date, bytes_processed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, type=parse_date, help="inclusive UTC date")
    parser.add_argument("--end-date", required=True, type=parse_date, help="exclusive UTC date")
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--maximum-bytes-billed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.end_date <= args.start_date:
        parser.error("--end-date must be after --start-date")
    if not 1 <= args.parallelism <= 4:
        parser.error("--parallelism must be between 1 and 4")
    feature_sql = (SQL_DIR / "014_compute_v2_10s_features.sql").read_text().replace("${project}", args.project)
    target_sql = (SQL_DIR / "015_compute_v2_10s_targets.sql").read_text().replace("${project}", args.project)
    # Each SQL file declares its own annualization constant. Wrap the second
    # file in a block so its DECLARE remains legal in one BigQuery script.
    sql = feature_sql + "\nBEGIN\n" + target_sql + "\nEND;"
    dates = [args.start_date + timedelta(days=i) for i in range((args.end_date - args.start_date).days)]
    verb = "validated" if args.dry_run else "backfilled"
    total_bytes = 0
    with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = {
            pool.submit(
                run_day,
                target_date,
                project=args.project,
                location=args.location,
                sql=sql,
                dry_run=args.dry_run,
                max_bytes=args.maximum_bytes_billed,
            ): target_date
            for target_date in dates
        }
        for future in as_completed(futures):
            target_date, bytes_processed = future.result()
            total_bytes += bytes_processed
            print(f"{verb} {target_date.isoformat()} bytes={bytes_processed:,}", flush=True)
    print(
        f"{verb} {len(dates)} UTC days; bytes={total_bytes:,}; parallelism={args.parallelism}",
        flush=True,
    )


if __name__ == "__main__":
    main()
