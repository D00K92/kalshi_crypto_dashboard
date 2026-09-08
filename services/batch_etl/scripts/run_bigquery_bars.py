"""Backfill canonical 10-second bars with bounded concurrent BigQuery jobs."""
from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery


SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "016_resample_bars_10s.sql"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _run_day(
    target_date: date,
    *,
    project: str,
    location: str,
    sql: str,
    dry_run: bool,
    maximum_bytes_billed: int | None,
) -> tuple[date, int, int]:
    target = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_start", "TIMESTAMP", target - timedelta(hours=1)),
            bigquery.ScalarQueryParameter("target_start", "TIMESTAMP", target),
            bigquery.ScalarQueryParameter("target_end", "TIMESTAMP", target + timedelta(days=1)),
        ],
        dry_run=dry_run,
        use_query_cache=False,
    )
    if maximum_bytes_billed is not None:
        config.maximum_bytes_billed = maximum_bytes_billed
    client = bigquery.Client(project=project, location=location)
    job = client.query(sql, job_config=config)
    bytes_processed = int(job.total_bytes_processed or 0)
    if not dry_run:
        job.result()
    return target_date, bytes_processed, int(job.num_dml_affected_rows or 0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, type=_parse_date, help="inclusive UTC date")
    parser.add_argument("--end-date", required=True, type=_parse_date, help="exclusive UTC date")
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--parallelism", type=int, default=2, help="concurrent daily BigQuery jobs")
    parser.add_argument("--maximum-bytes-billed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.end_date <= args.start_date:
        parser.error("--end-date must be after --start-date")
    if args.parallelism < 1 or args.parallelism > 4:
        parser.error("--parallelism must be between 1 and 4")
    sql = SQL_PATH.read_text().replace("${project}", args.project)
    dates = [args.start_date + timedelta(days=i) for i in range((args.end_date - args.start_date).days)]
    action = "validated" if args.dry_run else "backfilled"
    total_bytes = 0
    total_rows = 0
    with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = {
            pool.submit(
                _run_day,
                target_date,
                project=args.project,
                location=args.location,
                sql=sql,
                dry_run=args.dry_run,
                maximum_bytes_billed=args.maximum_bytes_billed,
            ): target_date
            for target_date in dates
        }
        for future in as_completed(futures):
            target_date, bytes_processed, rows = future.result()
            total_bytes += bytes_processed
            total_rows += rows
            print(
                f"{action} {target_date.isoformat()} "
                f"bytes={bytes_processed:,} rows={rows:,}",
                flush=True,
            )
    print(
        f"{action} {len(dates)} UTC days; bytes={total_bytes:,}; rows={total_rows:,}; "
        f"parallelism={args.parallelism}",
        flush=True,
    )


if __name__ == "__main__":
    main()
