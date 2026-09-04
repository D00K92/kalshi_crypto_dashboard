"""Compute future realized-volatility labels in BigQuery."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-hour", default=os.getenv("BATCH_ETL_TARGET_HOUR"))
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--location", default="asia-northeast3")
    parser.add_argument("--delay-hours", type=int, default=2)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    target = (
        datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc)
        if args.target_hour
        else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=args.delay_hours)
    )
    if target.minute or target.second or target.microsecond or args.delay_hours < 1:
        parser.error("target hour must be aligned to UTC hour and delay must be positive")
    sql = (Path(__file__).resolve().parents[1] / "sql" / "013_compute_future_realized_volatility.sql").read_text()
    sql = sql.replace("${project}", args.project)
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_start", "TIMESTAMP", target),
            bigquery.ScalarQueryParameter("target_end", "TIMESTAMP", target + timedelta(hours=1)),
        ],
        dry_run=args.dry_run,
        use_query_cache=False,
    )
    job = bigquery.Client(project=args.project, location=args.location).query(sql, job_config=config)
    if not args.dry_run:
        job.result()
    print(f"{'validated' if args.dry_run else 'wrote'} future-volatility labels for {target.isoformat()}", flush=True)


if __name__ == "__main__":
    main()
