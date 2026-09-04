"""Compute hourly realized-volatility features in BigQuery."""
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
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    target = (
        datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc)
        if args.target_hour
        else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    )
    if target.minute or target.second or target.microsecond:
        parser.error("target hour must be aligned to UTC hour")
    sql = (Path(__file__).resolve().parents[1] / "sql" / "011_compute_realized_volatility.sql").read_text()
    sql = sql.replace("${project}", args.project)
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("target_start", "TIMESTAMP", target),
        bigquery.ScalarQueryParameter("target_end", "TIMESTAMP", target + timedelta(hours=1)),
    ], dry_run=args.dry_run, use_query_cache=False)
    job = bigquery.Client(project=args.project, location=args.location).query(sql, job_config=job_config)
    if not args.dry_run:
        job.result()
    print(f"{'validated' if args.dry_run else 'wrote'} realized-volatility features for {target.isoformat()}", flush=True)


if __name__ == "__main__":
    main()
