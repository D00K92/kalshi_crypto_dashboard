"""Land one UTC hour and resample all configured frequencies in BigQuery."""
from __future__ import annotations

import argparse
import subprocess
import sys
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery

FREQUENCIES = {"1s": (1, "SECOND"), "5s": (5, "SECOND"), "1m": (1, "MINUTE"), "5m": (5, "MINUTE"), "10m": (10, "MINUTE"), "30m": (30, "MINUTE"), "1h": (1, "HOUR")}
VENUES = ("binance", "bitstamp", "coinbase", "crypto.com", "gemini", "kraken")


def render_sql(frequency: str) -> str:
    """Render the parameterized SQL template for one cadence."""
    count, unit = FREQUENCIES[frequency]
    sql = (Path(__file__).resolve().parents[1] / "sql" / "010_resample_bars_1m.sql").read_text()
    sql = sql.replace("INTERVAL 1 MINUTE", f"INTERVAL {count} {unit}")
    bucket_expr = f"TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(event_timestamp), {count * (1 if unit == 'SECOND' else 60 if unit == 'MINUTE' else 3600)}) * {count * (1 if unit == 'SECOND' else 60 if unit == 'MINUTE' else 3600)})"
    sql = sql.replace("TIMESTAMP_TRUNC(event_timestamp, MINUTE)", bucket_expr)
    sql = sql.replace("'1m' frequency", f"'{frequency}' frequency")
    return sql


def process_hour(target: datetime, *, venues: tuple[str, ...], frequencies: tuple[str, ...], bucket: str, project: str) -> None:
    """Land raw data, then execute SQL resampling for all venue/frequency pairs."""
    loader = Path(__file__).with_name("load_raw_to_bigquery.py")
    for venue in venues:
        instrument = "BTCUSDT" if venue == "binance" else "BTCUSD"
        subprocess.run([sys.executable, str(loader), "--date", target.strftime("%Y-%m-%d"), "--hour", target.strftime("%H"), "--venue", venue, "--instrument", instrument, "--bucket", bucket], check=True)
        client = bigquery.Client(project=project, location="asia-northeast3")
        for frequency in frequencies:
            config = bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("source_start", "TIMESTAMP", target - timedelta(hours=1)),
                bigquery.ScalarQueryParameter("target_start", "TIMESTAMP", target),
                bigquery.ScalarQueryParameter("target_end", "TIMESTAMP", target + timedelta(hours=1)),
                bigquery.ScalarQueryParameter("venue", "STRING", venue),
                bigquery.ScalarQueryParameter("instrument", "STRING", instrument),
            ])
            client.query(render_sql(frequency), job_config=config).result()
            print(f"resampled venue={venue} frequency={frequency} target={target.isoformat()}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-hour", default=os.getenv("BATCH_ETL_TARGET_HOUR"))
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET_NAME", "kalshi-crypto-tick-data"))
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--venues", default=os.getenv("BATCH_ETL_VENUES", ",".join(VENUES)))
    parser.add_argument("--frequencies", default=os.getenv("BATCH_ETL_FREQUENCIES", ",".join(FREQUENCIES)))
    args = parser.parse_args()
    target = (datetime.fromisoformat(args.target_hour.replace("Z", "+00:00")).astimezone(timezone.utc)
              if args.target_hour else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1))
    if target.minute or target.second or target.microsecond:
        parser.error("target hour must be aligned to an hour")
    process_hour(target, venues=tuple(args.venues.split(",")), frequencies=tuple(args.frequencies.split(",")), bucket=args.bucket, project=args.project)


if __name__ == "__main__":
    main()
