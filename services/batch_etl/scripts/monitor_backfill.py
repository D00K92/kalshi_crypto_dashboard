"""Report BigQuery raw-backfill coverage and fail on missing UTC dates."""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from google.cloud import bigquery

VENUES = ("binance", "bitstamp", "coinbase", "crypto.com", "gemini", "kraken")
KINDS = ("ticks", "books")


def _days(start: date, end: date) -> list[str]:
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--project", default="kalshi-crypto-506614")
    parser.add_argument("--dataset", default="backfill_raw")
    args = parser.parse_args()
    if args.end_date < args.start_date:
        parser.error("end-date must not precede start-date")

    client = bigquery.Client(project=args.project, location="asia-northeast3")
    expected = set(_days(args.start_date, args.end_date))
    report = []
    incomplete = False
    for kind in KINDS:
        for venue in VENUES:
            suffix = venue.replace(".", "_").replace("-", "_")
            table = f"{args.project}.{args.dataset}.{kind}_{suffix}"
            try:
                rows = list(client.query(
                    f"SELECT DATE(TIMESTAMP_MILLIS(exchange_ts_ms)) AS day, COUNT(*) AS row_count "
                    f"FROM `{table}` WHERE exchange_ts_ms IS NOT NULL "
                    f"AND DATE(TIMESTAMP_MILLIS(exchange_ts_ms)) BETWEEN @start AND @end "
                    f"GROUP BY day ORDER BY day",
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("start", "DATE", args.start_date),
                        bigquery.ScalarQueryParameter("end", "DATE", args.end_date),
                    ]),
                ).result())
                observed = {row.day.isoformat(): int(row.row_count) for row in rows}
                observed_days = set(observed)
                if observed_days:
                    first, last = min(observed_days), max(observed_days)
                    expected_span = {d for d in expected if first <= d <= last}
                else:
                    expected_span = set()
                missing = sorted(expected_span - observed_days)
                if missing:
                    incomplete = True
                report.append({"kind": kind, "venue": venue, "table": table,
                               "days": len(observed), "missing_days": missing,
                               "rows": sum(observed.values()),
                               "min_day": min(observed, default=None),
                               "max_day": max(observed, default=None)})
            except Exception as exc:
                incomplete = True
                report.append({"kind": kind, "venue": venue, "table": table,
                               "error": str(exc)})

    print(json.dumps({"start_date": args.start_date.isoformat(),
                      "end_date": args.end_date.isoformat(),
                      "complete": not incomplete, "tables": report}, indent=2))
    return 1 if incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
