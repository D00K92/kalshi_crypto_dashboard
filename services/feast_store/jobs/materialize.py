"""Materialize the latest completed feature interval into the online store."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from feast import FeatureStore


def _completed_utc_hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("end_time must include a UTC offset")
    parsed = parsed.astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise ValueError("end_time must be aligned to a completed UTC hour")
    if parsed > datetime.now(timezone.utc):
        raise ValueError("end_time must not be in the future")
    return parsed


def materialize_incremental(*, repo_path: str, end_time: datetime) -> None:
    """Materialize all registered views through a bounded completed UTC hour."""
    if end_time.tzinfo is None:
        raise ValueError("end_time must be timezone-aware")
    FeatureStore(repo_path=repo_path).materialize_incremental(end_date=end_time)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-path", default=".")
    parser.add_argument("--end-time", required=True, help="Completed UTC hour, e.g. 2026-09-07T04:00:00Z")
    args = parser.parse_args()
    materialize_incremental(repo_path=args.repo_path, end_time=_completed_utc_hour(args.end_time))
