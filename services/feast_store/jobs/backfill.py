"""Bounded backfill of Feast's online store from its canonical offline source."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from feast import FeatureStore

from registry import resolve_feature_spec


def _utc_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def backfill_features(*, repo_path: str, start_time: datetime, end_time: datetime, feature_version: str) -> None:
    """Repopulate a bounded online interval; batch ETL remains the offline writer."""
    if start_time.tzinfo is None or end_time.tzinfo is None:
        raise ValueError("backfill timestamps must be timezone-aware")
    if end_time <= start_time:
        raise ValueError("end_time must be after start_time")
    if end_time > datetime.now(timezone.utc):
        raise ValueError("end_time must not be in the future")
    spec = resolve_feature_spec("market_features", feature_version)
    FeatureStore(repo_path=repo_path).materialize(
        start_date=start_time, end_date=end_time, feature_views=[spec.feature_view]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-path", default=".")
    parser.add_argument("--start-time", required=True)
    parser.add_argument("--end-time", required=True)
    parser.add_argument("--feature-version", default="v1")
    args = parser.parse_args()
    backfill_features(
        repo_path=args.repo_path, start_time=_utc_time(args.start_time),
        end_time=_utc_time(args.end_time), feature_version=args.feature_version,
    )
