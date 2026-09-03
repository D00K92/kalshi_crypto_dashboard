"""Materialize the latest GCS feature rows into Feast's Redis online store."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from feast import FeatureStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default="feature_store")
    parser.add_argument("--end", help="UTC ISO timestamp; defaults to now")
    args = parser.parse_args()
    end = datetime.fromisoformat(args.end.replace("Z", "+00:00")) if args.end else datetime.now(timezone.utc)
    FeatureStore(repo_path=args.repo).materialize_incremental(end_date=end)


if __name__ == "__main__":
    main()
