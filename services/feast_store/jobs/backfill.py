"""Bounded historical materialization and offline-contract backfill hooks."""


def backfill_features(*, start_time, end_time, feature_version: str) -> None:
    """Backfill the offline feature source for a bounded UTC range."""
    raise NotImplementedError


def main() -> None:
    """CLI entrypoint for a resumable Feast backfill."""
    raise NotImplementedError
