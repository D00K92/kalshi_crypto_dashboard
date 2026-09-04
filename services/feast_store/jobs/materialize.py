"""Materialize the latest feature rows into the online store."""


def materialize_incremental(*, repo_path: str, end_time) -> None:
    """Materialize only the completed interval ending at ``end_time``."""
    raise NotImplementedError


def main() -> None:
    """CLI entrypoint for hourly materialization."""
    raise NotImplementedError
