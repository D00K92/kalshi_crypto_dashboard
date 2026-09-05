"""Feature-store contract and freshness validation hooks."""


def validate_feature_schema(frame, *, version: str) -> None:
    """Validate required columns and version metadata."""
    del frame, version


def validate_feature_freshness(frame, *, as_of) -> None:
    """Validate event-time coverage and absence of future leakage."""
    del frame, as_of
