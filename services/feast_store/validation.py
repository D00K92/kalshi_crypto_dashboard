"""Feature-store contract and freshness validation hooks."""


def validate_feature_schema(frame, *, version: str) -> None:
    """Validate required columns, dtypes, and version metadata."""
    raise NotImplementedError


def validate_feature_freshness(frame, *, as_of) -> None:
    """Validate event-time coverage and the absence of future leakage."""
    raise NotImplementedError
