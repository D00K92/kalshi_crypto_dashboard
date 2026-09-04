"""Offline data-source declarations for Feast.

The source schema is intentionally pending until the feature contract is
finalized. This module owns source definitions; it must not compute features.
"""


def build_market_feature_source():
    """Return the Parquet FileSource for the versioned feature dataset."""
    raise NotImplementedError


def build_entity_source():
    """Return the event-time/entity source used by historical retrieval."""
    raise NotImplementedError
