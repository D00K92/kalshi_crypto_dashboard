"""Offline data-source declarations for Feast."""
from feast import BigQuerySource

PROJECT = "kalshi-crypto-506614"
REALIZED_VOLATILITY_TABLE = f"{PROJECT}.feature_store.realized_volatility_v1"


def build_market_feature_source():
    """Return the BigQuery source populated by batch_etl."""
    return BigQuerySource(
        name="v1_realized_volatility_source",
        table=REALIZED_VOLATILITY_TABLE,
        timestamp_field="event_timestamp",
        created_timestamp_column="created_timestamp",
    )


def build_entity_source():
    """Return the event-time source used for point-in-time retrieval."""
    return build_market_feature_source()
