"""Feast v1 market feature view backed by daily GCS Parquet files."""
from datetime import timedelta
from feast import FeatureService, FeatureView, Field, FileSource
from feast.types import Float32
from entities import asset, frequency

FEATURE_COLUMNS = (
    "trade_log_return", "aggressor_imbalance", "wap_1", "wap_5", "wap_10",
    "microprice_1", "microprice_5", "microprice_10", "obi_1", "obi_5", "obi_10",
    "spread", "relative_spread", "book_slope_bid", "book_slope_ask",
    "liquidity_consumption", "ofi",
    *(f"{kind}_{window}s" for kind in ("rv", "bv", "jump_component", "gk_vol")
      for window in (30, 60, 300, 900, 1800, 3600)),
)

source = FileSource(
    name="v1_market_features_source",
    path="gs://kalshi-crypto-tick-data/features/v1",
    timestamp_field="timestamp",
)

v1_market_features = FeatureView(
    name="v1_market_features",
    entities=[asset, frequency],
    ttl=timedelta(days=30),
    schema=[Field(name=name, dtype=Float32) for name in FEATURE_COLUMNS],
    source=source,
    online=True,
)

# Stable model-facing contract. Models should reference this service rather
# than individual feature names so v2 can be introduced independently.
volatility_v1 = FeatureService(
    name="volatility_v1",
    features=[v1_market_features],
    tags={"model_version": "v1", "feature_version": "v1"},
)
