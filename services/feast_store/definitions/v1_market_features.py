"""Feast v1 market feature view backed by BigQuery."""
from datetime import timedelta
from feast import FeatureService, FeatureView, Field, PushSource
from feast.types import Float64, Int64
from definitions.entities import asset
from definitions.data_sources import build_market_feature_source

FEATURE_COLUMNS = ("synthetic_price", "log_return", "venue_count")

batch_source = build_market_feature_source()
source = PushSource(name="v1_market_features_push", batch_source=batch_source)

v1_market_features = FeatureView(
    name="v1_market_features",
    entities=[asset],
    ttl=timedelta(days=30),
    schema=[
        Field(name="synthetic_price", dtype=Float64),
        Field(name="log_return", dtype=Float64),
        Field(name="venue_count", dtype=Int64),
    ],
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
