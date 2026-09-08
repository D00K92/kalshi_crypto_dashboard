"""Feast v2_10s market features backed by the corrected 10-second source."""

from datetime import timedelta

from feast import FeatureService, FeatureView, Field, PushSource
from feast.types import Float64, Int64

from definitions.data_sources import build_v2_10s_market_feature_source
from definitions.entities import asset


FEATURE_COLUMNS = ("synthetic_price", "log_return", "venue_count")

batch_source = build_v2_10s_market_feature_source()
source = PushSource(name="v2_10s_market_features_push", batch_source=batch_source)

v2_10s_market_features = FeatureView(
    name="v2_10s_market_features",
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

volatility_v2_10s = FeatureService(
    name="volatility_v2_10s",
    features=[v2_10s_market_features],
    tags={"model_version": "v2_10s", "feature_version": "v2_10s"},
)
