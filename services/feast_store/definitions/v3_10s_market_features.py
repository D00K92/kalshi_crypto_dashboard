"""Feast contract for point-in-time-safe HAR volatility features."""

from datetime import timedelta

from feast import FeatureService, FeatureView, Field, PushSource
from feast.types import Float64, Int64

from definitions.data_sources import build_v3_10s_market_feature_source
from definitions.entities import asset


FEATURE_COLUMNS = (
    "synthetic_price",
    "log_return",
    "venue_count",
    "realized_vol_30s",
    "realized_vol_1m",
    "realized_vol_5m",
    "realized_vol_15m",
    "realized_vol_30m",
    "realized_vol_1h",
    "realized_vol_3h",
)

batch_source = build_v3_10s_market_feature_source()
source = PushSource(name="v3_10s_market_features_push", batch_source=batch_source)

v3_10s_market_features = FeatureView(
    name="v3_10s_market_features",
    entities=[asset],
    ttl=timedelta(days=30),
    schema=[
        Field(name="synthetic_price", dtype=Float64),
        Field(name="log_return", dtype=Float64),
        Field(name="venue_count", dtype=Int64),
        Field(name="realized_vol_30s", dtype=Float64),
        Field(name="realized_vol_1m", dtype=Float64),
        Field(name="realized_vol_5m", dtype=Float64),
        Field(name="realized_vol_15m", dtype=Float64),
        Field(name="realized_vol_30m", dtype=Float64),
        Field(name="realized_vol_1h", dtype=Float64),
        Field(name="realized_vol_3h", dtype=Float64),
    ],
    source=source,
    online=True,
)

volatility_v3_10s = FeatureService(
    name="volatility_v3_10s",
    features=[v3_10s_market_features],
    tags={"feature_version": "v3_10s", "architecture": "har"},
)
