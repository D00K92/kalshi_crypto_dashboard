"""Generated-schema Feast declaration for the canonical feature contract."""

from datetime import timedelta

from feast import FeatureService, FeatureView, Field, PushSource
from feast.types import Float64, Int64

from definitions.data_sources import build_canonical_10s_market_feature_source
from definitions.entities import asset
from registry.generated_feature_contracts import (
    CONTRACT_DEFINITIONS,
    CURRENT_CONTRACT_VERSION,
    FIELD_TYPES,
)

contract = CONTRACT_DEFINITIONS[CURRENT_CONTRACT_VERSION]
field_names = tuple(contract["fields"])
type_map = {"float64": Float64, "int64": Int64}

batch_source = build_canonical_10s_market_feature_source()
source = PushSource(name=f'{contract["feature_view"]}_push', batch_source=batch_source)

canonical_market_features = FeatureView(
    name=contract["feature_view"],
    entities=[asset],
    ttl=timedelta(days=30),
    schema=[Field(name=name, dtype=type_map[FIELD_TYPES[name]]) for name in field_names],
    source=source,
    online=True,
)

canonical_volatility_service = FeatureService(
    name=contract["feature_service"],
    features=[canonical_market_features],
    tags={
        "feature_version": CURRENT_CONTRACT_VERSION,
        "architecture": contract["default_architecture"],
    },
)
