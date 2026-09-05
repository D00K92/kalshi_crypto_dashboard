"""Declarative live-feature contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FeatureSpec:
    feature_set: str
    version: str
    feature_view: str
    push_source: str
    fields: tuple[str, ...]
    required_fields: tuple[str, ...]

    def validate(self, values: dict[str, Any]) -> None:
        missing = [name for name in self.required_fields if name not in values or values[name] is None]
        if missing:
            raise ValueError(f"feature payload missing fields: {missing}")


FEATURE_REGISTRY: dict[tuple[str, str], FeatureSpec] = {
    ("market_features", "v1"): FeatureSpec(
        feature_set="market_features",
        version="v1",
        feature_view="v1_market_features",
        push_source="v1_market_features_push",
        fields=("synthetic_price", "log_return", "venue_count"),
        required_fields=("synthetic_price", "venue_count"),
    ),
}


def resolve_feature_spec(feature_set: str, version: str) -> FeatureSpec:
    try:
        return FEATURE_REGISTRY[(feature_set, version)]
    except KeyError as exc:
        raise ValueError(f"unsupported feature contract: {feature_set}/{version}") from exc
