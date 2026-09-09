"""Immutable model input contracts used by loading, training, and artifacts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelFeatureContract:
    feature_set: str
    feature_version: str
    feature_view: str
    feature_service: str
    offline_table: str
    label_version: str
    feature_columns: tuple[str, ...]


CONTRACTS = {
    "v1": ModelFeatureContract(
        feature_set="market_features",
        feature_version="v1",
        feature_view="v1_market_features",
        feature_service="volatility_v1",
        offline_table="kalshi-crypto-506614.feature_store.realized_volatility_v1",
        # The legacy v1 feature table was paired with the existing v2_10s
        # labels; keep that rollback loader behavior intact.
        label_version="v2_10s",
        feature_columns=("log_return", "venue_count"),
    ),
    "v2_10s": ModelFeatureContract(
        feature_set="market_features",
        feature_version="v2_10s",
        feature_view="v2_10s_market_features",
        feature_service="volatility_v2_10s",
        offline_table="kalshi-crypto-506614.feature_store.realized_volatility_v2_10s",
        label_version="v2_10s",
        feature_columns=("log_return", "venue_count"),
    ),
}

CURRENT_CONTRACT_VERSION = "v2_10s"


def resolve_contract(version: str = CURRENT_CONTRACT_VERSION) -> ModelFeatureContract:
    try:
        return CONTRACTS[version]
    except KeyError as exc:
        raise ValueError(f"unsupported model feature contract: {version}") from exc
