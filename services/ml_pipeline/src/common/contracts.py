"""Immutable model input contracts used by loading, training, and artifacts."""

from __future__ import annotations

from dataclasses import dataclass

HORIZON_SECONDS = {"5m": 300, "15m": 900, "30m": 1_800, "1h": 3_600}
HORIZONS = tuple(HORIZON_SECONDS)


@dataclass(frozen=True, slots=True)
class ModelFeatureContract:
    feature_set: str
    feature_version: str
    feature_view: str
    feature_service: str
    offline_table: str
    label_version: str
    feature_columns: tuple[str, ...]
    default_architecture: str = "xgboost"
    horizon_feature_columns: tuple[tuple[str, tuple[str, ...]], ...] = ()

    def columns_for(self, horizon: str) -> tuple[str, ...]:
        """Return the ordered model inputs for one forecast horizon."""
        if horizon not in HORIZONS:
            raise ValueError(f"unsupported forecast horizon: {horizon}")
        configured = dict(self.horizon_feature_columns)
        return configured.get(horizon, self.feature_columns)


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
    "v3_10s": ModelFeatureContract(
        feature_set="market_features",
        feature_version="v3_10s",
        feature_view="v3_10s_market_features",
        feature_service="volatility_v3_10s",
        offline_table="kalshi-crypto-506614.feature_store.realized_volatility_v3_10s",
        # The future windows are unchanged; v3 changes predictors only.
        label_version="v2_10s",
        feature_columns=(
            "realized_vol_30s",
            "realized_vol_1m",
            "realized_vol_5m",
            "realized_vol_15m",
            "realized_vol_30m",
            "realized_vol_1h",
            "realized_vol_3h",
        ),
        default_architecture="har",
        # HAR uses a short, horizon-scale, and longer regime component.
        horizon_feature_columns=(
            ("5m", ("realized_vol_30s", "realized_vol_1m", "realized_vol_5m")),
            ("15m", ("realized_vol_5m", "realized_vol_15m", "realized_vol_1h")),
            ("30m", ("realized_vol_5m", "realized_vol_30m", "realized_vol_1h")),
            ("1h", ("realized_vol_15m", "realized_vol_1h", "realized_vol_3h")),
        ),
    ),
    "v4_10s": ModelFeatureContract(
        feature_set="market_features",
        feature_version="v4_10s",
        feature_view="v4_10s_market_features",
        feature_service="volatility_v4_10s",
        offline_table="kalshi-crypto-506614.feature_store.realized_volatility_v4_10s",
        label_version="v2_10s",
        feature_columns=(
            "realized_vol_30s",
            "realized_vol_1m",
            "realized_vol_5m",
            "realized_vol_15m",
            "realized_vol_30m",
            "realized_vol_1h",
            "realized_vol_3h",
            "log_buy_volume_30s",
            "log_buy_volume_5m",
            "log_buy_volume_10m",
        ),
        default_architecture="har",
        horizon_feature_columns=(
            (
                "5m",
                (
                    "realized_vol_30s",
                    "realized_vol_1m",
                    "realized_vol_5m",
                    "log_buy_volume_30s",
                    "log_buy_volume_5m",
                    "log_buy_volume_10m",
                ),
            ),
            ("15m", ("realized_vol_5m", "realized_vol_15m", "realized_vol_1h")),
            ("30m", ("realized_vol_5m", "realized_vol_30m", "realized_vol_1h")),
            ("1h", ("realized_vol_15m", "realized_vol_1h", "realized_vol_3h")),
        ),
    ),
}

CURRENT_CONTRACT_VERSION = "v3_10s"


def resolve_contract(version: str = CURRENT_CONTRACT_VERSION) -> ModelFeatureContract:
    try:
        return CONTRACTS[version]
    except KeyError as exc:
        raise ValueError(f"unsupported model feature contract: {version}") from exc
