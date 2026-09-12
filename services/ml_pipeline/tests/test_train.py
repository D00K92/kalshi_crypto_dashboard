import pandas as pd
import pytest
from sklearn.linear_model import LinearRegression

from src.common.contracts import HORIZONS, resolve_contract
from src.common.modeling import train_horizon


def test_training_uses_only_live_serving_features() -> None:
    rows = 40
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC"),
        "log_return": [index / 10_000 for index in range(rows)],
        "venue_count": [3 for _ in range(rows)],
        "realized_vol_1h": [99.0 for _ in range(rows)],
        "target_rv_5m": [0.1 + index / 1_000 for index in range(rows)],
    })

    _, metadata = train_horizon(table, "5m", feature_version="v2_10s")

    assert metadata["feature_columns"] == ["log_return", "venue_count"]
    assert metadata["feature_set"] == "market_features"
    assert metadata["feature_version"] == "v2_10s"
    assert metadata["feature_view"] == "v2_10s_market_features"
    assert metadata["feature_service"] == "volatility_v2_10s"
    assert metadata["label_version"] == "v2_10s"
    assert metadata["entity"] == "BTCUSD"


def test_training_rejects_missing_live_feature() -> None:
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=10, freq="min", tz="UTC"),
        "log_return": [0.01 for _ in range(10)],
        "target_rv_5m": [0.1 for _ in range(10)],
    })

    with pytest.raises(ValueError, match="venue_count"):
        train_horizon(table, "5m", feature_version="v2_10s")


def test_v1_contract_remains_available_for_rollback() -> None:
    rows = 40
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC"),
        "log_return": [index / 10_000 for index in range(rows)],
        "venue_count": [3 for _ in range(rows)],
        "target_rv_5m": [0.1 + index / 1_000 for index in range(rows)],
    })

    _, metadata = train_horizon(table, "5m", feature_version="v1")

    assert metadata["feature_version"] == "v1"
    assert metadata["feature_view"] == "v1_market_features"


def test_v3_har_uses_horizon_specific_feature_columns() -> None:
    rows = 80
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC"),
        **{
            column: [0.1 + index / (1_000 + offset) for index in range(rows)]
            for offset, column in enumerate(resolve_contract("v3_10s").feature_columns)
        },
        **{
            f"target_rv_{horizon}": [0.2 + index / 2_000 for index in range(rows)]
            for horizon in HORIZONS
        },
    })

    contract = resolve_contract("v3_10s")
    for horizon in HORIZONS:
        model, metadata = train_horizon(table, horizon, feature_version="v3_10s")
        assert isinstance(model, LinearRegression)
        assert metadata["architecture"] == "har"
        assert metadata["feature_columns"] == list(contract.columns_for(horizon))
        assert list(model.feature_names_in_) == list(contract.columns_for(horizon))
