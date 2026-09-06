import pandas as pd
import pytest

from src.common.modeling import LIVE_FEATURE_COLUMNS, train_horizon


def test_training_uses_only_live_serving_features() -> None:
    rows = 40
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC"),
        "log_return": [index / 10_000 for index in range(rows)],
        "venue_count": [3 for _ in range(rows)],
        "realized_vol_1h": [99.0 for _ in range(rows)],
        "target_rv_1m": [0.1 + index / 1_000 for index in range(rows)],
    })

    _, metadata = train_horizon(table, "1m")

    assert metadata["feature_columns"] == list(LIVE_FEATURE_COLUMNS)


def test_training_rejects_missing_live_feature() -> None:
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=10, freq="min", tz="UTC"),
        "log_return": [0.01 for _ in range(10)],
        "target_rv_1m": [0.1 for _ in range(10)],
    })

    with pytest.raises(ValueError, match="venue_count"):
        train_horizon(table, "1m")
