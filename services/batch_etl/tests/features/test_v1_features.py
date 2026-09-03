import pandas as pd

from kalshi_crypto_batch_etl.features.v1_features import (
    build_v1_dataset,
    build_v1_dataset_by_frequency,
    compute_synthetic_targets,
    compute_v1_features,
)


def frame() -> pd.DataFrame:
    rows = 10
    data = {
        "timestamp": pd.date_range("2026-09-01", periods=rows, freq="1s", tz="UTC"),
        "p_trade": [100.0 + i for i in range(rows)],
        "p_open": [100.0 + i for i in range(rows)],
        "p_close": [100.0 + i for i in range(rows)],
        "p_trade_mean": [100.0 + i for i in range(rows)],
        "p_high": [101.0 + i for i in range(rows)],
        "p_low": [99.0 + i for i in range(rows)],
        "v_trade": [2.0] * rows,
        "v_buy": [1.5] * rows,
        "v_sell": [0.5] * rows,
    }
    for level in range(1, 11):
        data[f"p_bid_{level}"] = [99.0 + i for i in range(rows)]
        data[f"p_ask_{level}"] = [101.0 + i for i in range(rows)]
        data[f"q_bid_{level}"] = [2.0] * rows
        data[f"q_ask_{level}"] = [3.0] * rows
    return pd.DataFrame(data)


def test_v1_features_and_synthetic_targets() -> None:
    source = frame()
    features = compute_v1_features(source, bar_seconds=1, venue="binance")
    assert {"wap_1", "microprice_10", "obi_5", "ofi", "gk_vol_60s"}.issubset(features.columns)
    assert features["asset"].eq("BTC").all()

    targets = compute_synthetic_targets({"binance": source, "gemini": source}, bar_seconds=1)
    assert targets["synthetic_price"].iloc[0] == 100.0
    assert {"target_vol_60s", "target_vol_300s", "target_vol_3600s"}.issubset(targets)

    dataset = build_v1_dataset({"binance": source, "gemini": source}, bar_seconds=1)
    assert "venue" not in dataset
    assert len(dataset) == len(source)

    multi_frequency = build_v1_dataset_by_frequency({1: {"binance": source}, 5: {"binance": source}})
    assert set(multi_frequency["frequency_seconds"]) == {1, 5}
