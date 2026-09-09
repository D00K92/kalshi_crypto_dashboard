from __future__ import annotations

from datetime import date

import pandas as pd
from google.cloud import bigquery

from src.common.data_io import load_training_table_from_feast


def test_v2_loader_uses_exact_timestamp_join(monkeypatch) -> None:
    captured: dict[str, object] = {}
    frame = pd.DataFrame(
        {
            "asset": ["BTCUSD"],
            "event_timestamp": [pd.Timestamp("2026-09-01T00:00:00Z")],
            "synthetic_price": [100.0],
            "log_return": [0.01],
            "venue_count": [6],
            "market_id": ["BTCUSD"],
            "prediction_timestamp": [pd.Timestamp("2026-09-01T00:00:00Z")],
            "label_window_end": [pd.Timestamp("2026-09-01T01:00:00Z")],
            "target_rv_1m": [0.2],
            "target_rv_5m": [0.2],
            "target_rv_15m": [0.2],
            "target_rv_30m": [0.2],
            "target_rv_1h": [0.2],
            "label_version": ["v2_10s"],
        }
    )

    class QueryResult:
        def to_dataframe(self):
            return frame.copy()

    class Client:
        def query(self, query, job_config):
            captured["query"] = query
            captured["job_config"] = job_config
            return QueryResult()

    monkeypatch.setattr(bigquery, "Client", lambda **_: Client())

    result = load_training_table_from_feast(
        project="test-project",
        feast_repo="/unused",
        start=date(2026, 9, 1),
        end=date(2026, 9, 1),
        feature_version="v2_10s",
    )

    query = str(captured["query"])
    assert "realized_volatility_v2_10s" in query
    assert "f.event_timestamp = l.prediction_timestamp" in query
    assert "l.label_window_end <= CURRENT_TIMESTAMP()" in query
    assert len(result) == 1
    assert result.iloc[0]["frequency"] == "10s"
    assert result.attrs["feature_contract"] == "v2_10s"
