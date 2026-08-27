from __future__ import annotations

from gcs_exporter.models import TradeRow
from gcs_exporter.object_names import trade_object_name
from test_models import make_entry


def test_object_name_is_partitioned_and_deterministic() -> None:
    rows = [
        TradeRow.from_entry(make_entry("1724677200000-0")),
        TradeRow.from_entry(make_entry("1724677200001-0")),
    ]

    first = trade_object_name(rows)

    assert first == trade_object_name(rows)
    assert first.startswith(
        "ticks/venue=binance/instrument=BTCUSDT/date=2024-08-26/hour=13/"
    )
    assert "1724677200000-0_1724677200001-0" in first
    assert first.endswith(".parquet")
