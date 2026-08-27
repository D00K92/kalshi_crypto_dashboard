from __future__ import annotations

from datetime import datetime, timezone

import orjson
import pytest

from gcs_exporter.models import RawStreamEntry, TradeRow


def make_entry(
    redis_id: str = "1724677200000-0", timestamp_ms: int = 1_724_677_200_000
) -> RawStreamEntry:
    payload = {
        "event_id": "binance:trade:123",
        "event_type": "trade",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "trade_id": "123",
        "price": "64234.12000000",
        "quantity": "0.00123000",
        "taker_side": "sell",
        "exchange_ts_ms": timestamp_ms,
        "received_ts_ms": timestamp_ms + 3,
        "schema_version": 1,
    }
    return RawStreamEntry(redis_id, {b"payload": orjson.dumps(payload)})


def test_trade_row_decodes_actual_nested_payload() -> None:
    row = TradeRow.from_entry(make_entry())

    assert row.redis_id == "1724677200000-0"
    assert row.venue == "binance"
    assert row.instrument == "BTCUSDT"
    assert row.price == 64234.12
    assert row.quantity == 0.00123
    assert row.partition == ("binance", "BTCUSDT", "2024-08-26", "13")


def test_invalid_json_is_rejected() -> None:
    entry = RawStreamEntry("1-0", {b"payload": b"not-json"})

    with pytest.raises(ValueError, match="valid JSON"):
        TradeRow.from_entry(entry)


def test_unknown_schema_version_is_rejected() -> None:
    payload = orjson.loads(make_entry().payload_bytes())
    payload["schema_version"] = 2

    with pytest.raises(ValueError, match="unsupported schema_version"):
        TradeRow.from_entry(RawStreamEntry("1-0", {b"payload": orjson.dumps(payload)}))
