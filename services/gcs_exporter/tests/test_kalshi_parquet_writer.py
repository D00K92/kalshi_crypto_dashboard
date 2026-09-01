import io
import json

import pyarrow.parquet as pq

from gcs_exporter.models import KalshiOrderBookRow, KalshiTickerRow, KalshiTradeRow
from gcs_exporter.parquet_writer import (
    KALSHI_ORDERBOOK_SCHEMA,
    KALSHI_TICKER_SCHEMA,
    KALSHI_TRADE_SCHEMA,
    write_kalshi_orderbooks,
    write_kalshi_tickers,
    write_kalshi_trades,
)
from test_models import kalshi_orderbook_entry, kalshi_ticker_entry, kalshi_trade_entry


def test_write_kalshi_tickers_schema_and_nullable_prices() -> None:
    data = write_kalshi_tickers([KalshiTickerRow.from_entry(kalshi_ticker_entry())])
    table = pq.read_table(io.BytesIO(data))

    assert table.schema == KALSHI_TICKER_SCHEMA
    assert not {"venue", "instrument", "series_ticker", "event_ticker", "market_ticker"} & set(table.column_names)
    assert table.schema.metadata[b"dataset"] == b"kalshi_tickers"
    assert table.to_pylist()[0]["last_price_dollars"] is None


def test_write_kalshi_trades_schema_and_decimal_strings() -> None:
    data = write_kalshi_trades([KalshiTradeRow.from_entry(kalshi_trade_entry())])
    table = pq.read_table(io.BytesIO(data))

    assert table.schema == KALSHI_TRADE_SCHEMA
    assert not {"venue", "instrument", "series_ticker", "event_ticker", "market_ticker"} & set(table.column_names)
    assert table.to_pylist()[0]["yes_price_dollars"] == "0.51"


def test_write_kalshi_orderbooks_schema_and_levels_json() -> None:
    data = write_kalshi_orderbooks([KalshiOrderBookRow.from_entry(kalshi_orderbook_entry())])
    table = pq.read_table(io.BytesIO(data))
    record = table.to_pylist()[0]

    assert table.schema == KALSHI_ORDERBOOK_SCHEMA
    assert not {"venue", "instrument", "series_ticker", "event_ticker", "market_ticker"} & set(table.column_names)
    assert record["exchange_ts_ms"] is None
    assert json.loads(record["yes_bids"])[0]["quantity"] == "10.00"
