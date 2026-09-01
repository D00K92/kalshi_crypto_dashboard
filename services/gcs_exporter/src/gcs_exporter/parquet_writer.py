"""In-memory, schema-enforced Parquet serialization."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import orjson

from gcs_exporter.models import (
    KalshiOrderBookRow,
    KalshiTickerRow,
    KalshiTradeRow,
    OrderBookRow,
    TradeRow,
)


TRADE_SCHEMA = pa.schema(
    [
        pa.field("redis_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("quantity", pa.float32(), nullable=False),
        pa.field("taker_side", pa.string(), nullable=False),
        pa.field("exchange_ts_ms", pa.int64(), nullable=False),
        pa.field("received_ts_ms", pa.int64(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ],
    metadata={b"dataset": b"crypto_trades", b"schema_version": b"1"},
)


def write_trades(rows: list[TradeRow]) -> bytes:
    if not rows:
        raise ValueError("cannot serialize an empty trade batch")

    table = pa.Table.from_pylist(
        [
            {
                "redis_id": row.redis_id,
                "event_id": row.event_id,
                "trade_id": row.trade_id,
                "price": row.price,
                "quantity": row.quantity,
                "taker_side": row.taker_side,
                "exchange_ts_ms": row.exchange_ts_ms,
                "received_ts_ms": row.received_ts_ms,
                "schema_version": row.schema_version,
            }
            for row in rows
        ],
        schema=TRADE_SCHEMA,
    )
    output = pa.BufferOutputStream()
    pq.write_table(
        table,
        output,
        compression="snappy",
        use_dictionary=["taker_side"],
        write_statistics=True,
    )
    return output.getvalue().to_pybytes()


BOOK_SCHEMA = pa.schema(
    [
        pa.field("redis_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("bids", pa.string(), nullable=False),
        pa.field("asks", pa.string(), nullable=False),
        pa.field("exchange_ts_ms", pa.int64(), nullable=False),
        pa.field("received_ts_ms", pa.int64(), nullable=False),
        pa.field("depth", pa.int16(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ],
    metadata={b"dataset": b"crypto_orderbooks", b"schema_version": b"1"},
)


def write_books(rows: list[OrderBookRow]) -> bytes:
    if not rows:
        raise ValueError("cannot serialize an empty order-book batch")
    table = pa.Table.from_pylist(
        [
            {
                "redis_id": row.redis_id,
                "event_id": row.event_id,
                "sequence": row.sequence,
                "bids": orjson.dumps([{"price": p, "quantity": q} for p, q in row.bids]).decode(),
                "asks": orjson.dumps([{"price": p, "quantity": q} for p, q in row.asks]).decode(),
                "exchange_ts_ms": row.exchange_ts_ms,
                "received_ts_ms": row.received_ts_ms,
                "depth": row.depth,
                "schema_version": row.schema_version,
            }
            for row in rows
        ],
        schema=BOOK_SCHEMA,
    )
    output = pa.BufferOutputStream()
    pq.write_table(
        table,
        output,
        compression="snappy",
        use_dictionary=False,
        write_statistics=True,
    )
    return output.getvalue().to_pybytes()


KALSHI_TICKER_SCHEMA = pa.schema(
    [
        pa.field("redis_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("yes_bid_dollars", pa.string()),
        pa.field("yes_ask_dollars", pa.string()),
        pa.field("last_price_dollars", pa.string()),
        pa.field("volume", pa.string()),
        pa.field("open_interest", pa.string()),
        pa.field("exchange_ts_ms", pa.int64(), nullable=False),
        pa.field("received_ts_ms", pa.int64(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ],
    metadata={b"dataset": b"kalshi_tickers", b"schema_version": b"1"},
)


KALSHI_TRADE_SCHEMA = pa.schema(
    [
        pa.field("redis_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("yes_price_dollars", pa.string(), nullable=False),
        pa.field("count", pa.string(), nullable=False),
        pa.field("taker_side", pa.string(), nullable=False),
        pa.field("exchange_ts_ms", pa.int64(), nullable=False),
        pa.field("received_ts_ms", pa.int64(), nullable=False),
        pa.field("schema_version", pa.int16(), nullable=False),
    ],
    metadata={b"dataset": b"kalshi_trades", b"schema_version": b"1"},
)


KALSHI_ORDERBOOK_SCHEMA = pa.schema(
    [
        pa.field("redis_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("yes_bids", pa.string(), nullable=False),
        pa.field("no_bids", pa.string(), nullable=False),
        pa.field("exchange_ts_ms", pa.int64()),
        pa.field("received_ts_ms", pa.int64(), nullable=False),
        pa.field("delta_price_dollars", pa.string()),
        pa.field("delta_fp", pa.string()),
        pa.field("delta_side", pa.string()),
        pa.field("schema_version", pa.int16(), nullable=False),
    ],
    metadata={b"dataset": b"kalshi_orderbooks", b"schema_version": b"1"},
)


def write_kalshi_tickers(rows: list[KalshiTickerRow]) -> bytes:
    if not rows:
        raise ValueError("cannot serialize an empty Kalshi ticker batch")
    return _write(
        [
            {
                "redis_id": row.redis_id,
                "event_id": row.event_id,
                "yes_bid_dollars": row.yes_bid_dollars,
                "yes_ask_dollars": row.yes_ask_dollars,
                "last_price_dollars": row.last_price_dollars,
                "volume": row.volume,
                "open_interest": row.open_interest,
                "exchange_ts_ms": row.exchange_ts_ms,
                "received_ts_ms": row.received_ts_ms,
                "schema_version": row.schema_version,
            }
            for row in rows
        ],
        KALSHI_TICKER_SCHEMA,
    )


def write_kalshi_trades(rows: list[KalshiTradeRow]) -> bytes:
    if not rows:
        raise ValueError("cannot serialize an empty Kalshi trade batch")
    return _write(
        [
            {
                "redis_id": row.redis_id,
                "event_id": row.event_id,
                "trade_id": row.trade_id,
                "yes_price_dollars": row.yes_price_dollars,
                "count": row.count,
                "taker_side": row.taker_side,
                "exchange_ts_ms": row.exchange_ts_ms,
                "received_ts_ms": row.received_ts_ms,
                "schema_version": row.schema_version,
            }
            for row in rows
        ],
        KALSHI_TRADE_SCHEMA,
    )


def write_kalshi_orderbooks(rows: list[KalshiOrderBookRow]) -> bytes:
    if not rows:
        raise ValueError("cannot serialize an empty Kalshi order-book batch")
    return _write(
        [
            {
                "redis_id": row.redis_id,
                "event_id": row.event_id,
                "event_type": row.event_type,
                "sequence": row.sequence,
                "yes_bids": orjson.dumps([{"price": p, "quantity": q} for p, q in row.yes_bids]).decode(),
                "no_bids": orjson.dumps([{"price": p, "quantity": q} for p, q in row.no_bids]).decode(),
                "exchange_ts_ms": row.exchange_ts_ms,
                "received_ts_ms": row.received_ts_ms,
                "delta_price_dollars": row.delta_price_dollars,
                "delta_fp": row.delta_fp,
                "delta_side": row.delta_side,
                "schema_version": row.schema_version,
            }
            for row in rows
        ],
        KALSHI_ORDERBOOK_SCHEMA,
    )


def _write(records: list[dict[str, object]], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(records, schema=schema)
    output = pa.BufferOutputStream()
    pq.write_table(
        table,
        output,
        compression="snappy",
        use_dictionary=False,
        write_statistics=True,
    )
    return output.getvalue().to_pybytes()
