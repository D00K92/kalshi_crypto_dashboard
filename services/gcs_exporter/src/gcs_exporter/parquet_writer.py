"""In-memory, schema-enforced Parquet serialization."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from gcs_exporter.models import TradeRow


TRADE_SCHEMA = pa.schema(
    [
        pa.field("redis_id", pa.string(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("venue", pa.string(), nullable=False),
        pa.field("instrument", pa.string(), nullable=False),
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
                "venue": row.venue,
                "instrument": row.instrument,
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
        use_dictionary=["venue", "instrument", "taker_side"],
        write_statistics=True,
    )
    return output.getvalue().to_pybytes()
