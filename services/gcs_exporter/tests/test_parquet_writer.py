from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from gcs_exporter.models import TradeRow
from gcs_exporter.parquet_writer import TRADE_SCHEMA, write_trades
from test_models import make_entry


def test_parquet_round_trip_has_fixed_schema_and_snappy_compression() -> None:
    row = TradeRow.from_entry(make_entry())

    data = write_trades([row])
    table = pq.read_table(pa.BufferReader(data))
    metadata = pq.ParquetFile(pa.BufferReader(data)).metadata

    assert table.schema == TRADE_SCHEMA
    assert table.column("redis_id").to_pylist() == [row.redis_id]
    assert table.column("quantity").type == pa.float32()
    assert metadata.row_group(0).column(0).compression == "SNAPPY"
