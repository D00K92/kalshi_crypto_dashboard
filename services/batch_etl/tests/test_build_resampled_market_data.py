from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from scripts import build_resampled_market_data
from scripts.build_resampled_market_data import (
    _decode_books,
    _resample_venue,
    _trade_events,
    hour_partition_path,
    source_partition_paths,
    write_dataset,
    write_hour_partition,
)


def test_trade_events_splits_taker_side_volume() -> None:
    raw = pd.DataFrame(
        {
            "exchange_ts_ms": [1_700_000_000_000, 1_700_000_001_000, 1_700_000_002_000],
            "price": ["100.0", "101.0", "102.0"],
            "quantity": ["1.5", "2.0", "3.0"],
            "taker_side": ["buy", "sell", "unknown"],
        }
    )

    events = _trade_events(raw)

    assert list(events.columns) == ["timestamp", "p_trade", "v_trade", "v_buy", "v_sell"]
    assert events["timestamp"].dt.tz is not None
    assert events["p_trade"].tolist() == [100.0, 101.0, 102.0]
    assert events["v_trade"].tolist() == [1.5, 2.0, 3.0]
    assert events["v_buy"].tolist() == [1.5, 0.0, 0.0]
    assert events["v_sell"].tolist() == [0.0, 2.0, 0.0]


def test_decode_books_extracts_top_level_and_tolerates_bad_rows() -> None:
    raw = pd.DataFrame(
        {
            "exchange_ts_ms": [1_700_000_000_000, 1_700_000_001_000],
            "bids": [
                json.dumps([
                    {"price": str(100 - level), "quantity": str(level)}
                    for level in range(1, 12)
                ]),
                "not-json",
            ],
            "asks": [
                [
                    {"price": str(100 + level), "quantity": str(level + 10)}
                    for level in range(1, 12)
                ],
                [],
            ],
        }
    )

    books = _decode_books(raw)

    assert "p_bid_10" in books.columns
    assert "q_ask_10" in books.columns
    assert "p_bid_11" not in books.columns
    assert books.loc[0, "p_bid_1"] == 99.0
    assert books.loc[0, "p_ask_1"] == 101.0
    assert books.loc[0, "q_bid_1"] == 1.0
    assert books.loc[0, "q_ask_1"] == 11.0
    assert books.loc[0, "p_bid_10"] == 90.0
    assert books.loc[0, "q_ask_10"] == 20.0
    assert np.isnan(books.loc[1, "p_bid_1"])
    assert np.isnan(books.loc[1, "p_ask_1"])


def test_resample_venue_uses_long_schema_and_requested_aggregations(monkeypatch) -> None:
    ticks = pd.DataFrame(
        {
            "exchange_ts_ms": [
                1_700_000_000_000,
                1_700_000_000_500,
                1_700_000_000_750,
                1_700_000_001_000,
            ],
            "price": ["100.0", "101.0", "103.0", "102.0"],
            "quantity": ["1.5", "0.0", "2.0", "2.0"],
            "taker_side": ["buy", "sell", "sell", "sell"],
        }
    )
    books = pd.DataFrame(
        {
            "exchange_ts_ms": [1_700_000_000_000, 1_700_000_000_500],
            "bids": [
                json.dumps([{"price": "99.0", "quantity": "1.0"}]),
                json.dumps([{"price": "98.0", "quantity": "2.0"}]),
            ],
            "asks": [
                json.dumps([{"price": "101.0", "quantity": "3.0"}]),
                json.dumps([{"price": "102.0", "quantity": "4.0"}]),
            ],
        }
    )

    def fake_read_parquet(_paths, **kwargs):
        if kwargs["columns"] == ["price", "quantity", "taker_side", "exchange_ts_ms"]:
            return dd.from_pandas(ticks, npartitions=1)
        return dd.from_pandas(books, npartitions=1)

    monkeypatch.setattr(build_resampled_market_data.dd, "read_parquet", fake_read_parquet)

    result = _resample_venue(
        fs=None,
        bucket="bucket",
        venue="binance",
        start=date(2023, 11, 14),
        end=date(2023, 11, 14),
        freq="1s",
        hour="22",
    ).compute()

    first = result.iloc[0]
    assert "venue" in result.columns
    assert not any(column.startswith("binance_") for column in result.columns)
    assert first["venue"] == "binance"
    assert first["p_trade"] == 103.0
    assert first["p_high"] == 103.0
    assert first["p_low"] == 100.0
    assert first["v_trade"] == 3.5
    assert first["v_buy"] == 1.5
    assert first["v_sell"] == 2.0
    assert first["cnt_trade"] == 2
    assert first["dt_fill_mean_ms"] == 750.0
    assert first["dt_fill_max_ms"] == 750.0
    assert first["dt_fill_min_ms"] == 750.0
    assert first["p_bid_1"] == 98.0
    assert first["p_ask_1"] == 102.0
    assert first["q_bid_1"] == 3.0
    assert first["q_ask_1"] == 7.0

    second = result.iloc[1]
    assert second["dt_fill_mean_ms"] == 250.0
    assert second["dt_fill_max_ms"] == 250.0
    assert second["dt_fill_min_ms"] == 250.0


def test_write_dataset_keeps_hive_keys_out_of_parquet_payload(tmp_path) -> None:
    output = tmp_path / "resampled"
    frame = dd.from_pandas(
        pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-09-01T08:00:00Z"]),
                "venue": ["binance"],
                "p_trade": [100.0],
                "date": ["2026-09-01"],
                "hour": ["08"],
            }
        ),
        npartitions=1,
    )

    write_dataset(frame, str(output), overwrite=True)

    parquet_file = next(output.rglob("*.parquet"))
    assert "date=2026-09-01/hour=08/venue=binance" in parquet_file.as_posix()
    assert pq.read_schema(parquet_file).names == ["timestamp", "p_trade"]

    logical_schema = ds.dataset(output, format="parquet", partitioning="hive").schema
    assert {"date", "hour", "venue"}.issubset(logical_schema.names)


def test_source_partition_paths_supports_midnight_context() -> None:
    paths = source_partition_paths(
        "bucket",
        "ticks",
        "binance",
        [(date(2026, 8, 31), "23"), (date(2026, 9, 1), "00")],
    )

    assert paths == [
        "gs://bucket/ticks/venue=binance/instrument=BTCUSDT/date=2026-08-31/hour=23/**/*.parquet",
        "gs://bucket/ticks/venue=binance/instrument=BTCUSDT/date=2026-09-01/hour=00/**/*.parquet",
    ]


def test_write_hour_partition_replaces_only_selected_hour(tmp_path) -> None:
    output = tmp_path / "frequency=1s"

    def frame(timestamp: str, price: float) -> dd.DataFrame:
        return dd.from_pandas(
            pd.DataFrame(
                {
                    "timestamp": pd.to_datetime([timestamp]),
                    "venue": ["binance"],
                    "p_trade": [price],
                }
            ),
            npartitions=1,
        )

    first_path = write_hour_partition(frame("2026-09-01T08:00:00Z", 100.0), str(output), date(2026, 9, 1), "08", "binance")
    second_path = write_hour_partition(frame("2026-09-01T09:00:00Z", 101.0), str(output), date(2026, 9, 1), "09", "binance")
    write_hour_partition(frame("2026-09-01T08:00:00Z", 102.0), str(output), date(2026, 9, 1), "08", "binance")

    assert first_path == hour_partition_path(str(output), date(2026, 9, 1), "08", "binance")
    assert len(list(Path(first_path).glob("*.parquet"))) == 1
    assert len(list(Path(second_path).glob("*.parquet"))) == 1
    assert pq.read_table(first_path).column("p_trade").to_pylist() == [102.0]
    assert pq.read_table(second_path).column("p_trade").to_pylist() == [101.0]
    parquet_file = next(Path(first_path).glob("*.parquet"))
    assert {"date", "hour", "venue"}.isdisjoint(pq.read_schema(parquet_file).names)
