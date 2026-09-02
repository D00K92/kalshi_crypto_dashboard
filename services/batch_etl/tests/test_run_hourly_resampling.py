from __future__ import annotations

from datetime import datetime, timezone

import dask.dataframe as dd
import pandas as pd
import pytest

from scripts import run_hourly_resampling
from scripts.run_hourly_resampling import parse_selection, parse_target_hour, run_hourly


def test_parse_target_hour_defaults_to_previous_complete_utc_hour() -> None:
    now = datetime(2026, 9, 2, 14, 27, 31, tzinfo=timezone.utc)

    assert parse_target_hour(None, now) == datetime(2026, 9, 2, 13, tzinfo=timezone.utc)
    assert parse_target_hour("2026-09-01T08:00:00+09:00") == datetime(2026, 8, 31, 23, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="aligned"):
        parse_target_hour("2026-09-01T08:01:00Z")


def test_parse_selection_rejects_unknown_values() -> None:
    assert parse_selection("binance,coinbase", ("binance", "coinbase"), "venues") == ("binance", "coinbase")

    with pytest.raises(ValueError, match="invalid venues"):
        parse_selection("binance,unknown", ("binance",), "venues")


def test_run_hourly_reads_boundary_context_and_writes_only_target(monkeypatch) -> None:
    target = datetime(2026, 9, 1, 0, tzinfo=timezone.utc)
    calls: dict[str, object] = {}

    def fake_resample_venue(**kwargs):
        calls["source_partitions"] = kwargs["source_partitions"]
        return dd.from_pandas(
            pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(["2026-08-31T23:00:00Z", "2026-09-01T00:00:00Z"]),
                    "venue": ["binance", "binance"],
                    "p_trade": [99.0, 100.0],
                }
            ),
            npartitions=1,
        )

    def fake_write(result, output, day, hour, venue):
        calls["written"] = result.compute()
        calls["destination"] = (output, day, hour, venue)
        return "gs://destination"

    monkeypatch.setattr(run_hourly_resampling, "_resample_venue", fake_resample_venue)
    monkeypatch.setattr(run_hourly_resampling, "write_hour_partition", fake_write)

    run_hourly(object(), "bucket", "processed/resampled_market_data", target, ("binance",), ("1h",))

    assert calls["source_partitions"] == (
        (datetime(2026, 8, 31).date(), "23"),
        (datetime(2026, 9, 1).date(), "00"),
    )
    assert calls["written"]["timestamp"].tolist() == [pd.Timestamp("2026-09-01T00:00:00Z")]
    assert calls["destination"] == (
        "gs://bucket/processed/resampled_market_data/frequency=1h",
        datetime(2026, 9, 1).date(),
        "00",
        "binance",
    )
