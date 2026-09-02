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
    calls: dict[str, object] = {"loads": 0, "resamples": [], "writes": []}

    def fake_load_venue_events(**kwargs):
        calls["loads"] += 1
        calls["source_partitions"] = kwargs["source_partitions"]
        source = dd.from_pandas(pd.DataFrame({"value": [1]}), npartitions=1)
        return source, source

    def fake_resample_events(_ticks, _books, venue, frequency):
        calls["resamples"].append((venue, frequency))
        periods = {"1h": 1, "30min": 2}[frequency]
        return dd.from_pandas(
            pd.DataFrame(
                {
                    "timestamp": pd.date_range("2026-09-01T00:00:00Z", periods=periods, freq=frequency),
                    "venue": ["binance"] * periods,
                    "p_trade": [100.0] * periods,
                }
            ),
            npartitions=1,
        )

    def fake_write(result, output, day, hour, venue):
        calls["writes"].append((result.compute(), output, day, hour, venue))
        return "gs://destination"

    monkeypatch.setattr(run_hourly_resampling, "_load_venue_events", fake_load_venue_events)
    monkeypatch.setattr(run_hourly_resampling, "_resample_events", fake_resample_events)
    monkeypatch.setattr(run_hourly_resampling, "write_hour_partition", fake_write)

    run_hourly(object(), "bucket", "processed/resampled_market_data", target, ("binance",), ("1h", "30m"))

    assert calls["loads"] == 1
    assert calls["source_partitions"] == (
        (datetime(2026, 8, 31).date(), "23"),
        (datetime(2026, 9, 1).date(), "00"),
    )
    assert calls["resamples"] == [("binance", "1h"), ("binance", "30min")]
    assert [len(written[0]) for written in calls["writes"]] == [1, 2]
    assert calls["writes"][0][1:] == (
        "gs://bucket/processed/resampled_market_data/frequency=1h",
        datetime(2026, 9, 1).date(),
        "00",
        "binance",
    )
