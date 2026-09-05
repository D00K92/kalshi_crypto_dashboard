from __future__ import annotations

import csv
import io
import importlib.util
from pathlib import Path
import zipfile


_SCRIPT = Path(__file__).parents[1] / "scripts" / "backfill_binance_trades.py"
_SPEC = importlib.util.spec_from_file_location("binance_backfill", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
_rows_from_csv = _MODULE._rows_from_csv
_timestamp_ms = _MODULE._timestamp_ms


def _archive(rows: list[list[str]]) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        csv_output = io.StringIO(newline="")
        writer = csv.writer(csv_output)
        writer.writerows(rows)
        archive.writestr("BTCUSDT-trades-2026-07-01.csv", csv_output.getvalue())
    return output.getvalue()


def test_microsecond_timestamp_is_normalized() -> None:
    assert _timestamp_ms("1735689600010866") == 1735689600010


def test_archive_rows_map_to_canonical_trades() -> None:
    payload = _archive(
        [
            ["1", "100000.10", "0.001", "100.0001", "1735689600010866", "False", "True"],
            ["2", "100000.20", "0.002", "200.0004", "1735689601010", "True", "True"],
        ]
    )

    rows = _rows_from_csv(payload, "BTCUSDT")

    assert rows[0].event_id == "binance:BTCUSDT:trade:1"
    assert rows[0].taker_side == "buy"
    assert rows[0].received_ts_ms == rows[0].exchange_ts_ms
    assert rows[1].taker_side == "sell"
