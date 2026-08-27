from __future__ import annotations

import logging

import orjson

from gcs_exporter.logging_config import JsonFormatter


def test_json_formatter_exposes_structured_batch_context() -> None:
    record = logging.LogRecord(
        name="gcs_exporter.service",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="parquet_batch_committed",
        args=(),
        exc_info=None,
    )
    record.object = "ticks/file.parquet"
    record.rows = 123
    record.bytes = 4567

    document = orjson.loads(JsonFormatter().format(record))

    assert document["message"] == "parquet_batch_committed"
    assert document["object"] == "ticks/file.parquet"
    assert document["rows"] == 123
    assert document["bytes"] == 4567
