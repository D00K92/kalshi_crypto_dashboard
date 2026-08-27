"""Deterministic GCS object names for archived batches."""

from __future__ import annotations

from hashlib import sha256
import re

from gcs_exporter.models import OrderBookRow, TradeRow


_SAFE_PART = re.compile(r"[^A-Za-z0-9_.-]+")


def _safe(value: str) -> str:
    normalized = _SAFE_PART.sub("_", value)
    if not normalized:
        raise ValueError("partition value contains no safe characters")
    return normalized


def trade_object_name(rows: list[TradeRow]) -> str:
    if not rows:
        raise ValueError("cannot name an empty batch")
    partitions = {row.partition for row in rows}
    if len(partitions) != 1:
        raise ValueError("one object cannot span multiple partitions")
    venue, instrument, date, hour = partitions.pop()
    first_id = _safe(rows[0].redis_id)
    last_id = _safe(rows[-1].redis_id)
    identity = sha256(
        "\n".join(row.redis_id for row in rows).encode("utf-8")
    ).hexdigest()[:12]
    return (
        f"ticks/venue={_safe(venue)}/instrument={_safe(instrument)}/"
        f"date={date}/hour={hour}/"
        f"btc_ticks_{date.replace('-', '')}_{hour}_{first_id}_{last_id}_{identity}.parquet"
    )


def dead_letter_object_name(redis_id: str, payload: bytes) -> str:
    identity = sha256(redis_id.encode("utf-8") + b"\0" + payload).hexdigest()[:16]
    return f"dead-letter/stream=ticks/redis_id={_safe(redis_id)}_{identity}.json"


def book_object_name(rows: list[OrderBookRow]) -> str:
    if not rows:
        raise ValueError("cannot name an empty batch")
    partitions = {row.partition for row in rows}
    if len(partitions) != 1:
        raise ValueError("one object cannot span multiple partitions")
    venue, instrument, date, hour = partitions.pop()
    identity = sha256("\n".join(row.redis_id for row in rows).encode()).hexdigest()[:12]
    return f"books/venue={_safe(venue)}/instrument={_safe(instrument)}/date={date}/hour={hour}/btc_books_{date.replace('-', '')}_{hour}_{_safe(rows[0].redis_id)}_{_safe(rows[-1].redis_id)}_{identity}.parquet"
