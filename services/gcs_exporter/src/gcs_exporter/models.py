"""Validated trade rows and raw Redis messages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from typing import Any, Mapping

import orjson


def _text(value: Any, field: str) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return parsed


def _non_negative_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field} must be a finite non-negative number")
    return parsed


@dataclass(frozen=True, slots=True)
class RawStreamEntry:
    redis_id: str
    fields: Mapping[bytes | str, bytes | str]

    def payload_bytes(self) -> bytes:
        payload = self.fields.get(b"payload", self.fields.get("payload"))
        if isinstance(payload, str):
            return payload.encode("utf-8")
        if isinstance(payload, bytes):
            return payload
        raise ValueError("Redis entry is missing payload")


@dataclass(frozen=True, slots=True)
class TradeRow:
    redis_id: str
    event_id: str
    venue: str
    instrument: str
    trade_id: str
    price: float
    quantity: float
    taker_side: str
    exchange_ts_ms: int
    received_ts_ms: int
    schema_version: int

    @classmethod
    def from_entry(cls, entry: RawStreamEntry) -> "TradeRow":
        try:
            payload = orjson.loads(entry.payload_bytes())
        except orjson.JSONDecodeError as exc:
            raise ValueError("payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        if payload.get("event_type") != "trade":
            raise ValueError("payload event_type must be 'trade'")

        side = _text(payload.get("taker_side"), "taker_side")
        if side not in {"buy", "sell"}:
            raise ValueError("taker_side must be 'buy' or 'sell'")

        schema_version = _positive_int(payload.get("schema_version"), "schema_version")
        if schema_version != 1:
            raise ValueError(f"unsupported schema_version {schema_version}")

        return cls(
            redis_id=entry.redis_id,
            event_id=_text(payload.get("event_id"), "event_id"),
            venue=_text(payload.get("venue"), "venue").lower(),
            instrument=_text(payload.get("instrument"), "instrument").upper(),
            trade_id=_text(payload.get("trade_id"), "trade_id"),
            price=_non_negative_float(payload.get("price"), "price"),
            quantity=_non_negative_float(payload.get("quantity"), "quantity"),
            taker_side=side,
            exchange_ts_ms=_positive_int(payload.get("exchange_ts_ms"), "exchange_ts_ms"),
            received_ts_ms=_positive_int(payload.get("received_ts_ms"), "received_ts_ms"),
            schema_version=schema_version,
        )

    @property
    def partition(self) -> tuple[str, str, str, str]:
        instant = datetime.fromtimestamp(self.exchange_ts_ms / 1000, tz=timezone.utc)
        return (
            self.venue,
            self.instrument,
            instant.strftime("%Y-%m-%d"),
            instant.strftime("%H"),
        )


@dataclass(frozen=True, slots=True)
class OrderBookRow:
    """Validated, source-compatible top-15 order-book snapshot."""

    redis_id: str
    event_id: str
    venue: str
    instrument: str
    sequence: int
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]
    exchange_ts_ms: int
    received_ts_ms: int
    depth: int
    schema_version: int

    @classmethod
    def from_entry(cls, entry: RawStreamEntry) -> "OrderBookRow":
        try:
            payload = orjson.loads(entry.payload_bytes())
        except orjson.JSONDecodeError as exc:
            raise ValueError("payload is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        if payload.get("event_type") != "book_snapshot":
            raise ValueError("payload event_type must be 'book_snapshot'")

        def levels(name: str) -> tuple[tuple[str, str], ...]:
            raw = payload.get(name)
            if not isinstance(raw, list) or not raw:
                raise ValueError(f"{name} must be a non-empty array")
            result: list[tuple[str, str]] = []
            for index, level in enumerate(raw[:15]):
                if not isinstance(level, dict):
                    raise ValueError(f"{name}[{index}] must be an object")
                result.append((_text(level.get("price"), f"{name}.price"), _text(level.get("quantity"), f"{name}.quantity")))
            return tuple(result)

        bids, asks = levels("bids"), levels("asks")
        if float(bids[0][0]) >= float(asks[0][0]):
            raise ValueError("book is crossed or locked")
        schema_version = _positive_int(payload.get("schema_version"), "schema_version")
        if schema_version != 1:
            raise ValueError(f"unsupported schema_version {schema_version}")
        received = _positive_int(payload.get("received_ts_ms"), "received_ts_ms")
        return cls(
            redis_id=entry.redis_id,
            event_id=_text(payload.get("event_id"), "event_id"),
            venue=_text(payload.get("venue"), "venue").lower(),
            instrument=_text(payload.get("instrument"), "instrument").upper(),
            sequence=_positive_int(payload.get("sequence"), "sequence"),
            bids=bids,
            asks=asks,
            exchange_ts_ms=_positive_int(payload.get("exchange_ts_ms") or received, "exchange_ts_ms"),
            received_ts_ms=received,
            depth=min(_positive_int(payload.get("depth"), "depth"), 15),
            schema_version=schema_version,
        )

    @property
    def partition(self) -> tuple[str, str, str, str]:
        instant = datetime.fromtimestamp(self.exchange_ts_ms / 1000, tz=timezone.utc)
        return self.venue, self.instrument, instant.strftime("%Y-%m-%d"), instant.strftime("%H")
