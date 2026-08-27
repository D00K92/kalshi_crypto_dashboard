"""Canonical market events emitted by venue adapters."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import ClassVar, Literal, Protocol

import orjson


SCHEMA_VERSION = 1


def _require_non_negative_decimal(name: str, value: str) -> None:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal string, got {value!r}") from exc
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"{name} must be finite and non-negative, got {value!r}")


class MarketEvent(Protocol):
    stream_name: ClassVar[str]
    pubsub_channel: ClassVar[str]
    event_id: str
    event_type: str
    venue: str
    instrument: str

    def to_json(self) -> bytes: ...


@dataclass(frozen=True, slots=True)
class BookLevel:
    price: str
    quantity: str

    def __post_init__(self) -> None:
        _require_non_negative_decimal("price", self.price)
        _require_non_negative_decimal("quantity", self.quantity)


@dataclass(frozen=True, slots=True)
class Trade:
    stream_name: ClassVar[str] = "stream:ticks"
    pubsub_channel: ClassVar[str] = "pub:btc_ticks"

    event_id: str
    event_type: Literal["trade"]
    venue: str
    instrument: str
    trade_id: str
    price: str
    quantity: str
    taker_side: Literal["buy", "sell"]
    exchange_ts_ms: int
    received_ts_ms: int
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_non_negative_decimal("price", self.price)
        _require_non_negative_decimal("quantity", self.quantity)
        if self.exchange_ts_ms <= 0 or self.received_ts_ms <= 0:
            raise ValueError("event timestamps must be positive")

    def to_json(self) -> bytes:
        return orjson.dumps(self)


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    stream_name: ClassVar[str] = "stream:orderbook_snapshots"
    pubsub_channel: ClassVar[str] = "pub:orderbook"

    event_id: str
    event_type: Literal["book_snapshot"]
    venue: str
    instrument: str
    sequence: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    exchange_ts_ms: int | None
    received_ts_ms: int
    depth: int
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.sequence < 0:
            raise ValueError("book sequence must be non-negative")
        if self.exchange_ts_ms is not None and self.exchange_ts_ms <= 0:
            raise ValueError("exchange timestamp must be positive when provided")
        if self.received_ts_ms <= 0:
            raise ValueError("receive timestamp must be positive")
        if self.depth <= 0:
            raise ValueError("book depth must be positive")
        if len(self.bids) > self.depth or len(self.asks) > self.depth:
            raise ValueError("book contains more levels than its declared depth")
        if self.bids and self.asks:
            best_bid = Decimal(self.bids[0].price)
            best_ask = Decimal(self.asks[0].price)
            if best_bid >= best_ask:
                raise ValueError("book is crossed or locked")

    def to_json(self) -> bytes:
        return orjson.dumps(self)
