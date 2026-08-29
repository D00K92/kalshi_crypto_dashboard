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


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return parsed


def _non_negative_float(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite non-negative number") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field} must be a finite non-negative number")
    return parsed


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field)


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
            sequence=_non_negative_int(payload.get("sequence"), "sequence"),
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


@dataclass(frozen=True, slots=True)
class KalshiTickerRow:
    redis_id: str
    event_id: str
    venue: str
    instrument: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    yes_bid_dollars: str | None
    yes_ask_dollars: str | None
    last_price_dollars: str | None
    volume: str | None
    open_interest: str | None
    exchange_ts_ms: int
    received_ts_ms: int
    schema_version: int

    @classmethod
    def from_entry(cls, entry: RawStreamEntry) -> "KalshiTickerRow":
        payload = _payload(entry)
        if payload.get("event_type") != "kalshi_ticker":
            raise ValueError("payload event_type must be 'kalshi_ticker'")
        return cls(
            redis_id=entry.redis_id,
            event_id=_text(payload.get("event_id"), "event_id"),
            venue=_kalshi_venue(payload),
            instrument=_text(payload.get("instrument"), "instrument").upper(),
            series_ticker=_text(payload.get("series_ticker"), "series_ticker"),
            event_ticker=_text(payload.get("event_ticker"), "event_ticker"),
            market_ticker=_text(payload.get("market_ticker"), "market_ticker"),
            yes_bid_dollars=_optional_text(payload.get("yes_bid_dollars"), "yes_bid_dollars"),
            yes_ask_dollars=_optional_text(payload.get("yes_ask_dollars"), "yes_ask_dollars"),
            last_price_dollars=_optional_text(payload.get("last_price_dollars"), "last_price_dollars"),
            volume=_optional_text(payload.get("volume"), "volume"),
            open_interest=_optional_text(payload.get("open_interest"), "open_interest"),
            exchange_ts_ms=_positive_int(payload.get("exchange_ts_ms"), "exchange_ts_ms"),
            received_ts_ms=_positive_int(payload.get("received_ts_ms"), "received_ts_ms"),
            schema_version=_schema_version(payload),
        )

    @property
    def partition(self) -> tuple[str, str, str, str, str, str]:
        instant = datetime.fromtimestamp(self.exchange_ts_ms / 1000, tz=timezone.utc)
        return (
            self.series_ticker,
            self.event_ticker,
            self.market_ticker,
            self.instrument,
            instant.strftime("%Y-%m-%d"),
            instant.strftime("%H"),
        )


@dataclass(frozen=True, slots=True)
class KalshiTradeRow:
    redis_id: str
    event_id: str
    venue: str
    instrument: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    trade_id: str
    yes_price_dollars: str
    count: str
    taker_side: str
    exchange_ts_ms: int
    received_ts_ms: int
    schema_version: int

    @classmethod
    def from_entry(cls, entry: RawStreamEntry) -> "KalshiTradeRow":
        payload = _payload(entry)
        if payload.get("event_type") != "kalshi_trade":
            raise ValueError("payload event_type must be 'kalshi_trade'")
        side = _text(payload.get("taker_side"), "taker_side")
        if side not in {"yes", "no"}:
            raise ValueError("taker_side must be 'yes' or 'no'")
        return cls(
            redis_id=entry.redis_id,
            event_id=_text(payload.get("event_id"), "event_id"),
            venue=_kalshi_venue(payload),
            instrument=_text(payload.get("instrument"), "instrument").upper(),
            series_ticker=_text(payload.get("series_ticker"), "series_ticker"),
            event_ticker=_text(payload.get("event_ticker"), "event_ticker"),
            market_ticker=_text(payload.get("market_ticker"), "market_ticker"),
            trade_id=_text(payload.get("trade_id"), "trade_id"),
            yes_price_dollars=_text(payload.get("yes_price_dollars"), "yes_price_dollars"),
            count=_text(payload.get("count"), "count"),
            taker_side=side,
            exchange_ts_ms=_positive_int(payload.get("exchange_ts_ms"), "exchange_ts_ms"),
            received_ts_ms=_positive_int(payload.get("received_ts_ms"), "received_ts_ms"),
            schema_version=_schema_version(payload),
        )

    @property
    def partition(self) -> tuple[str, str, str, str, str, str]:
        instant = datetime.fromtimestamp(self.exchange_ts_ms / 1000, tz=timezone.utc)
        return (
            self.series_ticker,
            self.event_ticker,
            self.market_ticker,
            self.instrument,
            instant.strftime("%Y-%m-%d"),
            instant.strftime("%H"),
        )


@dataclass(frozen=True, slots=True)
class KalshiOrderBookRow:
    redis_id: str
    event_id: str
    event_type: str
    venue: str
    instrument: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    sequence: int
    yes_bids: tuple[tuple[str, str], ...]
    no_bids: tuple[tuple[str, str], ...]
    exchange_ts_ms: int | None
    received_ts_ms: int
    delta_price_dollars: str | None
    delta_fp: str | None
    delta_side: str | None
    schema_version: int

    @classmethod
    def from_entry(cls, entry: RawStreamEntry) -> "KalshiOrderBookRow":
        payload = _payload(entry)
        event_type = _text(payload.get("event_type"), "event_type")
        if event_type not in {"kalshi_orderbook_snapshot", "kalshi_orderbook_delta"}:
            raise ValueError("payload event_type must be a Kalshi order-book event")
        delta_side = _optional_text(payload.get("delta_side"), "delta_side")
        if delta_side is not None and delta_side not in {"yes", "no"}:
            raise ValueError("delta_side must be 'yes' or 'no'")
        exchange_ts_ms = payload.get("exchange_ts_ms")
        received_ts_ms = _positive_int(payload.get("received_ts_ms"), "received_ts_ms")
        return cls(
            redis_id=entry.redis_id,
            event_id=_text(payload.get("event_id"), "event_id"),
            event_type=event_type,
            venue=_kalshi_venue(payload),
            instrument=_text(payload.get("instrument"), "instrument").upper(),
            series_ticker=_text(payload.get("series_ticker"), "series_ticker"),
            event_ticker=_text(payload.get("event_ticker"), "event_ticker"),
            market_ticker=_text(payload.get("market_ticker"), "market_ticker"),
            sequence=_non_negative_int(payload.get("sequence"), "sequence"),
            yes_bids=_kalshi_levels(payload.get("yes_bids"), "yes_bids"),
            no_bids=_kalshi_levels(payload.get("no_bids"), "no_bids"),
            exchange_ts_ms=None if exchange_ts_ms is None else _positive_int(exchange_ts_ms, "exchange_ts_ms"),
            received_ts_ms=received_ts_ms,
            delta_price_dollars=_optional_text(payload.get("delta_price_dollars"), "delta_price_dollars"),
            delta_fp=_optional_text(payload.get("delta_fp"), "delta_fp"),
            delta_side=delta_side,
            schema_version=_schema_version(payload),
        )

    @property
    def partition(self) -> tuple[str, str, str, str, str, str]:
        timestamp_ms = self.exchange_ts_ms or self.received_ts_ms
        instant = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return (
            self.series_ticker,
            self.event_ticker,
            self.market_ticker,
            self.instrument,
            instant.strftime("%Y-%m-%d"),
            instant.strftime("%H"),
        )


def _payload(entry: RawStreamEntry) -> dict[str, Any]:
    try:
        payload = orjson.loads(entry.payload_bytes())
    except orjson.JSONDecodeError as exc:
        raise ValueError("payload is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    return payload


def _schema_version(payload: Mapping[str, Any]) -> int:
    schema_version = _positive_int(payload.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise ValueError(f"unsupported schema_version {schema_version}")
    return schema_version


def _kalshi_venue(payload: Mapping[str, Any]) -> str:
    venue = _text(payload.get("venue"), "venue").lower()
    if venue != "kalshi":
        raise ValueError("venue must be 'kalshi'")
    return venue


def _kalshi_levels(value: Any, field: str) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")
    result: list[tuple[str, str]] = []
    for index, level in enumerate(value):
        if not isinstance(level, dict):
            raise ValueError(f"{field}[{index}] must be an object")
        result.append((_text(level.get("price"), f"{field}.price"), _text(level.get("quantity"), f"{field}.quantity")))
    return tuple(result)
