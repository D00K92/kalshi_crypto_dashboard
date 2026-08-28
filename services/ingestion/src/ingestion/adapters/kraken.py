"""Kraken Spot WebSocket v2 adapter for BTC/USD trades and L2 books."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
import logging
import math
import time
from typing import Any

import orjson
from websockets.asyncio.client import connect

from ingestion.models import BookLevel, BookSnapshot, MarketEvent, Trade
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
VENUE = "kraken"
MAX_BOOK_LEVELS = 15


class KrakenMessageError(ValueError):
    pass


@dataclass(slots=True)
class KrakenHealth:
    connected: bool = False
    synchronized: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


def _text(value: object, field: str) -> str:
    if isinstance(value, bool) or value is None:
        raise KrakenMessageError(f"{field} must be numeric text")
    text = str(value)
    try:
        number = float(text)
    except ValueError as exc:
        raise KrakenMessageError(f"{field} must be numeric text") from exc
    if not math.isfinite(number) or number < 0:
        raise KrakenMessageError(f"{field} must be finite and non-negative")
    return text


def _timestamp(value: object) -> int:
    if not isinstance(value, str) or not value:
        raise KrakenMessageError("timestamp must be an ISO-8601 string")
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError as exc:
        raise KrakenMessageError("timestamp must be an ISO-8601 string") from exc


def _levels(value: object, field: str) -> list[tuple[str, str]]:
    if not isinstance(value, list):
        raise KrakenMessageError(f"{field} must be an array")
    levels: list[tuple[str, str]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise KrakenMessageError(f"{field}[{index}] must be an object")
        levels.append((_text(raw.get("price"), f"{field}[{index}].price"), _text(raw.get("qty"), f"{field}[{index}].qty")))
    return levels


class KrakenBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.sequence = 0

    def apply(self, event: dict[str, Any], received_ts_ms: int) -> BookSnapshot | None:
        event_type = event.get("type")
        if event_type == "snapshot":
            self.bids.clear()
            self.asks.clear()
        elif event_type != "update":
            return None
        else:
            if "bids" not in event and "asks" not in event:
                raise KrakenMessageError("book update must contain bids or asks")

        for field, target in (("bids", self.bids), ("asks", self.asks)):
            if field not in event:
                continue
            for price, quantity in _levels(event[field], field):
                if float(quantity) == 0:
                    target.pop(price, None)
                else:
                    target[price] = quantity

        self.sequence += 1
        bids = tuple(BookLevel(price, self.bids[price]) for price in sorted(self.bids, key=float, reverse=True)[:MAX_BOOK_LEVELS])
        asks = tuple(BookLevel(price, self.asks[price]) for price in sorted(self.asks, key=float)[:MAX_BOOK_LEVELS])
        if not bids or not asks:
            return None
        timestamp = event.get("timestamp")
        exchange_ts = _timestamp(timestamp) if timestamp else None
        return BookSnapshot(
            event_id=f"{VENUE}:{self.symbol}:book:{self.sequence}",
            event_type="book_snapshot",
            venue=VENUE,
            instrument=self.symbol,
            sequence=self.sequence,
            bids=bids,
            asks=asks,
            exchange_ts_ms=exchange_ts,
            received_ts_ms=received_ts_ms,
            depth=max(len(bids), len(asks)),
        )


def parse_kraken_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> list[dict[str, Any]]:
    """Return normalized intermediate messages; book state is maintained by KrakenBook."""
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        frame = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise KrakenMessageError("frame is not valid JSON") from exc
    if not isinstance(frame, dict) or frame.get("channel") not in {"book", "trade"}:
        return []
    data = frame.get("data")
    # Subscription acknowledgements use the channel name but have no data.
    if data is None:
        return []
    if not isinstance(data, list):
        raise KrakenMessageError("data must be an array")
    return [
        {"channel": frame["channel"], "type": frame.get("type"), "payload": item, "received_ts_ms": received_ts_ms}
        for item in data
        if isinstance(item, dict) and item.get("symbol")
    ]


def trade_from_message(message: dict[str, Any]) -> Trade:
    data = message["payload"]
    symbol = str(data["symbol"])
    trade_id = str(data.get("trade_id"))
    if not trade_id or trade_id == "None":
        raise KrakenMessageError("trade_id is required")
    side = data.get("side")
    if side not in {"buy", "sell"}:
        raise KrakenMessageError("trade side must be buy or sell")
    exchange_ts = _timestamp(data.get("timestamp"))
    return Trade(
        event_id=f"{VENUE}:{symbol}:trade:{trade_id}", event_type="trade", venue=VENUE,
        instrument=symbol, trade_id=trade_id, price=_text(data.get("price"), "price"),
        quantity=_text(data.get("qty"), "qty"), taker_side=side,
        exchange_ts_ms=exchange_ts, received_ts_ms=message["received_ts_ms"],
    )


class KrakenFeed:
    def __init__(self, ws_url: str, symbol: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._symbol, self._pipeline = ws_url, symbol, pipeline
        self._book = KrakenBook(symbol)
        self.health = KrakenHealth()

    async def run(self) -> None:
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=20, max_size=1_048_576, max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                await websocket.send(orjson.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": [self._symbol]}}))
                await websocket.send(orjson.dumps({"method": "subscribe", "params": {"channel": "book", "symbol": [self._symbol], "depth": 25, "snapshot": True}}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        for message in parse_kraken_message(raw, received_ts_ms=received):
                            if message["channel"] == "trade":
                                await self._pipeline.put(trade_from_message(message))
                            else:
                                data = {**message["payload"], "type": message["type"]}
                                if data.get("symbol") != self._symbol:
                                    continue
                                snapshot = self._book.apply(data, received)
                                if snapshot is not None:
                                    await self._pipeline.put(snapshot)
                                    self.health.synchronized = True
                            self.health.last_event_received_ts_ms = received
                    except (KrakenMessageError, KeyError, TypeError) as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning("venue_message_rejected", extra={"venue": VENUE, "reason": str(exc)})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": VENUE}, exc_info=True)
            finally:
                self.health.connected = False
                self.health.synchronized = False
