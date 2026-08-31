"""Bitstamp WebSocket v2 adapter for public BTC/USD trades and L2 snapshots."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
import logging
import time
from typing import Any

import orjson
from websockets.asyncio.client import connect

from ingestion.models import BookLevel, BookSnapshot, MarketEvent, Trade
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
VENUE = "bitstamp"
MAX_BOOK_LEVELS = 15


class BitstampMessageError(ValueError):
    pass


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BitstampMessageError(f"{field} must be an object")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise BitstampMessageError(f"{field} must be a non-empty string")
    return value


def _integer(value: object, field: str) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise BitstampMessageError(f"{field} must be an integer") from exc


def _levels(value: object, field: str, *, reverse: bool) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise BitstampMessageError(f"{field} must be an array")
    parsed: list[BookLevel] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, list) or len(raw) < 2:
            raise BitstampMessageError(f"{field}[{index}] must contain price and quantity")
        parsed.append(BookLevel(
            price=_text(raw[0], f"{field}[{index}].price"),
            quantity=_text(raw[1], f"{field}[{index}].quantity"),
        ))
    return tuple(sorted(parsed, key=lambda level: Decimal(level.price), reverse=reverse))


def _exchange_ts_ms(data: dict[str, Any]) -> int:
    if data.get("microtimestamp"):
        return _integer(data["microtimestamp"], "data.microtimestamp") // 1_000
    return _integer(data.get("timestamp"), "data.timestamp") * 1_000


def parse_bitstamp_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> MarketEvent | None:
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        frame = _mapping(orjson.loads(raw), "frame")
    except orjson.JSONDecodeError as exc:
        raise BitstampMessageError("frame is not valid JSON") from exc
    channel = _text(frame.get("channel"), "channel")
    if frame.get("event") in {"bts:subscription_succeeded", "bts:request_reconnect", "info"}:
        return None
    data = _mapping(frame.get("data"), "data")
    instrument = "BTCUSD"
    if channel == "live_trades_btcusd" and frame.get("event") == "trade":
        raw_trade_id = data.get("id")
        if raw_trade_id is None or (isinstance(raw_trade_id, str) and not raw_trade_id):
            raise BitstampMessageError("data.id must be a non-empty identifier")
        trade_id = str(raw_trade_id)
        trade_type = _integer(data.get("type"), "data.type")
        if trade_type not in {0, 1}:
            raise BitstampMessageError("data.type must be 0 or 1")
        return Trade(
            event_id=f"{VENUE}:{instrument}:trade:{trade_id}", event_type="trade",
            venue=VENUE, instrument=instrument, trade_id=trade_id,
            price=_text(data.get("price_str") or data.get("price"), "data.price"),
            quantity=_text(data.get("amount_str") or data.get("amount"), "data.amount"),
            taker_side="buy" if trade_type == 0 else "sell",
            exchange_ts_ms=_exchange_ts_ms(data), received_ts_ms=received_ts_ms,
        )
    if channel == "order_book_btcusd" and frame.get("event") == "data":
        bids = _levels(data.get("bids"), "data.bids", reverse=True)[:MAX_BOOK_LEVELS]
        asks = _levels(data.get("asks"), "data.asks", reverse=False)[:MAX_BOOK_LEVELS]
        if not bids or not asks:
            raise BitstampMessageError("order book must contain both sides")
        if Decimal(bids[0].price) >= Decimal(asks[0].price):
            best_bid = Decimal(bids[0].price)
            asks = tuple(level for level in asks if Decimal(level.price) > best_bid)
            if not bids or not asks:
                raise BitstampMessageError("order book snapshot is crossed")
        sequence = _integer(data.get("microtimestamp") or data.get("timestamp"), "data.sequence")
        return BookSnapshot(
            event_id=f"{VENUE}:{instrument}:book:{sequence}", event_type="book_snapshot",
            venue=VENUE, instrument=instrument, sequence=sequence, bids=bids, asks=asks,
            exchange_ts_ms=_exchange_ts_ms(data), received_ts_ms=received_ts_ms,
            depth=max(len(bids), len(asks)),
        )
    return None


@dataclass(slots=True)
class BitstampHealth:
    connected: bool = False
    synchronized: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


class BitstampFeed:
    def __init__(self, ws_url: str, symbol: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._symbol, self._pipeline = ws_url, symbol.lower(), pipeline
        self.health = BitstampHealth()

    async def run(self) -> None:
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5,
                                       ping_interval=20, max_size=1_048_576,
                                       max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                for channel in (f"live_trades_{self._symbol}", f"order_book_{self._symbol}"):
                    await websocket.send(orjson.dumps({"event": "bts:subscribe", "data": {"channel": channel}}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        event = parse_bitstamp_message(raw, received_ts_ms=received)
                    except (BitstampMessageError, ValueError) as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning("venue_message_rejected", extra={"venue": VENUE, "reason": str(exc)})
                        continue
                    if event is None:
                        continue
                    await self._pipeline.put(event)
                    self.health.last_event_received_ts_ms = received
                    if event.event_type == "book_snapshot":
                        self.health.synchronized = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": VENUE}, exc_info=True)
            finally:
                self.health.connected = False
                self.health.synchronized = False
