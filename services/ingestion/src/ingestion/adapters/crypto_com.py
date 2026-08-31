"""Crypto.com Exchange public WebSocket adapter for BTC_USD market data."""

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
VENUE = "crypto.com"
MAX_BOOK_LEVELS = 15


class CryptoComMessageError(ValueError):
    pass


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CryptoComMessageError(f"{field} must be an object")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise CryptoComMessageError(f"{field} must be a non-empty string")
    return value


def _integer(value: object, field: str) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise CryptoComMessageError(f"{field} must be an integer") from exc


def _levels(value: object, field: str) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise CryptoComMessageError(f"{field} must be an array")
    result = []
    for index, raw in enumerate(value):
        if not isinstance(raw, list) or len(raw) < 2:
            raise CryptoComMessageError(f"{field}[{index}] must contain price and quantity")
        result.append(BookLevel(_text(raw[0], f"{field}[{index}].price"), _text(raw[1], f"{field}[{index}].quantity")))
    return tuple(result)


def parse_crypto_com_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> list[MarketEvent]:
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        message = _mapping(orjson.loads(raw), "frame")
    except orjson.JSONDecodeError as exc:
        raise CryptoComMessageError("frame is not valid JSON") from exc
    if message.get("method") == "public/heartbeat" or "result" not in message:
        return []
    result = _mapping(message.get("result"), "result")
    channel = _text(result.get("channel"), "result.channel")
    instrument = _text(result.get("instrument_name"), "result.instrument_name").upper().replace("_", "")
    data = result.get("data")
    if not isinstance(data, list):
        raise CryptoComMessageError("result.data must be an array")
    if channel.startswith("trade"):
        events: list[MarketEvent] = []
        for index, raw_trade in enumerate(data):
            trade = _mapping(raw_trade, f"result.data[{index}]")
            side = _text(trade.get("s"), "trade.s").upper()
            if side not in {"BUY", "SELL"}:
                raise CryptoComMessageError("trade.s must be BUY or SELL")
            trade_id = _text(trade.get("d"), "trade.d")
            events.append(Trade(event_id=f"{VENUE}:{instrument}:trade:{trade_id}", event_type="trade", venue=VENUE, instrument=instrument, trade_id=trade_id, price=_text(trade.get("p"), "trade.p"), quantity=_text(trade.get("q"), "trade.q"), taker_side=side.lower(), exchange_ts_ms=_integer(trade.get("t"), "trade.t"), received_ts_ms=received_ts_ms))
        return events
    if not channel.startswith("book") or not data:
        return []
    book = _mapping(data[0], "result.data[0]")
    bids = _levels(book.get("bids"), "book.bids")[:MAX_BOOK_LEVELS]
    asks = _levels(book.get("asks"), "book.asks")[:MAX_BOOK_LEVELS]
    if not bids or not asks:
        raise CryptoComMessageError("book snapshot must contain both sides")
    if Decimal(bids[0].price) >= Decimal(asks[0].price):
        best_bid = Decimal(bids[0].price)
        asks = tuple(level for level in asks if Decimal(level.price) > best_bid)
        if not bids or not asks:
            raise CryptoComMessageError("book snapshot is crossed")
    sequence = _integer(book.get("u") or message.get("id") or received_ts_ms, "book.sequence")
    return [BookSnapshot(event_id=f"{VENUE}:{instrument}:book:{sequence}", event_type="book_snapshot", venue=VENUE, instrument=instrument, sequence=sequence, bids=bids, asks=asks, exchange_ts_ms=_integer(book.get("tt"), "book.tt") if book.get("tt") else None, received_ts_ms=received_ts_ms, depth=max(len(bids), len(asks)))]


@dataclass(slots=True)
class CryptoComHealth:
    connected: bool = False
    synchronized: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


class CryptoComFeed:
    def __init__(self, ws_url: str, symbol: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._symbol, self._pipeline = ws_url, symbol.upper(), pipeline
        self.health = CryptoComHealth()

    async def run(self) -> None:
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=20, max_size=1_048_576, max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                # Crypto.com recommends a one-second delay after connect before subscribing.
                await asyncio.sleep(1)
                await websocket.send(orjson.dumps({"id": 1, "method": "subscribe", "params": {"channels": [f"trade.{self._symbol}", f"book.{self._symbol}.50"]}}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        frame = orjson.loads(raw)
                        if isinstance(frame, dict) and frame.get("method") == "public/heartbeat":
                            await websocket.send(orjson.dumps({"id": frame.get("id"), "method": "public/respond-heartbeat"}))
                            continue
                        events = parse_crypto_com_message(raw, received_ts_ms=received)
                    except (CryptoComMessageError, ValueError) as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning("venue_message_rejected", extra={"venue": VENUE, "reason": str(exc)})
                        continue
                    for event in events:
                        await self._pipeline.put(event)
                        self.health.last_event_received_ts_ms = received
                        if event.event_type == "book_snapshot":
                            self.health.synchronized = True
                LOGGER.warning("venue_stream_ended", extra={"venue": VENUE})
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": VENUE}, exc_info=True)
            finally:
                self.health.connected = False
                self.health.synchronized = False
