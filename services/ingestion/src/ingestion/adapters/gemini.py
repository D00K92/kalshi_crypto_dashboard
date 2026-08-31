"""Gemini Spot WebSocket adapter for public BTCUSD trades and L2 snapshots."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any

import orjson
from websockets.asyncio.client import connect

from ingestion.models import BookLevel, BookSnapshot, MarketEvent, Trade
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
VENUE = "gemini"
MAX_BOOK_LEVELS = 15


class GeminiMessageError(ValueError):
    pass


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GeminiMessageError(f"{field} must be an object")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise GeminiMessageError(f"{field} must be a non-empty string")
    return value


def _integer(value: object, field: str) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise GeminiMessageError(f"{field} must be an integer") from exc


def _timestamp_ns(value: object, field: str) -> int:
    timestamp = _integer(value, field)
    return timestamp // 1_000_000 if timestamp > 10**14 else timestamp


def _levels(value: object, field: str) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise GeminiMessageError(f"{field} must be an array")
    levels = []
    for index, raw in enumerate(value):
        if not isinstance(raw, list) or len(raw) < 2:
            raise GeminiMessageError(f"{field}[{index}] must contain price and quantity")
        levels.append(BookLevel(_text(raw[0], f"{field}[{index}].price"), _text(raw[1], f"{field}[{index}].quantity")))
    return tuple(levels)


def parse_gemini_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> MarketEvent | None:
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        decoded = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise GeminiMessageError("frame is not valid JSON") from exc
    data = _mapping(decoded, "frame")
    if "status" in data:
        return None
    instrument = _text(data.get("symbol") or data.get("s"), "symbol/s").upper()
    if "p" in data and "q" in data and "t" in data:
        event_ts = data.get("E")
        return Trade(
            event_id=f"{VENUE}:{instrument}:trade:{_integer(data.get('t'), 't')}",
            event_type="trade", venue=VENUE, instrument=instrument,
            trade_id=str(_integer(data.get("t"), "t")), price=_text(data.get("p"), "p"),
            quantity=_text(data.get("q"), "q"),
            taker_side="sell" if data.get("m") is True else "buy",
            exchange_ts_ms=_timestamp_ns(event_ts, "E"), received_ts_ms=received_ts_ms,
        )
    if "bids" in data or "asks" in data:
        bids = _levels(data.get("bids"), "bids")[:MAX_BOOK_LEVELS]
        asks = _levels(data.get("asks"), "asks")[:MAX_BOOK_LEVELS]
        if not bids and not asks:
            raise GeminiMessageError("depth snapshot must contain a level")
        sequence = _integer(data.get("lastUpdateId"), "lastUpdateId")
        return BookSnapshot(
            event_id=f"{VENUE}:{instrument}:book:{sequence}", event_type="book_snapshot", venue=VENUE,
            instrument=instrument, sequence=sequence, bids=bids, asks=asks, exchange_ts_ms=None,
            received_ts_ms=received_ts_ms, depth=max(len(bids), len(asks)),
        )
    return None


@dataclass(slots=True)
class GeminiHealth:
    connected: bool = False
    synchronized: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


class GeminiFeed:
    def __init__(self, ws_url: str, symbol: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._symbol = ws_url, symbol.lower()
        self._pipeline = pipeline
        self.health = GeminiHealth()

    async def run(self) -> None:
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=20, max_size=1_048_576, max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                await websocket.send(orjson.dumps({"id": "1", "method": "subscribe", "params": [f"{self._symbol}@trade", f"{self._symbol}@depth20"]}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        event = parse_gemini_message(raw, received_ts_ms=received)
                    except GeminiMessageError as exc:
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
