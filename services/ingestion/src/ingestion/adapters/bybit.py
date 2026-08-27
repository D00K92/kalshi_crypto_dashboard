"""Bybit Spot WebSocket adapter for trades and order-book updates."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time

import orjson
from websockets.asyncio.client import connect

from ingestion.models import BookLevel, BookSnapshot, Trade
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
VENUE = "bybit"
MAX_BOOK_LEVELS = 15


class BybitMessageError(ValueError):
    pass


@dataclass(slots=True)
class BybitHealth:
    connected: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


class BybitBook:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.sequence = 0

    def apply(self, frame: dict[str, object], received_ts_ms: int) -> BookSnapshot | None:
        data = frame.get("data")
        if not isinstance(data, dict):
            raise BybitMessageError("data must be an object")
        if frame.get("type") == "snapshot":
            self.bids.clear(); self.asks.clear()
        for key, target in (("b", self.bids), ("a", self.asks)):
            levels = data.get(key, [])
            if not isinstance(levels, list): raise BybitMessageError(f"{key} must be an array")
            for level in levels:
                if not isinstance(level, list) or len(level) < 2: raise BybitMessageError("book level is invalid")
                price, size = str(level[0]), str(level[1])
                if float(size) == 0: target.pop(price, None)
                else: target[price] = size
        self.sequence = int(data.get("u", self.sequence + 1))
        bids = tuple(BookLevel(p, self.bids[p]) for p in sorted(self.bids, key=float, reverse=True)[:MAX_BOOK_LEVELS])
        asks = tuple(BookLevel(p, self.asks[p]) for p in sorted(self.asks, key=float)[:MAX_BOOK_LEVELS])
        if not bids or not asks: return None
        return BookSnapshot(event_id=f"{VENUE}:{self.symbol}:book:{self.sequence}", event_type="book_snapshot",
            venue=VENUE, instrument=self.symbol, sequence=self.sequence, bids=bids, asks=asks,
            exchange_ts_ms=int(frame.get("ts", received_ts_ms)), received_ts_ms=received_ts_ms,
            depth=max(len(bids), len(asks)))


def parse_bybit_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> list[Trade] | None:
    if received_ts_ms is None: received_ts_ms = time.time_ns() // 1_000_000
    try: frame = orjson.loads(raw)
    except orjson.JSONDecodeError as exc: raise BybitMessageError("frame is not valid JSON") from exc
    if not isinstance(frame, dict) or frame.get("topic", "").startswith("publicTrade.") is False: return None
    data = frame.get("data")
    if not isinstance(data, list): raise BybitMessageError("trade data must be an array")
    events: list[Trade] = []
    for item in data:
        if not isinstance(item, dict): raise BybitMessageError("trade item must be an object")
        side = item.get("S")
        if side not in {"Buy", "Sell"}: raise BybitMessageError("trade side is invalid")
        events.append(Trade(event_id=f"{VENUE}:{item['s']}:trade:{item['i']}", event_type="trade", venue=VENUE,
            instrument=str(item["s"]), trade_id=str(item["i"]), price=str(item["p"]), quantity=str(item["v"]),
            taker_side="buy" if side == "Buy" else "sell", exchange_ts_ms=int(item["T"]), received_ts_ms=received_ts_ms))
    return events


class BybitFeed:
    def __init__(self, ws_url: str, symbol: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._symbol, self._pipeline = ws_url, symbol, pipeline
        self._book = BybitBook(symbol)
        self.health = BybitHealth()

    async def run(self) -> None:
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=None,
                                       max_size=1_048_576, max_queue=32, compression=None):
            self.health.connected = True
            try:
                await websocket.send(orjson.dumps({"op": "subscribe", "args": [f"publicTrade.{self._symbol}", f"orderbook.50.{self._symbol}"]}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        events = parse_bybit_message(raw, received_ts_ms=received)
                        if events is not None:
                            for event in events: await self._pipeline.put(event)
                        else:
                            frame = orjson.loads(raw)
                            if isinstance(frame, dict) and str(frame.get("topic", "")).startswith("orderbook."):
                                book = self._book.apply(frame, received)
                                if book is not None: await self._pipeline.put(book)
                        self.health.last_event_received_ts_ms = received
                    except (BybitMessageError, KeyError, ValueError) as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning("venue_message_rejected", extra={"venue": VENUE, "reason": str(exc)})
            except asyncio.CancelledError: raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": VENUE}, exc_info=True)
            finally: self.health.connected = False
