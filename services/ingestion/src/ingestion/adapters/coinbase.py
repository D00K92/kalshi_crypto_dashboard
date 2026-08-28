"""Coinbase Advanced Trade WebSocket adapter for BTC-USD trades and L2."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time
from datetime import datetime

import orjson
from websockets.asyncio.client import connect

from ingestion.models import BookLevel, BookSnapshot, Trade
from ingestion.pipeline.event_pipeline import EventPipeline
from ingestion.adapters.coinbase_auth import build_coinbase_ws_jwt

LOGGER = logging.getLogger(__name__)
VENUE = "coinbase"
MAX_BOOK_LEVELS = 15


class CoinbaseMessageError(ValueError):
    pass


@dataclass(slots=True)
class CoinbaseHealth:
    connected: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


class CoinbaseBook:
    def __init__(self, product_id: str) -> None:
        self.product_id = product_id
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.sequence = 0

    def apply(self, data: dict[str, object], received_ts_ms: int) -> BookSnapshot | None:
        message_type = data.get("type")
        if message_type == "snapshot":
            self.bids.clear(); self.asks.clear()
            for side, target in (("bids", self.bids), ("asks", self.asks)):
                levels = data.get(side)
                if not isinstance(levels, list):
                    raise CoinbaseMessageError(f"{side} must be an array")
                for level in levels:
                    if not isinstance(level, list) or len(level) < 2:
                        raise CoinbaseMessageError(f"{side} level is invalid")
                    target[_text(level[0], f"{side}.price")] = _text(level[1], f"{side}.size")
            self.sequence = int(data.get("sequence", 0))
        elif message_type == "l2update":
            changes = data.get("changes")
            if not isinstance(changes, list):
                raise CoinbaseMessageError("changes must be an array")
            for change in changes:
                if not isinstance(change, list) or len(change) < 3:
                    raise CoinbaseMessageError("level2 change is invalid")
                side, price, size = change[0], _text(change[1], "change.price"), _text(change[2], "change.size")
                target = self.bids if side == "buy" else self.asks if side == "sell" else None
                if target is None:
                    raise CoinbaseMessageError("change side must be buy or sell")
                if size == "0": target.pop(price, None)
                else: target[price] = size
            self.sequence += 1
        else:
            return None
        bids = tuple(BookLevel(p, self.bids[p]) for p in sorted(self.bids, key=float, reverse=True)[:MAX_BOOK_LEVELS])
        asks = tuple(BookLevel(p, self.asks[p]) for p in sorted(self.asks, key=float)[:MAX_BOOK_LEVELS])
        if not bids or not asks:
            return None
        return BookSnapshot(event_id=f"{VENUE}:{self.product_id}:book:{self.sequence}", event_type="book_snapshot",
            venue=VENUE, instrument=self.product_id, sequence=self.sequence, bids=bids, asks=asks,
            exchange_ts_ms=None, received_ts_ms=received_ts_ms, depth=max(len(bids), len(asks)))

    def apply_advanced(self, event: dict[str, object], received_ts_ms: int) -> BookSnapshot | None:
        """Apply an Advanced Trade l2_data event (snapshot or update)."""
        event_type = event.get("type")
        if event_type == "snapshot":
            self.bids.clear()
            self.asks.clear()
        elif event_type != "update":
            return None
        updates = event.get("updates")
        if not isinstance(updates, list):
            raise CoinbaseMessageError("l2 event updates must be an array")
        for update in updates:
            if not isinstance(update, dict):
                raise CoinbaseMessageError("l2 update must be an object")
            side = update.get("side")
            target = self.bids if side == "bid" else self.asks if side in {"offer", "ask"} else None
            if target is None:
                raise CoinbaseMessageError("l2 update side must be bid or offer")
            price = _text(update.get("price_level"), "price_level")
            quantity = _text(update.get("new_quantity"), "new_quantity")
            if quantity == "0":
                target.pop(price, None)
            else:
                target[price] = quantity
        self.sequence += 1
        bids = tuple(BookLevel(p, self.bids[p]) for p in sorted(self.bids, key=float, reverse=True)[:MAX_BOOK_LEVELS])
        asks = tuple(BookLevel(p, self.asks[p]) for p in sorted(self.asks, key=float)[:MAX_BOOK_LEVELS])
        if not bids or not asks:
            return None
        return BookSnapshot(event_id=f"{VENUE}:{self.product_id}:book:{self.sequence}", event_type="book_snapshot",
            venue=VENUE, instrument=self.product_id, sequence=self.sequence, bids=bids, asks=asks,
            exchange_ts_ms=_timestamp(event.get("event_time")) if event.get("event_time") else None,
            received_ts_ms=received_ts_ms, depth=max(len(bids), len(asks)))


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise CoinbaseMessageError(f"{field} must be a non-empty string")
    return value


def _timestamp(value: object) -> int:
    text = _text(value, "time")
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError as exc:
        raise CoinbaseMessageError("time must be an ISO-8601 timestamp") from exc


def _parse_trade_dict(data: dict[str, object], received_ts_ms: int) -> Trade:
    product = _text(data.get("product_id"), "product_id")
    raw_trade_id = data.get("trade_id")
    if isinstance(raw_trade_id, bool) or not isinstance(raw_trade_id, (str, int)):
        raise CoinbaseMessageError("trade_id must be a string or integer")
    side = _text(data.get("side"), "side").lower()
    if side not in {"buy", "sell"}:
        raise CoinbaseMessageError("side must be buy or sell")
    return Trade(event_id=f"{VENUE}:{product}:trade:{raw_trade_id}", event_type="trade", venue=VENUE,
        instrument=product, trade_id=str(raw_trade_id), price=_text(data.get("price"), "price"),
        quantity=_text(data.get("size"), "size"), taker_side=side,
        exchange_ts_ms=_timestamp(data.get("time")), received_ts_ms=received_ts_ms)


def parse_coinbase_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> Trade | None:
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise CoinbaseMessageError("frame is not valid JSON") from exc
    if not isinstance(data, dict):
        return None
    if data.get("type") in {"match", "last_match"}:
        return _parse_trade_dict(data, received_ts_ms)
    if data.get("channel") != "market_trades":
        return None
    events = data.get("events")
    if not isinstance(events, list) or not events:
        return None
    first = events[0]
    trades = first.get("trades") if isinstance(first, dict) else None
    if not isinstance(trades, list) or not trades:
        return None
    trade = trades[0]
    if not isinstance(trade, dict):
        raise CoinbaseMessageError("trade_id must be a string or integer")
    return _parse_trade_dict({**trade, "time": trade.get("time") or data.get("timestamp")}, received_ts_ms)


class CoinbaseFeed:
    def __init__(self, ws_url: str, product_id: str, pipeline: EventPipeline, api_key: str = "", secret: str = "") -> None:
        self._ws_url, self._product_id, self._pipeline = ws_url, product_id, pipeline
        self._api_key, self._secret = api_key, secret
        self.health = CoinbaseHealth()
        self._book = CoinbaseBook(product_id)
        self._diagnostic_logged = False

    async def run(self) -> None:
        # Coinbase level2 snapshots can exceed the websockets default 1 MiB
        # frame limit. We cap the retained book to 15 levels after parsing.
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=20,
                                       max_size=None, max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                token = build_coinbase_ws_jwt(self._api_key, self._secret)
                for channel in ("market_trades", "level2"):
                    await websocket.send(orjson.dumps({"type": "subscribe", "channel": channel,
                        "product_ids": [self._product_id], "jwt": token}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        event = parse_coinbase_message(raw, received_ts_ms=received)
                        book_events: list[BookSnapshot] = []
                        if event is None:
                            decoded = orjson.loads(raw)
                            if isinstance(decoded, dict):
                                message_type = decoded.get("type") or decoded.get("channel")
                                if message_type in {"subscriptions", "error"}:
                                    LOGGER.info("coinbase_control_frame type=%s detail=%s", message_type, decoded.get("message"))
                                if not self._diagnostic_logged and decoded.get("channel") == "l2_data":
                                    LOGGER.info("coinbase_book_frame_shape channel=%s keys=%s", message_type, sorted(decoded.keys()))
                                    self._diagnostic_logged = True
                                if decoded.get("channel") == "l2_data":
                                    for book_event in decoded.get("events", []):
                                        if isinstance(book_event, dict) and book_event.get("product_id", self._product_id) == self._product_id:
                                            snapshot = self._book.apply_advanced(book_event, received)
                                            if snapshot is not None:
                                                book_events.append(snapshot)
                    except CoinbaseMessageError as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning("venue_message_rejected", extra={"venue": VENUE, "reason": str(exc)})
                        continue
                    if event is not None:
                        await self._pipeline.put(event)
                        self.health.last_event_received_ts_ms = received
                    for book_event in book_events:
                        await self._pipeline.put(book_event)
                        self.health.last_event_received_ts_ms = received
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": VENUE}, exc_info=True)
            finally:
                self.health.connected = False
