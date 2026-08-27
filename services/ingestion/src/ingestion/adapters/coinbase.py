"""Coinbase Exchange WebSocket adapter for BTC-USD matches."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time
from datetime import datetime

import orjson
from websockets.asyncio.client import connect

from ingestion.models import Trade
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
VENUE = "coinbase"


class CoinbaseMessageError(ValueError):
    pass


@dataclass(slots=True)
class CoinbaseHealth:
    connected: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


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


def parse_coinbase_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> Trade | None:
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise CoinbaseMessageError("frame is not valid JSON") from exc
    if not isinstance(data, dict) or data.get("type") not in {"match", "last_match"}:
        return None
    product = _text(data.get("product_id"), "product_id")
    raw_trade_id = data.get("trade_id")
    if isinstance(raw_trade_id, bool) or not isinstance(raw_trade_id, (str, int)):
        raise CoinbaseMessageError("trade_id must be a string or integer")
    trade_id = str(raw_trade_id)
    side = _text(data.get("side"), "side").lower()
    if side not in {"buy", "sell"}:
        raise CoinbaseMessageError("side must be buy or sell")
    return Trade(
        event_id=f"{VENUE}:{product}:trade:{trade_id}", event_type="trade", venue=VENUE,
        instrument=product, trade_id=trade_id, price=_text(data.get("price"), "price"),
        quantity=_text(data.get("size"), "size"), taker_side=side,
        exchange_ts_ms=_timestamp(data.get("time")), received_ts_ms=received_ts_ms,
    )


class CoinbaseFeed:
    def __init__(self, ws_url: str, product_id: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._product_id, self._pipeline = ws_url, product_id, pipeline
        self.health = CoinbaseHealth()

    async def run(self) -> None:
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=20,
                                       max_size=1_048_576, max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                await websocket.send(orjson.dumps({"type": "subscribe", "product_ids": [self._product_id], "channels": ["matches"]}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        event = parse_coinbase_message(raw, received_ts_ms=received)
                    except CoinbaseMessageError as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning("venue_message_rejected", extra={"venue": VENUE, "reason": str(exc)})
                        continue
                    if event is not None:
                        await self._pipeline.put(event)
                        self.health.last_event_received_ts_ms = received
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": VENUE}, exc_info=True)
            finally:
                self.health.connected = False
