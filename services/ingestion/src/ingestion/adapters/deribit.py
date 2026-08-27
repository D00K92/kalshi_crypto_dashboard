"""Deribit JSON-RPC WebSocket adapter for the BTC_USDT spot tape."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import time

import orjson
from websockets.asyncio.client import connect

from ingestion.models import Trade
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
VENUE = "deribit"


class DeribitMessageError(ValueError):
    pass


@dataclass(slots=True)
class DeribitHealth:
    connected: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


def parse_deribit_message(raw: str | bytes, *, received_ts_ms: int | None = None) -> Trade | None:
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        frame = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise DeribitMessageError("frame is not valid JSON") from exc
    if not isinstance(frame, dict) or frame.get("method") != "subscription":
        return None
    params = frame.get("params")
    data = params.get("data") if isinstance(params, dict) else None
    if not isinstance(data, dict):
        raise DeribitMessageError("subscription data must be an object")
    instrument = data.get("instrument_name")
    trade_id = data.get("trade_id")
    if not isinstance(instrument, str) or not instrument or not isinstance(trade_id, str) or not trade_id:
        raise DeribitMessageError("trade requires instrument_name and trade_id")
    direction = data.get("direction")
    if direction not in {"buy", "sell"}:
        raise DeribitMessageError("direction must be buy or sell")
    try:
        exchange_ts = int(data["timestamp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise DeribitMessageError("timestamp must be an integer") from exc
    return Trade(event_id=f"{VENUE}:{instrument}:trade:{trade_id}", event_type="trade", venue=VENUE,
                 instrument=instrument, trade_id=trade_id, price=str(data["price"]),
                 quantity=str(data["amount"]), taker_side=direction,
                 exchange_ts_ms=exchange_ts, received_ts_ms=received_ts_ms)


class DeribitFeed:
    def __init__(self, ws_url: str, instrument: str, pipeline: EventPipeline) -> None:
        self._ws_url, self._instrument, self._pipeline = ws_url, instrument, pipeline
        self.health = DeribitHealth()

    async def run(self) -> None:
        channel = f"trades.{self._instrument}.raw"
        async for websocket in connect(self._ws_url, open_timeout=10, close_timeout=5, ping_interval=20,
                                       max_size=1_048_576, max_queue=32, compression=None):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                await websocket.send(orjson.dumps({"jsonrpc": "2.0", "id": 1, "method": "public/subscribe", "params": {"channels": [channel]}}))
                async for raw in websocket:
                    received = time.time_ns() // 1_000_000
                    try:
                        event = parse_deribit_message(raw, received_ts_ms=received)
                    except DeribitMessageError as exc:
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
