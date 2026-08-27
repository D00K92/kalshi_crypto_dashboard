"""Binance Spot WebSocket adapter for trades and bounded L2 snapshots."""

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
VENUE = "binance"


class BinanceMessageError(ValueError):
    """Raised when a Binance frame violates the expected market-data schema."""


@dataclass(slots=True)
class BinanceHealth:
    connected: bool = False
    synchronized: bool = False
    last_event_received_ts_ms: int | None = None
    last_error: str | None = None


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BinanceMessageError(f"{field} must be an object")
    return value


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise BinanceMessageError(f"{field} must be an integer")
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise BinanceMessageError(f"{field} must be an integer") from exc


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise BinanceMessageError(f"{field} must be a non-empty string")
    return value


def _levels(value: object, field: str) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise BinanceMessageError(f"{field} must be an array")
    levels: list[BookLevel] = []
    for index, raw_level in enumerate(value):
        if not isinstance(raw_level, list) or len(raw_level) < 2:
            raise BinanceMessageError(f"{field}[{index}] must contain price and quantity")
        levels.append(
            BookLevel(
                price=_text(raw_level[0], f"{field}[{index}].price"),
                quantity=_text(raw_level[1], f"{field}[{index}].quantity"),
            )
        )
    return tuple(levels)


def parse_binance_message(
    raw: str | bytes,
    *,
    received_ts_ms: int | None = None,
) -> MarketEvent | None:
    """Convert one combined-stream frame into a canonical event."""
    if received_ts_ms is None:
        received_ts_ms = time.time_ns() // 1_000_000
    try:
        decoded = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise BinanceMessageError("frame is not valid JSON") from exc

    envelope = _mapping(decoded, "frame")
    stream = envelope.get("stream")
    data = _mapping(envelope.get("data"), "data")

    if isinstance(stream, str) and stream.endswith("@trade"):
        instrument = _text(data.get("s"), "data.s")
        trade_id = str(_integer(data.get("t"), "data.t"))
        exchange_ts_ms = _integer(data.get("T"), "data.T")
        buyer_is_maker = data.get("m")
        if not isinstance(buyer_is_maker, bool):
            raise BinanceMessageError("data.m must be a boolean")
        return Trade(
            event_id=f"{VENUE}:{instrument}:trade:{trade_id}",
            event_type="trade",
            venue=VENUE,
            instrument=instrument,
            trade_id=trade_id,
            price=_text(data.get("p"), "data.p"),
            quantity=_text(data.get("q"), "data.q"),
            taker_side="sell" if buyer_is_maker else "buy",
            exchange_ts_ms=exchange_ts_ms,
            received_ts_ms=received_ts_ms,
        )

    if isinstance(stream, str) and "@depth" in stream:
        instrument = stream.split("@", 1)[0].upper()
        sequence = _integer(data.get("lastUpdateId"), "data.lastUpdateId")
        bids = _levels(data.get("bids"), "data.bids")
        asks = _levels(data.get("asks"), "data.asks")
        depth = max(len(bids), len(asks))
        if depth == 0:
            raise BinanceMessageError("depth snapshot must contain at least one level")
        return BookSnapshot(
            event_id=f"{VENUE}:{instrument}:book:{sequence}",
            event_type="book_snapshot",
            venue=VENUE,
            instrument=instrument,
            sequence=sequence,
            bids=bids,
            asks=asks,
            exchange_ts_ms=None,
            received_ts_ms=received_ts_ms,
            depth=depth,
        )

    return None


class BinanceFeed:
    def __init__(self, ws_url: str, pipeline: EventPipeline) -> None:
        self._ws_url = ws_url
        self._pipeline = pipeline
        self.health = BinanceHealth()

    async def run(self) -> None:
        async for websocket in connect(
            self._ws_url,
            open_timeout=10,
            close_timeout=5,
            ping_interval=None,
            max_size=1_048_576,
            max_queue=32,
            compression=None,
        ):
            self.health.connected = True
            self.health.last_error = None
            LOGGER.info("venue_connected", extra={"venue": VENUE})
            try:
                async for raw in websocket:
                    received_ts_ms = time.time_ns() // 1_000_000
                    try:
                        event = parse_binance_message(
                            raw, received_ts_ms=received_ts_ms
                        )
                    except BinanceMessageError as exc:
                        self.health.last_error = str(exc)
                        LOGGER.warning(
                            "venue_message_rejected",
                            extra={"venue": VENUE, "reason": str(exc)},
                        )
                        continue
                    if event is None:
                        continue
                    await self._pipeline.put(event)
                    self.health.last_event_received_ts_ms = received_ts_ms
                    if event.event_type == "book_snapshot":
                        self.health.synchronized = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.health.last_error = str(exc)
                LOGGER.warning(
                    "venue_connection_lost", extra={"venue": VENUE}, exc_info=True
                )
            finally:
                self.health.connected = False
                self.health.synchronized = False
