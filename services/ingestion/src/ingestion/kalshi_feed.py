"""KXBTCD discovery, WebSocket consumption, and safe hourly rotation."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import Any

import orjson
from websockets.asyncio.client import connect

from ingestion.adapters.kalshi import WS_PATH, KalshiMessageError, parse_kalshi_message
from ingestion.config import Settings
from ingestion.kalshi_auth import build_auth_headers, load_private_key
from ingestion.kalshi_discovery import KalshiEventSet, KalshiRestDiscovery
from ingestion.kalshi_state import KalshiBookState, KalshiRolloverState, KalshiStateError
from ingestion.pipeline.event_pipeline import EventPipeline

LOGGER = logging.getLogger(__name__)
CHANNELS = ("ticker", "trade", "orderbook_delta")


class KalshiFeed:
    def __init__(self, settings: Settings, pipeline: EventPipeline) -> None:
        self._settings = settings
        self._pipeline = pipeline
        private_key = load_private_key(settings.kalshi_private_key)
        self._discovery = KalshiRestDiscovery(settings.kalshi_rest_url, settings.kalshi_api_key, private_key)
        self._private_key = private_key
        self._state = KalshiRolloverState()
        self._books: dict[str, KalshiBookState] = {}
        self.last_error: str | None = None

    def _subscribe(self, market_tickers: tuple[str, ...], message_id: int) -> bytes:
        return orjson.dumps({"id": message_id, "cmd": "subscribe", "params": {"channels": list(CHANNELS), "market_tickers": list(market_tickers)}})

    def _update(self, sid: int, action: str, market_tickers: tuple[str, ...], message_id: int) -> bytes:
        return orjson.dumps({"id": message_id, "cmd": "update_subscription", "params": {"sids": [sid], "market_tickers": list(market_tickers), "action": action}})

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self._settings.kalshi_api_key or not self._settings.kalshi_private_key:
            LOGGER.info("venue_disabled", extra={"venue": "kalshi", "reason": "credentials_not_configured"})
            return
        delay = 1.0
        while not stop_event.is_set():
            try:
                if self._state.active is None:
                    self._state.active = await self._discovery.discover(self._settings.kalshi_series_ticker)
                await self._connection(stop_event)
                delay = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                LOGGER.warning("venue_connection_lost", extra={"venue": "kalshi"}, exc_info=True)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
                delay = min(delay * 2, 30.0)

    async def _connection(self, stop_event: asyncio.Event) -> None:
        active = self._state.active
        if active is None:
            return
        message_id = 1
        sid: int | None = None
        market_context: dict[str, KalshiEventSet] = {ticker: active for ticker in active.market_tickers}
        next_discovery = time.monotonic()
        headers = build_auth_headers(self._settings.kalshi_api_key, self._private_key, path=WS_PATH)
        async with connect(self._settings.kalshi_ws_url, additional_headers=headers, open_timeout=10, close_timeout=5, ping_interval=20, max_size=4_194_304, max_queue=256, compression=None) as websocket:
            await websocket.send(self._subscribe(active.market_tickers, message_id))
            message_id += 1
            LOGGER.info("venue_connected", extra={"venue": "kalshi", "event_ticker": active.event_ticker, "markets": len(active.market_tickers)})
            while not stop_event.is_set():
                if time.monotonic() >= next_discovery:
                    await self._check_rollover(websocket, sid, message_context=market_context, message_id=message_id)
                    message_id += 1
                    next_discovery = time.monotonic() + 15.0
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                except TimeoutError:
                    continue
                frame = orjson.loads(raw)
                if not isinstance(frame, dict):
                    continue
                if frame.get("type") == "subscribed":
                    candidate_sid = frame.get("msg", {}).get("sid")
                    if candidate_sid is not None:
                        sid = int(candidate_sid)
                    continue
                market = frame.get("msg", {}).get("market_ticker")
                event_set = market_context.get(market)
                if event_set is None:
                    continue
                try:
                    event = parse_kalshi_message(raw, series_ticker=event_set.series_ticker, event_ticker=event_set.event_ticker, received_ts_ms=time.time_ns() // 1_000_000)
                    if event is None:
                        continue
                    if self._state.pending and event_set.event_ticker == self._state.pending.event_ticker:
                        old_markets = self._state.confirm(event_set.event_ticker)
                        if sid is not None and old_markets:
                            await websocket.send(self._update(sid, "delete_markets", tuple(old_markets), message_id))
                            message_id += 1
                        for old_market in old_markets:
                            self._books.pop(old_market, None)
                        LOGGER.info("event_rollover_complete", extra={"venue": "kalshi", "event_ticker": event_set.event_ticker, "generation": self._state.generation})
                    if event.event_type.startswith("kalshi_orderbook"):
                        book = self._books.setdefault(event.market_ticker, KalshiBookState())
                        try:
                            book.apply(event)
                        except KalshiStateError as exc:
                            self.last_error = str(exc)
                            LOGGER.warning("kalshi_book_rejected", extra={"market_ticker": event.market_ticker, "reason": str(exc)})
                            continue
                    await self._pipeline.put(event)
                except (KalshiMessageError, KeyError, TypeError, ValueError) as exc:
                    self.last_error = str(exc)
                    LOGGER.warning("venue_message_rejected", extra={"venue": "kalshi", "reason": str(exc)})

    async def _check_rollover(self, websocket: Any, sid: int | None, *, message_context: dict[str, KalshiEventSet], message_id: int) -> None:
        if sid is None:
            # Do not stage a candidate until the subscription SID is known;
            # update_subscription cannot be issued safely without it.
            return
        try:
            candidate = await self._discovery.discover(self._settings.kalshi_series_ticker)
        except Exception as exc:
            self.last_error = str(exc)
            LOGGER.warning("kalshi_discovery_failed", extra={"reason": str(exc)})
            return
        if not self._state.stage(candidate):
            return
        for ticker in candidate.market_tickers:
            message_context[ticker] = candidate
        await websocket.send(self._update(sid, "add_markets", candidate.market_tickers, message_id))
