"""Authenticated Kalshi market-data WebSocket transport."""

from __future__ import annotations

import asyncio
import logging
import random
from hashlib import sha256
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import orjson
from websockets.asyncio.client import connect

from ingestion.kalshi_auth import build_auth_headers, load_private_key
from ingestion.models import BookLevel, KalshiOrderBookSnapshot, KalshiTicker, KalshiTrade, MarketEvent

LOGGER = logging.getLogger(__name__)
WS_PATH = "/trade-api/ws/v2"


class KalshiMessageError(ValueError):
    """Raised when a Kalshi market-data frame is malformed."""


def _text(value: object, field: str) -> str:
    if value is None or isinstance(value, bool) or not str(value):
        raise KalshiMessageError(f"{field} is required")
    return str(value)


def _timestamp(message: dict[str, Any], received_ts_ms: int) -> int:
    value = message.get("ts_ms")
    if value is None and message.get("ts") is not None:
        value = int(message["ts"]) * 1000
    if value is None:
        return received_ts_ms
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise KalshiMessageError("timestamp must be an integer") from exc
    if parsed <= 0:
        raise KalshiMessageError("timestamp must be positive")
    return parsed


def _levels(value: object, field: str) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise KalshiMessageError(f"{field} must be an array")
    result: list[BookLevel] = []
    for index, level in enumerate(value):
        if not isinstance(level, list) or len(level) != 2:
            raise KalshiMessageError(f"{field}[{index}] must contain price and quantity")
        result.append(BookLevel(_text(level[0], f"{field}[{index}].price"), _text(level[1], f"{field}[{index}].quantity")))
    return tuple(result)


def parse_kalshi_message(
    raw: str | bytes,
    *,
    series_ticker: str,
    event_ticker: str,
    received_ts_ms: int,
) -> MarketEvent | None:
    """Normalize one Kalshi frame; control/ack frames return ``None``."""
    try:
        frame = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise KalshiMessageError("frame is not valid JSON") from exc
    if not isinstance(frame, dict):
        raise KalshiMessageError("frame must be an object")
    event_type = frame.get("type")
    message = frame.get("msg")
    if event_type in {"subscribed", "ok", "error", "heartbeat", "market_lifecycle"}:
        return None
    if not isinstance(message, dict):
        raise KalshiMessageError("market-data frame must contain msg object")
    market = _text(message.get("market_ticker"), "market_ticker")
    instrument = "BTCUSD"
    exchange_ts = _timestamp(message, received_ts_ms)
    if event_type == "ticker":
        identity = sha256(orjson.dumps(message, option=orjson.OPT_SORT_KEYS)).hexdigest()[:16]
        return KalshiTicker(
            event_id=f"kalshi:{market}:ticker:{identity}", event_type="kalshi_ticker", venue="kalshi", instrument=instrument,
            series_ticker=series_ticker, event_ticker=event_ticker, market_ticker=market,
            yes_bid_dollars=message.get("yes_bid_dollars"), yes_ask_dollars=message.get("yes_ask_dollars"),
            last_price_dollars=message.get("price_dollars"), volume=message.get("volume_fp"),
            open_interest=message.get("open_interest_fp"), exchange_ts_ms=exchange_ts, received_ts_ms=received_ts_ms,
        )
    if event_type == "trade":
        trade_id = _text(message.get("trade_id"), "trade_id")
        side = _text(message.get("taker_side"), "taker_side")
        if side not in {"yes", "no"}:
            raise KalshiMessageError("taker_side must be yes or no")
        return KalshiTrade(
            event_id=f"kalshi:{market}:trade:{trade_id}", event_type="kalshi_trade", venue="kalshi", instrument=instrument,
            series_ticker=series_ticker, event_ticker=event_ticker, market_ticker=market, trade_id=trade_id,
            yes_price_dollars=_text(message.get("yes_price_dollars"), "yes_price_dollars"),
            count=_text(message.get("count_fp"), "count_fp"), taker_side=side, exchange_ts_ms=exchange_ts, received_ts_ms=received_ts_ms,
        )
    if event_type == "orderbook_snapshot":
        return KalshiOrderBookSnapshot(
            event_id=f"kalshi:{market}:book:{int(frame.get('seq', 0))}", event_type="kalshi_orderbook_snapshot", venue="kalshi", instrument=instrument,
            series_ticker=series_ticker, event_ticker=event_ticker, market_ticker=market, sequence=int(frame.get("seq", 0)),
            yes_bids=_levels(message.get("yes_dollars_fp"), "yes_dollars_fp"), no_bids=_levels(message.get("no_dollars_fp"), "no_dollars_fp"),
            exchange_ts_ms=None, received_ts_ms=received_ts_ms,
        )
    if event_type == "orderbook_delta":
        side = _text(message.get("side"), "side")
        if side not in {"yes", "no"}:
            raise KalshiMessageError("orderbook side must be yes or no")
        sequence = int(frame.get("seq", 0))
        return KalshiOrderBookSnapshot(
            event_id=f"kalshi:{market}:book:{sequence}", event_type="kalshi_orderbook_delta", venue="kalshi", instrument=instrument,
            series_ticker=series_ticker, event_ticker=event_ticker, market_ticker=market, sequence=sequence,
            yes_bids=(), no_bids=(), exchange_ts_ms=exchange_ts, received_ts_ms=received_ts_ms,
            delta_price_dollars=_text(message.get("price_dollars"), "price_dollars"), delta_fp=_text(message.get("delta_fp"), "delta_fp"), delta_side=side,
        )
    return None


class KalshiWebSocket:
    """Reconnectable transport; event discovery and state live above this layer."""

    def __init__(self, ws_url: str, api_key: str, private_key_pem: str) -> None:
        self._ws_url = ws_url
        self._api_key = api_key
        self._private_key = load_private_key(private_key_pem)
        self._message_id = 1

    def subscribe_message(self, channels: Sequence[str], market_tickers: Sequence[str]) -> bytes:
        return orjson.dumps({
            "id": self._next_id(), "cmd": "subscribe",
            "params": {"channels": list(channels), "market_tickers": list(market_tickers)},
        })

    def update_subscription_message(self, sid: int, action: str, market_tickers: Sequence[str]) -> bytes:
        if action not in {"add_markets", "delete_markets"}:
            raise ValueError("action must be add_markets or delete_markets")
        return orjson.dumps({
            "id": self._next_id(), "cmd": "update_subscription",
            "params": {"sids": [sid], "market_tickers": list(market_tickers), "action": action},
        })

    def _next_id(self) -> int:
        value = self._message_id
        self._message_id += 1
        return value

    async def run(
        self,
        market_tickers: Sequence[str],
        on_message: Callable[[dict[str, Any]], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        delay = 1.0
        while not stop_event.is_set():
            try:
                headers = build_auth_headers(self._api_key, self._private_key, path=WS_PATH)
                async with connect(
                    self._ws_url, additional_headers=headers, open_timeout=10,
                    close_timeout=5, ping_interval=20, max_size=4_194_304,
                    max_queue=256, compression=None,
                ) as websocket:
                    delay = 1.0
                    await websocket.send(self.subscribe_message(
                        ["ticker", "trade", "orderbook_delta"], market_tickers
                    ))
                    LOGGER.info("venue_connected", extra={"venue": "kalshi"})
                    async for raw in websocket:
                        frame = orjson.loads(raw)
                        if isinstance(frame, dict):
                            await on_message(frame)
                        if stop_event.is_set():
                            break
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.warning("venue_connection_lost", extra={"venue": "kalshi"}, exc_info=True)
                if not stop_event.is_set():
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=delay)
                    except TimeoutError:
                        pass
                delay = min(delay * 2, 30.0) + random.uniform(0, 0.25)
