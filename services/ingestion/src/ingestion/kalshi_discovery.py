"""Authenticated REST discovery for the active KXBTCD event."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ingestion.kalshi_auth import build_auth_headers


class KalshiDiscoveryError(RuntimeError):
    """Raised when the discovery response cannot produce a valid event set."""


@dataclass(frozen=True, slots=True)
class KalshiEventSet:
    series_ticker: str
    event_ticker: str
    market_tickers: tuple[str, ...]
    event: dict[str, Any]
    markets: tuple[dict[str, Any], ...]


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def choose_event(events: list[dict[str, Any]], *, now: datetime | None = None) -> dict[str, Any]:
    """Choose the nearest future hourly event, never a stale/closed event."""
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    for event in events:
        if not isinstance(event, dict) or not event.get("event_ticker"):
            continue
        if event.get("status") in {"closed", "settled", "determined", "finalized"}:
            continue
        when = _parse_time(event.get("strike_date")) or _parse_time(event.get("close_time"))
        if when is not None and when > current:
            candidates.append((when, event))
    if not candidates:
        raise KalshiDiscoveryError("no future KXBTCD event was discovered")
    # Prefer the next two-hour event when daily products coexist with hourly ones.
    near = [(when, event) for when, event in candidates if when <= current + timedelta(hours=2)]
    return min(near or candidates, key=lambda item: item[0])[1]


class KalshiRestDiscovery:
    def __init__(self, base_url: str, api_key: str, private_key: Any) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._private_key = private_key

    def _get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        query = urlencode(params)
        request = Request(
            f"{self._base_url}{path}?{query}",
            headers={"Content-Type": "application/json", **build_auth_headers(self._api_key, self._private_key, path=path)},
            method="GET",
        )
        try:
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read())
        except Exception as exc:
            raise KalshiDiscoveryError(f"Kalshi REST discovery failed for {path}") from exc
        if not isinstance(payload, dict):
            raise KalshiDiscoveryError(f"Kalshi REST response for {path} was not an object")
        return payload

    async def discover(self, series_ticker: str) -> KalshiEventSet:
        events_response = await asyncio.to_thread(
            self._get,
            "/trade-api/v2/events",
            {"series_ticker": series_ticker, "status": "open", "with_nested_markets": "true"},
        )
        events = events_response.get("events")
        if not isinstance(events, list):
            raise KalshiDiscoveryError("Kalshi events response did not contain an events array")
        event = choose_event(events)
        event_ticker = str(event["event_ticker"])
        markets = event.get("markets")
        if not isinstance(markets, list):
            markets_response = await asyncio.to_thread(
                self._get, "/trade-api/v2/markets", {"event_ticker": event_ticker, "status": "open"}
            )
            markets = markets_response.get("markets")
        if not isinstance(markets, list):
            raise KalshiDiscoveryError("Kalshi event did not contain a markets array")
        valid = tuple(m for m in markets if isinstance(m, dict) and m.get("ticker"))
        if not valid:
            raise KalshiDiscoveryError(f"Kalshi event {event_ticker} has no market tickers")
        return KalshiEventSet(series_ticker, event_ticker, tuple(str(m["ticker"]) for m in valid), event, valid)
