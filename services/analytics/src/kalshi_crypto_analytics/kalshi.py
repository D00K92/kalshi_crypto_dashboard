from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .schemas import MarketMetadata


def _timestamp_ms(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return int(datetime.fromisoformat(value).astimezone(UTC).timestamp() * 1000)
    except ValueError:
        return None


def _numeric(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def parse_market(market: dict[str, Any], event_ticker: str) -> MarketMetadata | None:
    ticker = market.get("ticker")
    if not isinstance(ticker, str) or not ticker:
        return None
    strike_type = str(market.get("strike_type", "")).lower()
    if strike_type not in {"greater", "greater_than", "above"}:
        return None
    strike = _numeric(market.get("floor_strike") or market.get("functional_strike") or market.get("strike"))
    suffix = re.search(r"-T(\d+(?:\.\d+)?)$", ticker)
    suffix_strike = float(suffix.group(1)) if suffix else None
    if strike is None:
        strike = suffix_strike
    elif suffix_strike is not None and strike != suffix_strike:
        return None
    # Kalshi's expiration_time may be a long-lived administrative deadline
    # (often days after settlement). Pricing must use the contract's actual
    # near-term resolution time when available.
    expiry = next((_timestamp_ms(market.get(field)) for field in
                   ("expected_expiration_time", "settlement_time", "determination_time",
                    "close_time", "expiration_time") if market.get(field)), None)
    if strike is None or expiry is None:
        return None
    return MarketMetadata(ticker, event_ticker, strike, expiry, str(market.get("status", "")),
                          market.get("rules_primary") or market.get("settlement_source"))


class KalshiRestClient:
    def __init__(self, base_url: str, api_key: str, private_key_pem: str) -> None:
        self.base_url, self.api_key = base_url.rstrip("/"), api_key
        self.private_key_pem = private_key_pem

    def _headers(self, method: str, path: str) -> dict[str, str]:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        timestamp = str(int(time.time() * 1000))
        key = serialization.load_pem_private_key(self.private_key_pem.encode(), password=None)
        signature = key.sign(f"{timestamp}{method}{path}".encode(), padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return {"KALSHI-ACCESS-KEY": self.api_key, "KALSHI-ACCESS-TIMESTAMP": timestamp,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode()}

    def _get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        request = Request(f"{self.base_url}{path}?{urlencode(params)}",
                          headers={"Content-Type": "application/json", **self._headers("GET", path)})
        with urlopen(request, timeout=10) as response:
            result = json.loads(response.read())
        if not isinstance(result, dict):
            raise TypeError("Kalshi response must be an object")
        return result

    async def event_markets(self, series_ticker: str) -> list[dict[str, Any]]:
        response = await asyncio.to_thread(self._get, "/trade-api/v2/events",
                                           {"series_ticker": series_ticker, "status": "open", "with_nested_markets": "true"})
        events = response.get("events")
        if not isinstance(events, list):
            raise TypeError("Kalshi events response has no events")
        normalized: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict) or not event.get("event_ticker"):
                continue
            if not isinstance(event.get("markets"), list):
                market_response = await asyncio.to_thread(
                    self._get,
                    "/trade-api/v2/markets",
                    {"event_ticker": str(event["event_ticker"]), "status": "open"},
                )
                event = {**event, "markets": market_response.get("markets")}
            normalized.append(event)
        return normalized


@dataclass(slots=True)
class CachedMetadataProvider:
    client: Any
    refresh_ms: int = 15_000
    _cached: dict[str, MarketMetadata] | None = None
    _cached_at_ms: int = 0

    async def markets(self, series_ticker: str, now_ms: int) -> dict[str, MarketMetadata]:
        if self._cached is not None and now_ms - self._cached_at_ms < self.refresh_ms:
            return self._cached
        parsed: dict[str, MarketMetadata] = {}
        for event in await self.client.event_markets(series_ticker):
            event_ticker = str(event.get("event_ticker", ""))
            markets = event.get("markets")
            if not isinstance(markets, list):
                continue
            for market in markets:
                metadata = parse_market(market, event_ticker) if isinstance(market, dict) else None
                if metadata is not None:
                    parsed[metadata.market_ticker] = metadata
        self._cached, self._cached_at_ms = parsed, now_ms
        return parsed
