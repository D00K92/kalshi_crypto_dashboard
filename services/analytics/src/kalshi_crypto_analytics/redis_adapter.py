from __future__ import annotations

import json
from typing import Any

from redis.exceptions import ResponseError

from .schemas import (
    FeatureObservation,
    PricingUnavailable,
    Spot,
    Ticker,
    UnavailableReason,
    VolatilitySnapshot,
)


def _decode(raw: Any) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise TypeError("payload must be an object")
    return value


def _field(fields: dict[Any, Any], name: str) -> Any:
    return fields.get(name) if name in fields else fields.get(name.encode())


def _ticker(fields: dict[Any, Any]) -> Ticker:
    payload = _decode(_field(fields, "payload"))
    if payload.get("event_type") != "kalshi_ticker":
        raise ValueError("not a Kalshi ticker")
    return Ticker(
        market_ticker=str(payload["market_ticker"]), event_ticker=str(payload["event_ticker"]),
        series_ticker=str(payload["series_ticker"]), yes_bid_dollars=payload.get("yes_bid_dollars"),
        yes_ask_dollars=payload.get("yes_ask_dollars"), exchange_ts_ms=int(payload["exchange_ts_ms"]),
    )


class RedisMarketData:
    def __init__(self, client: Any, *, stream: str = "stream:kalshi_tickers", group: str = "analytics-pricing-v1", consumer: str = "analytics-1", bootstrap_count: int = 1000, feature_version: str = "v2_10s", feature_key: str | None = None) -> None:
        self.client, self.stream, self.group, self.consumer = client, stream, group, consumer
        self.feature_version = feature_version
        self.feature_key = feature_key or f"market:features:{feature_version}:BTCUSD:latest"
        self.bootstrap_count = bootstrap_count

    async def ensure_group(self) -> None:
        try:
            await self.client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_spot(self) -> Spot:
        try:
            payload = _decode(await self.client.get("market:spot:BTCUSDT:latest"))
            return Spot(float(payload["price"]), int(payload["generated_ts_ms"]))
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise PricingUnavailable(UnavailableReason.STALE_SPOT) from exc

    async def read_features(self) -> FeatureObservation:
        try:
            raw = await self.client.get(self.feature_key)
            payload = _decode(raw)
            if payload.get("feature_set") != "market_features" or payload.get("feature_version") != self.feature_version:
                raise ValueError("unexpected feature schema")
            values = payload["values"]
            if not isinstance(values, dict):
                raise TypeError("feature values must be an object")
            event_timestamp_ms = int(payload["event_timestamp_ms"])
            available_timestamp_ms = int(payload.get("available_timestamp_ms", event_timestamp_ms))
            return FeatureObservation(values, event_timestamp_ms, available_timestamp_ms, "market_features", self.feature_version)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
            raise PricingUnavailable(UnavailableReason.STALE_FEATURES) from exc

    async def bootstrap_tickers(self) -> list[Ticker]:
        latest: dict[str, Ticker] = {}
        for _, fields in await self.client.xrevrange(self.stream, count=self.bootstrap_count):
            try:
                item = _ticker(fields)
            except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                continue
            latest.setdefault(item.market_ticker, item)
        return list(latest.values())

    async def consume_tickers(self) -> list[tuple[str, Ticker]]:
        entries: list[tuple[Any, dict[Any, Any]]] = []
        try:
            claimed = await self.client.xautoclaim(self.stream, self.group, self.consumer, 60_000, "0-0", count=100)
            if len(claimed) > 1:
                entries.extend(claimed[1])
        except (AttributeError, ResponseError):
            pass
        streams = await self.client.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=100, block=1000)
        for _, stream_entries in streams:
            entries.extend(stream_entries)
        decoded: list[tuple[str, Ticker]] = []
        for entry_id, fields in entries:
            try:
                decoded.append((entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id), _ticker(fields)))
            except (TypeError, ValueError, KeyError, json.JSONDecodeError):
                continue
        return decoded

    async def acknowledge(self, entry_ids: list[str]) -> None:
        if entry_ids:
            await self.client.xack(self.stream, self.group, *entry_ids)


class RedisPricingPublisher:
    def __init__(self, client: Any, *, ttl_seconds: int = 60, status_ttl_seconds: int = 60) -> None:
        self.client, self.ttl_seconds, self.status_ttl_seconds = client, ttl_seconds, status_ttl_seconds

    async def publish_volatility(self, snapshot: VolatilitySnapshot) -> None:
        await self.client.set("market:volatility:v2_10s:BTCUSD:latest", json.dumps(snapshot.payload(), separators=(",", ":")), ex=self.ttl_seconds)

    async def publish_price(self, market_ticker: str, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, separators=(",", ":"), allow_nan=False)
        pipe = self.client.pipeline(transaction=False)
        pipe.set(f"market:pricing:v1:{market_ticker}", encoded, ex=self.ttl_seconds)
        pipe.delete(f"market:pricing:v1:status:{market_ticker}")
        pipe.zadd("market:pricing:v1:active", {market_ticker: payload["generated_ts_ms"]})
        pipe.xadd("stream:pricing:v1", {"payload": encoded}, maxlen=5000, approximate=True)
        pipe.publish("pub:pricing:v1", encoded)
        await pipe.execute()

    async def unavailable(self, market_ticker: str, reason: str, now_ms: int) -> None:
        payload = json.dumps({"schema_version": 1, "status": "unavailable", "market_ticker": market_ticker,
                              "reason": reason, "generated_ts_ms": now_ms}, separators=(",", ":"))
        pipe = self.client.pipeline(transaction=False)
        pipe.delete(f"market:pricing:v1:{market_ticker}")
        pipe.zrem("market:pricing:v1:active", market_ticker)
        pipe.set(f"market:pricing:v1:status:{market_ticker}", payload, ex=self.status_ttl_seconds)
        await pipe.execute()

    async def expire_inactive(self, active_tickers: set[str], now_ms: int) -> None:
        cutoff = now_ms - self.ttl_seconds * 1000
        stale = await self.client.zrangebyscore("market:pricing:v1:active", "-inf", cutoff)
        pipe = self.client.pipeline(transaction=False)
        pipe.zremrangebyscore("market:pricing:v1:active", "-inf", cutoff)
        for ticker in stale:
            name = ticker.decode() if isinstance(ticker, bytes) else str(ticker)
            pipe.delete(f"market:pricing:v1:{name}")
        current = await self.client.zrange("market:pricing:v1:active", 0, -1)
        for ticker in current:
            name = ticker.decode() if isinstance(ticker, bytes) else str(ticker)
            if name not in active_tickers:
                pipe.zrem("market:pricing:v1:active", name)
                pipe.delete(f"market:pricing:v1:{name}")
        await pipe.execute()

    async def ping(self) -> bool:
        return bool(await self.client.ping())
