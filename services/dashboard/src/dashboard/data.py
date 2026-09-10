from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import orjson
import redis


@dataclass(frozen=True)
class DashboardData:
    book: dict[str, Any]
    spot: dict[str, Any]
    candles: list[dict[str, Any]]
    cvd: list[dict[str, Any]]
    kalshi_contracts: list[dict[str, Any]]
    redis_ok: bool = True
    redis_error: str | None = None


@dataclass
class KalshiStreamCache:
    """Decoded stream windows shared by the monitor and slower contract table."""

    stream_ids: tuple[str | None, str | None, str | None]
    tickers: list[dict[str, Any]]
    trades: list[dict[str, Any]]
    orderbooks: list[dict[str, Any]]
    checked_at_ms: int


def decode(raw: bytes | str | None, fallback: Any) -> Any:
    if raw is None:
        return fallback
    try:
        return orjson.loads(raw)
    except (orjson.JSONDecodeError, TypeError):
        return fallback


class RedisReader:
    """Read aggregator latest-state keys; Pub/Sub is intentionally a later phase."""

    def __init__(
        self,
        client: Any,
        prefix: str = "market",
        instrument: str = "BTCUSDT",
        kalshi_ticker_stream: str = "stream:kalshi_tickers",
        kalshi_trade_stream: str = "stream:kalshi_trades",
        kalshi_orderbook_stream: str = "stream:kalshi_orderbook",
        kalshi_cache_ttl_ms: int = 500,
    ) -> None:
        self.client, self.prefix, self.instrument = client, prefix, instrument
        self.kalshi_ticker_stream = kalshi_ticker_stream
        self.kalshi_trade_stream = kalshi_trade_stream
        self.kalshi_orderbook_stream = kalshi_orderbook_stream
        self.kalshi_cache_ttl_ms = kalshi_cache_ttl_ms
        self._kalshi_cache: KalshiStreamCache | None = None

    def read(self) -> DashboardData:
        try:
            values = self.client.mget(
                f"{self.prefix}:book:{self.instrument}:latest",
                f"{self.prefix}:spot:{self.instrument}:latest",
                f"{self.prefix}:candles:{self.instrument}:30s",
                f"{self.prefix}:cvd:{self.instrument}:30s",
            )
            kalshi_contracts = self._read_kalshi_contracts()
        except redis.RedisError as exc:
            return DashboardData(
                book={"bids": [], "asks": [], "venues": [], "stale_venues": []},
                spot={"price": None, "total_volume": "0", "stale_venues": []},
                candles=[],
                cvd=[],
                kalshi_contracts=[],
                redis_ok=False,
                redis_error=type(exc).__name__,
            )
        return DashboardData(
            book=decode(values[0], {"bids": [], "asks": [], "venues": [], "stale_venues": []}),
            spot=decode(values[1], {"price": None, "total_volume": "0", "stale_venues": []}),
            candles=decode(values[2], []),
            cvd=decode(values[3], []),
            kalshi_contracts=kalshi_contracts,
        )

    def read_market_data(self) -> dict[str, Any]:
        """Read only the latest-state keys used by market panels."""
        try:
            values = self.client.mget(
                f"{self.prefix}:book:{self.instrument}:latest",
                f"{self.prefix}:spot:{self.instrument}:latest",
                f"{self.prefix}:candles:{self.instrument}:30s",
            )
        except redis.RedisError as exc:
            return {
                "book": {"bids": [], "asks": [], "venues": [], "stale_venues": []},
                "spot": {"price": None},
                "candles": [],
                "redis_ok": False,
                "redis_error": type(exc).__name__,
            }
        return {
            "book": decode(values[0], {"bids": [], "asks": [], "venues": [], "stale_venues": []}),
            "spot": decode(values[1], {"price": None}),
            "candles": decode(values[2], []),
            "redis_ok": True,
            "redis_error": None,
        }

    def read_fast_market_data(self) -> dict[str, Any]:
        """Read the high-frequency spot key used by dashboard panels."""
        try:
            values = self.client.mget(f"{self.prefix}:spot:{self.instrument}:latest")
        except redis.RedisError as exc:
            return {
                "spot": {"price": None},
                "redis_ok": False,
                "redis_error": type(exc).__name__,
            }
        return {
            "spot": decode(values[0], {"price": None}),
            "redis_ok": True,
            "redis_error": None,
        }

    def read_candle_data(self) -> dict[str, Any]:
        """Read the candle snapshot at its lower refresh frequency."""
        try:
            raw = self.client.get(f"{self.prefix}:candles:{self.instrument}:30s")
        except redis.RedisError as exc:
            return {"candles": [], "redis_ok": False, "redis_error": type(exc).__name__}
        return {"candles": decode(raw, []), "redis_ok": True, "redis_error": None}

    def read_volatility_data(self) -> dict[str, Any]:
        """Read the analytics-owned four-horizon volatility snapshot."""
        try:
            raw = self.client.get("market:volatility:v2_10s:BTCUSD:latest")
        except redis.RedisError as exc:
            return {"annualized_volatility": {}, "redis_ok": False, "redis_error": type(exc).__name__}
        payload = decode(raw, {})
        if not isinstance(payload, dict):
            payload = {}
        volatility = payload.get("annualized_volatility")
        payload["annualized_volatility"] = volatility if isinstance(volatility, dict) else {}
        payload["redis_ok"] = True
        payload["redis_error"] = None
        return payload

    def read_kalshi_data(self, spot: Any = None) -> dict[str, Any]:
        """Read and window Kalshi data before sending it to the browser."""
        try:
            from dashboard.kalshi_contracts import select_contract_window

            try:
                spot_price = float(spot) if spot is not None else None
            except (TypeError, ValueError):
                spot_price = None
            rows = select_contract_window(self._attach_analytics(self._kalshi_contract_rows()), spot_price)
            waiting_for_hourly_contract = bool(rows) and all(
                row.get("pricing_reason") == "outside_supported_lifetime" for row in rows
            )
            return {
                "contracts": rows,
                "spot": spot,
                "waiting_for_hourly_contract": waiting_for_hourly_contract,
                "redis_ok": True,
                "redis_error": None,
            }
        except redis.RedisError as exc:
            return {
                "contracts": [], "spot": spot, "waiting_for_hourly_contract": False,
                "redis_ok": False, "redis_error": type(exc).__name__,
            }

    def _read_kalshi_contracts(self) -> list[dict[str, Any]]:
        return self._attach_analytics(self._kalshi_contract_rows())

    def _kalshi_contract_rows(self) -> list[dict[str, Any]]:
        """Reuse decoded stream windows until their high-water marks advance."""
        from dashboard.kalshi_contracts import contract_rows

        now_ms = int(time.time() * 1000)
        cache = self._kalshi_cache
        if cache is None or now_ms - cache.checked_at_ms >= self.kalshi_cache_ttl_ms:
            stream_ids = tuple(self._latest_stream_id(stream) for stream in (
                self.kalshi_ticker_stream, self.kalshi_trade_stream, self.kalshi_orderbook_stream,
            ))
            if cache is None or cache.stream_ids != stream_ids:
                cache = KalshiStreamCache(
                    stream_ids=stream_ids,
                    tickers=self._stream_payloads(self.kalshi_ticker_stream, 600),
                    trades=self._stream_payloads(self.kalshi_trade_stream, 300),
                    orderbooks=self._stream_payloads(self.kalshi_orderbook_stream, 600),
                    checked_at_ms=now_ms,
                )
                self._kalshi_cache = cache
            else:
                cache.checked_at_ms = now_ms
        assert cache is not None
        # Rebuild rows from cached payloads so age labels continue advancing.
        return contract_rows(cache.tickers, cache.trades, cache.orderbooks, now_ms=now_ms)

    def _latest_stream_id(self, stream: str) -> str | None:
        entries = self.client.xrevrange(stream, count=1)
        if not entries:
            return None
        entry_id = entries[0][0]
        return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)

    def _attach_analytics(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Join analytics-owned latest keys without changing producer schemas."""
        if not rows:
            return rows
        keys = [f"market:pricing:v1:{row['market_ticker']}" for row in rows]
        payloads = self.client.mget(*keys)
        status_payloads = self.client.mget(*[
            f"market:pricing:v1:status:{row['market_ticker']}" for row in rows
        ])
        now_ms = int(time.time() * 1000)
        for row, raw, status_raw in zip(rows, payloads, status_payloads, strict=False):
            row.update({
                "model_value": "-", "model_vol": "-", "tau": "-",
                "edge_mid": "-", "pricing_reason": None,
            })
            price = decode(raw, {})
            status = decode(status_raw, {})
            if isinstance(status, dict) and isinstance(status.get("reason"), str):
                row["pricing_reason"] = status["reason"]
            generated = price.get("generated_ts_ms") if isinstance(price, dict) else None
            if (not isinstance(price, dict) or price.get("status") != "available"
                    or not isinstance(generated, (int, float)) or now_ms - generated > 60_000
                    or generated - now_ms > 2_000):
                continue
            model_probability = _float(price.get("model_probability"))
            bid_cents = _float(row.get("bid_value"))
            ask_cents = _float(row.get("ask_value"))
            if model_probability is not None and bid_cents is not None and ask_cents is not None:
                edge_mid = ((bid_cents + ask_cents) / 200) - model_probability
            else:
                published_edge = _float(price.get("edge_vs_mid_probability"))
                edge_mid = -published_edge if published_edge is not None else None
            row.update({
                "model_value": _cents_display(price.get("model_value_dollars")),
                "model_value_cents": price.get("model_value_cents"),
                "model_probability": model_probability,
                "model_vol": _percent_display(price.get("annualized_volatility")),
                "annualized_volatility": price.get("annualized_volatility"),
                "tau": _minutes_display(price.get("time_to_expiry_minutes")),
                "time_to_expiry_minutes": price.get("time_to_expiry_minutes"),
                "edge_mid": _signed_cents(edge_mid),
            })
        return rows

    def _stream_payloads(self, stream: str, count: int) -> list[dict[str, Any]]:
        entries = self.client.xrevrange(stream, count=count)
        payloads: list[dict[str, Any]] = []
        for _, fields in entries:
            if isinstance(fields, dict):
                payload = decode(fields.get(b"payload") or fields.get("payload"), None)
                if isinstance(payload, dict):
                    payloads.append(payload)
        return payloads


def redis_client_from_env() -> redis.Redis:
    """Build a client for local Redis or a forwarded/private GCP endpoint."""
    import os

    url = os.getenv("REDIS_URL")
    if url:
        return redis.Redis.from_url(url, decode_responses=False, health_check_interval=30)
    return redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=False, health_check_interval=30)


def _cents_display(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed * 100:.1f}¢"


def _signed_cents(value: Any) -> str:
    try:
        parsed = float(value) * 100
    except (TypeError, ValueError):
        return "-"
    return f"{parsed:+.1f}¢"


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percent_display(value: Any) -> str:
    try:
        parsed = float(value) * 100
    except (TypeError, ValueError):
        return "-"
    return f"{parsed:.1f}%"


def _minutes_display(value: Any) -> str:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "-"
    return f"{parsed:.1f}m"
