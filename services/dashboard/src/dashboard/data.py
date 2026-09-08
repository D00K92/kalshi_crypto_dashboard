from __future__ import annotations

from dataclasses import dataclass
import time
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
    ) -> None:
        self.client, self.prefix, self.instrument = client, prefix, instrument
        self.kalshi_ticker_stream = kalshi_ticker_stream
        self.kalshi_trade_stream = kalshi_trade_stream

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

    def read_kalshi_data(self, spot: Any = None) -> dict[str, Any]:
        """Read and window Kalshi data before sending it to the browser."""
        try:
            from dashboard.kalshi_contracts import contract_rows, select_contract_window

            try:
                spot_price = float(spot) if spot is not None else None
            except (TypeError, ValueError):
                spot_price = None
            rows = self._attach_analytics(contract_rows(
                self._stream_payloads(self.kalshi_ticker_stream, 600),
                self._stream_payloads(self.kalshi_trade_stream, 300),
            ))
            return {"contracts": select_contract_window(rows, spot_price), "spot": spot, "redis_ok": True, "redis_error": None}
        except redis.RedisError as exc:
            return {"contracts": [], "spot": spot, "redis_ok": False, "redis_error": type(exc).__name__}

    def _read_kalshi_contracts(self) -> list[dict[str, Any]]:
        from dashboard.kalshi_contracts import contract_rows

        return self._attach_analytics(contract_rows(
            self._stream_payloads(self.kalshi_ticker_stream, 600),
            self._stream_payloads(self.kalshi_trade_stream, 300),
        ))

    def _attach_analytics(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Join analytics-owned latest keys without changing producer schemas."""
        if not rows:
            return rows
        keys = [f"market:pricing:v1:{row['market_ticker']}" for row in rows]
        payloads = self.client.mget(*keys)
        now_ms = int(time.time() * 1000)
        for row, raw in zip(rows, payloads, strict=False):
            row.update({"model_value": "-", "edge_mid": "-", "buy_yes_edge": "-", "sell_yes_edge": "-"})
            price = decode(raw, {})
            generated = price.get("generated_ts_ms") if isinstance(price, dict) else None
            if (not isinstance(price, dict) or price.get("status") != "available"
                    or not isinstance(generated, (int, float)) or now_ms - generated > 60_000
                    or generated - now_ms > 2_000):
                continue
            row.update({
                "model_value": _cents_display(price.get("model_value_dollars")),
                "model_value_cents": price.get("model_value_cents"),
                "model_probability": price.get("model_probability"),
                "edge_mid": _signed_cents(price.get("edge_vs_mid_probability")),
                "buy_yes_edge": _signed_cents(price.get("buy_yes_edge_probability")),
                "sell_yes_edge": _signed_cents(price.get("sell_yes_edge_probability")),
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
