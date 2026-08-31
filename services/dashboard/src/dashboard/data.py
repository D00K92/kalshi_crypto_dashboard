from __future__ import annotations

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
                f"{self.prefix}:candles:{self.instrument}:5s",
                f"{self.prefix}:cvd:{self.instrument}:5s",
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

    def _read_kalshi_contracts(self) -> list[dict[str, Any]]:
        from dashboard.kalshi_contracts import contract_rows

        return contract_rows(
            self._stream_payloads(self.kalshi_ticker_stream, 600),
            self._stream_payloads(self.kalshi_trade_stream, 300),
        )

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
