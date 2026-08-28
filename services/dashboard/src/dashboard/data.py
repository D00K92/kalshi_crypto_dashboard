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


def decode(raw: bytes | str | None, fallback: Any) -> Any:
    if raw is None:
        return fallback
    try:
        return orjson.loads(raw)
    except (orjson.JSONDecodeError, TypeError):
        return fallback


class RedisReader:
    """Read aggregator latest-state keys; Pub/Sub is intentionally a later phase."""

    def __init__(self, client: Any, prefix: str = "market", instrument: str = "BTCUSDT") -> None:
        self.client, self.prefix, self.instrument = client, prefix, instrument

    def read(self) -> DashboardData:
        values = self.client.mget(
            f"{self.prefix}:book:{self.instrument}:latest",
            f"{self.prefix}:spot:{self.instrument}:latest",
            f"{self.prefix}:candles:{self.instrument}:5s",
            f"{self.prefix}:cvd:{self.instrument}:5s",
        )
        return DashboardData(
            book=decode(values[0], {"bids": [], "asks": [], "venues": [], "stale_venues": []}),
            spot=decode(values[1], {"price": None, "total_volume": "0", "stale_venues": []}),
            candles=decode(values[2], []),
            cvd=decode(values[3], []),
        )


def redis_client_from_env() -> redis.Redis:
    """Build a client for local Redis or a forwarded/private GCP endpoint."""
    import os

    url = os.getenv("REDIS_URL")
    if url:
        return redis.Redis.from_url(url, decode_responses=False, health_check_interval=30)
    return redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", "6379")), decode_responses=False, health_check_interval=30)
