"""Environment-backed service configuration."""

from __future__ import annotations

from dataclasses import dataclass
import os


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _redis_url() -> str:
    explicit_url = os.getenv("INGESTION_REDIS_URL")
    if explicit_url:
        return explicit_url

    host = os.getenv("REDIS_HOST", "localhost")
    port = _positive_int("REDIS_PORT", 6379)
    return f"redis://{host}:{port}/0"


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str
    binance_ws_url: str
    queue_maxsize: int
    stream_maxlen: int
    shutdown_grace_seconds: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        symbol = os.getenv("BINANCE_SYMBOL", "btcusdt").lower()
        default_streams = f"{symbol}@trade/{symbol}@depth20@100ms"
        default_ws_url = (
            "wss://stream.binance.com:9443/stream?streams=" + default_streams
        )
        return cls(
            redis_url=_redis_url(),
            binance_ws_url=os.getenv("BINANCE_WS_URL", default_ws_url),
            queue_maxsize=_positive_int("INGESTION_QUEUE_MAXSIZE", 10_000),
            stream_maxlen=_positive_int("INGESTION_STREAM_MAXLEN", 1_000_000),
            shutdown_grace_seconds=_positive_int(
                "INGESTION_SHUTDOWN_GRACE_SECONDS", 10
            ),
            log_level=os.getenv("INGESTION_LOG_LEVEL", "INFO").upper(),
        )
