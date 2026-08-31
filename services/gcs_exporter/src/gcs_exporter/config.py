"""Environment-backed exporter configuration."""

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


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


def _redis_url() -> str:
    explicit = os.getenv("GCS_EXPORTER_REDIS_URL")
    if explicit:
        return explicit
    host = os.getenv("REDIS_HOST", "localhost")
    port = _positive_int("REDIS_PORT", 6379)
    return f"redis://{host}:{port}/0"


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str
    stream_name: str
    consumer_group: str
    consumer_name: str
    bucket_name: str
    flush_size: int
    flush_interval_seconds: float
    read_count: int
    read_block_ms: int
    reclaim_interval_seconds: float
    reclaim_min_idle_ms: int
    shutdown_grace_seconds: int
    health_port: int
    log_level: str
    excluded_venues: frozenset[str] = frozenset()

    @classmethod
    def from_env(cls) -> "Settings":
        hostname = os.getenv("HOSTNAME", "local")
        return cls(
            redis_url=_redis_url(),
            stream_name=os.getenv("STREAM_NAME", "stream:ticks"),
            consumer_group=os.getenv("CONSUMER_GROUP", "gcs_archiver_group"),
            consumer_name=os.getenv("CONSUMER_NAME", f"pod-{hostname}"),
            bucket_name=os.getenv("GCS_BUCKET_NAME", "kalshi-crypto-tick-data"),
            flush_size=_positive_int("FLUSH_SIZE", 10_000),
            flush_interval_seconds=_positive_float("FLUSH_INTERVAL_SEC", 60.0),
            read_count=_positive_int("READ_COUNT", 500),
            read_block_ms=_positive_int("READ_BLOCK_MS", 1_000),
            reclaim_interval_seconds=_positive_float(
                "RECLAIM_INTERVAL_SEC", 30.0
            ),
            reclaim_min_idle_ms=_positive_int("RECLAIM_MIN_IDLE_MS", 120_000),
            shutdown_grace_seconds=_positive_int(
                "SHUTDOWN_GRACE_SECONDS", 30
            ),
            health_port=_positive_int("HEALTH_PORT", 8080),
            log_level=os.getenv("GCS_EXPORTER_LOG_LEVEL", "INFO").upper(),
            excluded_venues=frozenset(
                venue.strip().lower()
                for venue in os.getenv("GCS_EXCLUDED_VENUES", "bybit").split(",")
                if venue.strip()
            ),
        )
