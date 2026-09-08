from __future__ import annotations

from dataclasses import dataclass
import os


def _int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _redis_url() -> str:
    # Keep the old variable as a rollout-safe fallback for existing secrets and
    # manifests. New deployments should set AGGREGATOR_REDIS_URL.
    explicit = os.getenv("AGGREGATOR_REDIS_URL") or os.getenv("MARKET_AGGREGATOR_REDIS_URL")
    if explicit:
        return explicit
    return f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{_int('REDIS_PORT', 6379)}/0"


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str
    book_stream: str
    trade_stream: str
    book_group: str
    trade_group: str
    group_start_id: str
    consumer_name: str
    price_tick: str | None
    book_depth: int
    freshness_ms: int
    read_count: int
    read_block_ms: int
    output_prefix: str
    bars_stream: str
    bars_maxlen: int
    orderbook_stream: str
    orderbook_maxlen: int
    bar_frequencies: tuple[tuple[str, int], ...]
    history_ms: int
    trade_freshness_ms: int
    health_port: int
    aggregation_venues: tuple[str, ...]
    taker_fees: tuple[tuple[str, str], ...]

    @classmethod
    def from_env(cls) -> "Settings":
        host = os.getenv("HOSTNAME", "local")
        configured_tick = os.getenv("AGGREGATION_PRICE_TICK")
        price_tick = None if not configured_tick or configured_tick.lower() == "auto" else configured_tick
        fees = os.getenv(
            "AGGREGATION_TAKER_FEES",
            "binance=0.001,bitstamp=0.004,crypto.com=0.005,gemini=0.012,coinbase=0.006,kraken=0.004",
        )
        return cls(
            redis_url=_redis_url(),
            book_stream=os.getenv("BOOK_STREAM", "stream:orderbook_snapshots"),
            trade_stream=os.getenv("TRADE_STREAM", "stream:ticks"),
            book_group=os.getenv("BOOK_CONSUMER_GROUP", "market_aggregator_books"),
            trade_group=os.getenv("TRADE_CONSUMER_GROUP", "market_aggregator_trades"),
            group_start_id=os.getenv("AGGREGATOR_GROUP_START_ID", "0"),
            consumer_name=os.getenv("CONSUMER_NAME", f"aggregator-{host}"),
            # Infer the finest common price precision unless explicitly set.
            price_tick=price_tick,
            book_depth=_int("AGGREGATION_BOOK_DEPTH", 10),
            freshness_ms=_int("AGGREGATION_FRESHNESS_MS", 500),
            read_count=_int("AGGREGATOR_READ_COUNT", 200),
            read_block_ms=_int("AGGREGATOR_READ_BLOCK_MS", 1000),
            output_prefix=os.getenv("AGGREGATOR_OUTPUT_PREFIX", "market"),
            bars_stream=os.getenv("AGGREGATED_BARS_STREAM", "stream:bars:v1"),
            bars_maxlen=_int("AGGREGATED_BARS_MAXLEN", 50_000),
            orderbook_stream=os.getenv("AGGREGATED_ORDERBOOK_STREAM", "stream:orderbook:v1"),
            orderbook_maxlen=_int("AGGREGATED_ORDERBOOK_MAXLEN", 10_000),
            bar_frequencies=tuple(
                (name, _int(f"BAR_{name.upper()}_MS", milliseconds))
                for name, milliseconds in (("1m", 60_000), ("5m", 300_000), ("10m", 600_000), ("15m", 900_000), ("30m", 1_800_000), ("1h", 3_600_000))
            ),
            history_ms=_int("AGGREGATION_HISTORY_MS", 2 * 60 * 60 * 1000),
            trade_freshness_ms=_int("FEATURE_TRADE_FRESHNESS_MS", 60_000),
            health_port=_int("HEALTH_PORT", 8080),
            aggregation_venues=tuple(
                venue.strip().lower()
                for venue in os.getenv(
                    "AGGREGATION_VENUES",
                    "binance,bitstamp,crypto.com,gemini,coinbase,kraken",
                ).split(",")
                if venue.strip()
            ),
            taker_fees=tuple(
                (venue.strip().lower(), rate.strip())
                for item in fees.split(",")
                if "=" in item
                for venue, rate in [item.split("=", 1)]
                if venue.strip() and rate.strip()
            ),
        )
