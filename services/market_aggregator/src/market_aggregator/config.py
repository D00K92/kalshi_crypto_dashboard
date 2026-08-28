from __future__ import annotations

from dataclasses import dataclass
import os


def _int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _redis_url() -> str:
    explicit = os.getenv("MARKET_AGGREGATOR_REDIS_URL")
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
    consumer_name: str
    price_tick: str
    book_depth: int
    freshness_ms: int
    read_count: int
    read_block_ms: int
    output_prefix: str

    @classmethod
    def from_env(cls) -> "Settings":
        host = os.getenv("HOSTNAME", "local")
        return cls(
            redis_url=_redis_url(),
            book_stream=os.getenv("BOOK_STREAM", "stream:orderbook_snapshots"),
            trade_stream=os.getenv("TRADE_STREAM", "stream:ticks"),
            book_group=os.getenv("BOOK_CONSUMER_GROUP", "market_aggregator_books"),
            trade_group=os.getenv("TRADE_CONSUMER_GROUP", "market_aggregator_trades"),
            consumer_name=os.getenv("CONSUMER_NAME", f"aggregator-{host}"),
            price_tick=os.getenv("AGGREGATION_PRICE_TICK", "0.01"),
            book_depth=_int("AGGREGATION_BOOK_DEPTH", 10),
            freshness_ms=_int("AGGREGATION_FRESHNESS_MS", 5000),
            read_count=_int("AGGREGATOR_READ_COUNT", 200),
            read_block_ms=_int("AGGREGATOR_READ_BLOCK_MS", 1000),
            output_prefix=os.getenv("AGGREGATOR_OUTPUT_PREFIX", "market"),
        )
