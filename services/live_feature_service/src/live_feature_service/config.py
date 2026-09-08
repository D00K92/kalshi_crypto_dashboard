from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str = "redis://127.0.0.1:6379/0"
    bars_stream: str = "stream:bars:v1"
    feature_stream: str = "stream:features:v1"
    feature_key: str = "market:features:BTCUSD:latest"
    state_key: str = "market:features:BTCUSD:state:v1"
    consumer_group: str = "live-features-v1"
    consumer_name: str = "live-features-1"
    health_port: int = 8080
    ewma_decay: float = 0.96
    max_bar_age_ms: int = 120_000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
            bars_stream=os.getenv("BARS_STREAM", "stream:bars:v1"),
            feature_stream=os.getenv("FEATURE_STREAM", "stream:features:v1"),
            feature_key=os.getenv("FEATURE_KEY", "market:features:BTCUSD:latest"),
            state_key=os.getenv("FEATURE_STATE_KEY", "market:features:BTCUSD:state:v1"),
            consumer_group=os.getenv("LIVE_FEATURE_GROUP", "live-features-v1"),
            consumer_name=os.getenv("HOSTNAME", "live-features-1"),
            health_port=int(os.getenv("HEALTH_PORT", "8080")),
            ewma_decay=float(os.getenv("EWMA_DECAY", "0.96")),
            max_bar_age_ms=int(os.getenv("MAX_BAR_AGE_MS", "120000")),
        )
