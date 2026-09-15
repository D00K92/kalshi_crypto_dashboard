from __future__ import annotations

import os
from dataclasses import dataclass

from .generated_feature_contracts import CURRENT_CONTRACT_VERSION, REALIZED_VOL_VERSIONS


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str = "redis://127.0.0.1:6379/0"
    bars_stream: str = "stream:primitives:v1"
    feature_stream: str = f"stream:features:{CURRENT_CONTRACT_VERSION}"
    feature_key: str = f"market:features:{CURRENT_CONTRACT_VERSION}:BTCUSD:latest"
    state_key: str = f"market:features:BTCUSD:state:{CURRENT_CONTRACT_VERSION}"
    feature_version: str = CURRENT_CONTRACT_VERSION
    consumer_group: str = f"live-features-{CURRENT_CONTRACT_VERSION.replace('_', '-')}"
    consumer_name: str = "live-features-1"
    health_port: int = 8080
    ewma_decay: float = 0.96
    max_bar_age_ms: int = 120_000
    history_bars: int = 450
    replay_count: int = 5_000
    primitive_maxlen: int = 100_000
    pending_idle_ms: int = 60_000

    @classmethod
    def from_env(cls) -> "Settings":
        feature_version = os.getenv("FEATURE_VERSION", CURRENT_CONTRACT_VERSION)
        har = feature_version in REALIZED_VOL_VERSIONS
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
            bars_stream=os.getenv("BARS_STREAM", "stream:primitives:v1"),
            feature_stream=os.getenv("FEATURE_STREAM", f"stream:features:{feature_version}"),
            feature_key=os.getenv("FEATURE_KEY", f"market:features:{feature_version}:BTCUSD:latest"),
            state_key=os.getenv("FEATURE_STATE_KEY", f"market:features:BTCUSD:state:{feature_version}"),
            feature_version=feature_version,
            consumer_group=os.getenv("LIVE_FEATURE_GROUP", f"live-features-{feature_version.replace('_', '-')}"),
            consumer_name=os.getenv("HOSTNAME", "live-features-1"),
            health_port=int(os.getenv("HEALTH_PORT", "8080")),
            ewma_decay=float(os.getenv("EWMA_DECAY", "0.96")),
            max_bar_age_ms=int(os.getenv("MAX_BAR_AGE_MS", "120000")),
            history_bars=int(os.getenv("HISTORY_BARS", "1100" if har else "450")),
            replay_count=int(os.getenv("REPLAY_COUNT", "10000" if har else "5000")),
            primitive_maxlen=int(os.getenv("PRIMITIVE_STREAM_MAXLEN", "100000")),
            pending_idle_ms=int(os.getenv("LIVE_FEATURE_PENDING_IDLE_MS", "60000")),
        )
