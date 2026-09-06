from __future__ import annotations

import os
from dataclasses import dataclass

from .schemas import HORIZONS


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str
    kalshi_rest_url: str
    kalshi_api_key: str
    kalshi_private_key: str
    model_resources: dict[str, str]
    gcp_project: str
    gcp_region: str
    model_version: str
    consumer_name: str
    health_port: int
    spot_max_age_ms: int
    ticker_max_age_ms: int
    feature_max_age_ms: int
    volatility_max_age_ms: int
    future_skew_ms: int
    metadata_refresh_ms: int

    @classmethod
    def from_env(cls) -> Settings:
        redis_url = os.getenv("ANALYTICS_REDIS_URL") or f"redis://{os.getenv('REDIS_HOST', 'localhost')}:{_positive_int('REDIS_PORT', 6379)}/0"
        resources = {horizon: os.getenv(f"VOLATILITY_MODEL_{horizon.upper()}", "").strip() for horizon in HORIZONS}
        missing = [horizon for horizon, resource in resources.items() if not resource]
        if missing:
            raise ValueError(f"missing exact model resources: {', '.join(missing)}")
        return cls(redis_url, os.getenv("KALSHI_REST_URL", "https://external-api.kalshi.com"),
                   os.getenv("KALSHI_API_KEY", ""), os.getenv("KALSHI_PRIVATE_KEY", ""), resources,
                   os.environ["GCP_PROJECT_ID"], os.getenv("GCP_REGION", "asia-northeast3"),
                   os.getenv("VOLATILITY_MODEL_VERSION", "v1"),
                   os.getenv("CONSUMER_NAME", f"analytics-{os.getenv('HOSTNAME', 'local')}"),
                   _positive_int("HEALTH_PORT", 8080), _positive_int("SPOT_MAX_AGE_MS", 5_000),
                   _positive_int("TICKER_MAX_AGE_MS", 60_000), _positive_int("FEATURE_MAX_AGE_MS", 60_000),
                   _positive_int("VOLATILITY_MAX_AGE_MS", 60_000), _positive_int("ALLOWED_FUTURE_SKEW_MS", 2_000),
                   _positive_int("KALSHI_METADATA_REFRESH_MS", 15_000))
