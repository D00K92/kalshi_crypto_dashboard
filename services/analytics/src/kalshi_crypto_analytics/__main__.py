from __future__ import annotations

import asyncio
import logging
import signal

import redis.asyncio as redis

from .config import Settings
from .forecast import ConfiguredForecastProvider, HttpForecastProvider, VertexGCSResolver
from .health import HealthServer
from .kalshi import CachedMetadataProvider, KalshiRestClient
from .redis_adapter import RedisMarketData, RedisPricingPublisher
from .service import AnalyticsService

LOGGER = logging.getLogger(__name__)


async def _run() -> None:
    settings = Settings.from_env()
    client = redis.Redis.from_url(settings.redis_url, decode_responses=False, health_check_interval=30)
    market_data = RedisMarketData(client, consumer=settings.consumer_name)
    publisher = RedisPricingPublisher(client)
    metadata = CachedMetadataProvider(
        KalshiRestClient(settings.kalshi_rest_url, settings.kalshi_api_key, settings.kalshi_private_key),
        refresh_ms=settings.metadata_refresh_ms,
    )
    if settings.forecast_provider == "http":
        forecasts = HttpForecastProvider(base_url=settings.model_serving_url,
                                         timeout_ms=settings.model_serving_timeout_ms,
                                         model_version=settings.model_version)
    else:
        forecasts = ConfiguredForecastProvider(settings.model_resources,
                                               VertexGCSResolver(project=settings.gcp_project, location=settings.gcp_region),
                                               model_version=settings.model_version)
    service = AnalyticsService(
        market_data, metadata, forecasts, publisher,
        spot_max_age_ms=settings.spot_max_age_ms, ticker_max_age_ms=settings.ticker_max_age_ms,
        feature_max_age_ms=settings.feature_max_age_ms, volatility_max_age_ms=settings.volatility_max_age_ms,
        future_skew_ms=settings.future_skew_ms,
    )
    health = HealthServer(settings.health_port, lambda: service.ready)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await health.start()
    try:
        try:
            await forecasts.load()
        except Exception:
            LOGGER.exception("forecast_provider_load_failed provider=%s", settings.forecast_provider)
        await service.run(stop)
    finally:
        await health.close()
        await client.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(_run())


if __name__ == "__main__":
    main()
