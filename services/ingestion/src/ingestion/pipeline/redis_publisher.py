"""Durable-first Redis publication for normalized market events."""

from __future__ import annotations

import logging

import redis.asyncio as redis
from redis.exceptions import ConnectionError, TimeoutError

from ingestion.models import MarketEvent


LOGGER = logging.getLogger(__name__)


class RedisPublisher:
    def __init__(self, redis_url: str, stream_maxlen: int) -> None:
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        self._stream_maxlen = stream_maxlen

    async def ready(self) -> None:
        await self._client.ping()

    async def publish(self, event: MarketEvent) -> None:
        payload = event.to_json()
        await self._client.xadd(
            event.stream_name,
            {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "venue": event.venue,
                "instrument": event.instrument,
                "payload": payload,
            },
            maxlen=self._stream_maxlen,
            approximate=True,
        )
        try:
            await self._client.publish(event.pubsub_channel, payload)
        except (ConnectionError, TimeoutError):
            LOGGER.warning(
                "pubsub_publish_failed",
                extra={"event_id": event.event_id, "channel": event.pubsub_channel},
                exc_info=True,
            )

    async def close(self) -> None:
        await self._client.aclose()
