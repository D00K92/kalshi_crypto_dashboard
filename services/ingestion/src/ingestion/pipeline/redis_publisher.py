"""Durable-first Redis publication for normalized market events."""

from __future__ import annotations

import logging
from collections.abc import Sequence

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
        await self.publish_many((event,))

    async def publish_many(self, events: Sequence[MarketEvent]) -> None:
        """Append a batch atomically at the transport layer, then fan out Pub/Sub."""
        if not events:
            return
        pipe = self._client.pipeline(transaction=False)
        payloads: list[tuple[MarketEvent, bytes]] = []
        for event in events:
            payload = event.to_json()
            payloads.append((event, payload))
            pipe.xadd(
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
        for event, payload in payloads:
            pipe.publish(event.pubsub_channel, payload)
        try:
            await pipe.execute()
        except (ConnectionError, TimeoutError):
            LOGGER.warning(
                "redis_publish_batch_failed",
                extra={"event_count": len(events)},
                exc_info=True,
            )
            raise

    async def close(self) -> None:
        await self._client.aclose()
