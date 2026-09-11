"""Durable-first Redis publication for normalized market events."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

import redis.asyncio as redis
from redis.exceptions import ConnectionError, TimeoutError

from ingestion.models import MarketEvent


LOGGER = logging.getLogger(__name__)


class RedisPublisher:
    def __init__(self, redis_url: str, stream_maxlen: int, *,
                 retry_limit: int | None = None,
                 retry_base_seconds: float = 0.1,
                 retry_max_seconds: float = 2.0,
                 publisher_name: str = "critical") -> None:
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        self._stream_maxlen = stream_maxlen
        self._retry_limit = retry_limit
        self._retry_base_seconds = retry_base_seconds
        self._retry_max_seconds = retry_max_seconds
        self._publisher_name = publisher_name

    async def ready(self) -> None:
        await self._client.ping()

    async def publish(self, event: MarketEvent) -> None:
        await self.publish_many((event,))

    async def publish_many(self, events: Sequence[MarketEvent]) -> None:
        """Append a batch atomically at the transport layer, then fan out Pub/Sub."""
        if not events:
            return
        payloads: list[tuple[MarketEvent, bytes]] = []
        for event in events:
            payload = event.to_json()
            payloads.append((event, payload))

        failures = 0
        retry_delay = self._retry_base_seconds
        while True:
            # A timed-out pipeline has an ambiguous result and must never be
            # reused. Rebuild it so redis-py can obtain a healthy connection.
            pipe = self._client.pipeline(transaction=False)
            for event, payload in payloads:
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
                failures += 1
                if failures == 1:
                    LOGGER.warning(
                        "redis_publish_retrying",
                        extra={
                            "publisher": self._publisher_name,
                            "event_count": len(events),
                        },
                        exc_info=True,
                    )
                if self._retry_limit is not None and failures >= self._retry_limit:
                    LOGGER.error(
                        "redis_publish_retry_exhausted",
                        extra={
                            "publisher": self._publisher_name,
                            "event_count": len(events),
                            "attempts": failures,
                        },
                    )
                    raise
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self._retry_max_seconds)
                continue

            if failures:
                LOGGER.info(
                    "redis_publish_recovered",
                    extra={
                        "publisher": self._publisher_name,
                        "event_count": len(events),
                        "attempts": failures + 1,
                    },
                )
            return

    async def close(self) -> None:
        await self._client.aclose()
