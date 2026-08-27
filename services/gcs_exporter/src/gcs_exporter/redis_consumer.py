"""Redis Streams consumer-group operations."""

from __future__ import annotations

from typing import Any

import redis.asyncio as redis
from redis.exceptions import ResponseError

from gcs_exporter.models import RawStreamEntry


class RedisStreamConsumer:
    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        group_name: str,
        consumer_name: str,
    ) -> None:
        self._client = redis.Redis.from_url(
            redis_url,
            decode_responses=False,
            health_check_interval=30,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name

    async def ready(self) -> None:
        await self._client.ping()

    async def ensure_group(self) -> None:
        try:
            await self._client.xgroup_create(
                self.stream_name, self.group_name, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read_new(self, count: int, block_ms: int) -> list[RawStreamEntry]:
        response = await self._client.xreadgroup(
            groupname=self.group_name,
            consumername=self.consumer_name,
            streams={self.stream_name: ">"},
            count=count,
            block=block_ms,
            noack=False,
        )
        return self._flatten(response)

    async def reclaim(
        self, min_idle_ms: int, count: int
    ) -> list[RawStreamEntry]:
        response = await self._client.xautoclaim(
            self.stream_name,
            self.group_name,
            self.consumer_name,
            min_idle_ms,
            start_id="0-0",
            count=count,
            justid=False,
        )
        messages = response[1] if len(response) >= 2 else []
        return self._entries(messages)

    async def ack(self, redis_ids: list[str]) -> int:
        if not redis_ids:
            return 0
        return int(
            await self._client.xack(
                self.stream_name, self.group_name, *redis_ids
            )
        )

    async def close(self) -> None:
        await self._client.aclose()

    @classmethod
    def _flatten(cls, response: Any) -> list[RawStreamEntry]:
        entries: list[RawStreamEntry] = []
        for _, messages in response or []:
            entries.extend(cls._entries(messages))
        return entries

    @staticmethod
    def _entries(messages: Any) -> list[RawStreamEntry]:
        result: list[RawStreamEntry] = []
        for redis_id, fields in messages or []:
            if isinstance(redis_id, bytes):
                redis_id = redis_id.decode("ascii")
            result.append(RawStreamEntry(redis_id=str(redis_id), fields=fields))
        return result
