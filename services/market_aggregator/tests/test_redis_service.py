from __future__ import annotations

import orjson

from market_aggregator.redis_service import AggregatorService


class FakeRedis:
    def __init__(self) -> None:
        self.acks: list[tuple[object, ...]] = []

    async def xack(self, *args) -> int:
        self.acks.append(args)
        return len(args) - 2


async def test_process_entries_acknowledges_batch_in_one_round_trip() -> None:
    service = object.__new__(AggregatorService)
    service.client = FakeRedis()
    handled: list[tuple[dict, int]] = []

    async def handler(event: dict, published_ts_ms: int) -> None:
        handled.append((event, published_ts_ms))

    entries = [
        (b"1000-0", {b"payload": orjson.dumps({"event_id": "one"})}),
        (b"1001-0", {b"payload": orjson.dumps({"event_id": "two"})}),
    ]

    await service._process_entries("stream:ticks", "group", entries, handler)

    assert handled == [
        ({"event_id": "one"}, 1000),
        ({"event_id": "two"}, 1001),
    ]
    assert service.client.acks == [
        ("stream:ticks", "group", b"1000-0", b"1001-0")
    ]


async def test_process_entries_acknowledges_rejected_rows_with_batch() -> None:
    service = object.__new__(AggregatorService)
    service.client = FakeRedis()

    async def handler(event: dict, published_ts_ms: int) -> None:
        raise AssertionError("malformed entry must not reach handler")

    entries = [(b"1000-0", {})]

    await service._process_entries("stream:ticks", "group", entries, handler)

    assert service.client.acks == [("stream:ticks", "group", b"1000-0")]
