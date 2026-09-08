from __future__ import annotations

from types import SimpleNamespace

import orjson

from aggregator.aggregation import MarketAggregator
from aggregator.redis_service import AggregatorService


class FakeRedis:
    def __init__(self) -> None:
        self.acks: list[tuple[object, ...]] = []

    async def xack(self, *args) -> int:
        self.acks.append(args)
        return len(args) - 2


class RecordingPipeline:
    def __init__(self) -> None:
        self.operations: list[tuple[str, tuple[object, ...]]] = []

    def set(self, *args, **kwargs) -> None:
        self.operations.append(("set", args))

    def xadd(self, *args, **kwargs) -> None:
        self.operations.append(("xadd", args))

    def publish(self, *args, **kwargs) -> None:
        self.operations.append(("publish", args))

    async def execute(self) -> None:
        return None


class TradeRedis:
    def __init__(self) -> None:
        self.pipelines: list[RecordingPipeline] = []

    def pipeline(self, **kwargs) -> RecordingPipeline:
        pipeline = RecordingPipeline()
        self.pipelines.append(pipeline)
        return pipeline


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


async def test_trade_publishes_candle_state_once_per_ten_second_bucket() -> None:
    service = object.__new__(AggregatorService)
    service.client = TradeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(output_prefix="market")
    service._last_candle_publish_bucket = None

    first = {"event_id": "one", "event_type": "trade", "venue": "binance", "instrument": "BTCUSDT", "price": "100", "quantity": "1", "taker_side": "buy", "exchange_ts_ms": 10_000, "received_ts_ms": 10_000}
    second = {**first, "event_id": "two", "price": "101", "exchange_ts_ms": 10_001, "received_ts_ms": 10_001}
    third = {**first, "event_id": "three", "price": "102", "exchange_ts_ms": 20_000, "received_ts_ms": 20_000}

    await service._handle_trade(first)
    await service._handle_trade(second)
    await service._handle_trade(third)

    state_writes = [
        operation for pipeline in service.client.pipelines for operation in pipeline.operations
        if operation[0] == "set" and operation[1][0] == "market:candle_state:BTCUSDT:10s"
    ]
    assert len(state_writes) == 2
