from __future__ import annotations

from types import SimpleNamespace

import orjson

from aggregator.aggregation import MarketAggregator
from aggregator.redis_service import AggregatorService


class FakeRedis:
    def __init__(self) -> None:
        self.acks: list[tuple[object, ...]] = []
        self.pipelines: list[RecordingPipeline] = []

    def pipeline(self, **kwargs) -> "RecordingPipeline":
        pipeline = RecordingPipeline()
        self.pipelines.append(pipeline)
        return pipeline

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

    def xack(self, *args, **kwargs) -> None:
        self.operations.append(("xack", args))

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
    service.settings = SimpleNamespace(trade_stream="other")
    handled: list[tuple[dict, int]] = []

    async def handler(event: dict, published_ts_ms: int, pipeline) -> None:
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
    assert service.client.acks == []
    assert service.client.pipelines[0].operations[-1] == (
        "xack", ("stream:ticks", "group", b"1000-0", b"1001-0")
    )


async def test_process_entries_acknowledges_rejected_rows_with_batch(caplog) -> None:
    service = object.__new__(AggregatorService)
    service.client = FakeRedis()
    service.settings = SimpleNamespace(trade_stream="other")

    async def handler(event: dict, published_ts_ms: int) -> None:
        raise AssertionError("malformed entry must not reach handler")

    entries = [(b"1000-0", {})]

    await service._process_entries("stream:ticks", "group", entries, handler)

    assert service.client.acks == []
    assert service.client.pipelines[0].operations[-1] == (
        "xack", ("stream:ticks", "group", b"1000-0")
    )
    assert "event_rejected stream=stream:ticks redis_id=b'1000-0'" in caplog.text
    assert "error_type=ValueError error=missing payload" in caplog.text


async def test_trade_publishes_resampled_dashboard_candles_after_watermark() -> None:
    service = object.__new__(AggregatorService)
    service.client = TradeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(output_prefix="market", allowed_lateness_ms=5_000)
    service._last_finalized_primitive_bucket = None
    service._max_trade_event_ts_ms = None

    first = {"event_id": "one", "event_type": "trade", "venue": "binance", "instrument": "BTCUSDT", "price": "100", "quantity": "1", "taker_side": "buy", "exchange_ts_ms": 10_000, "received_ts_ms": 10_000}
    second = {**first, "event_id": "two", "price": "101", "exchange_ts_ms": 10_001, "received_ts_ms": 10_001}
    third = {**first, "event_id": "three", "price": "102", "exchange_ts_ms": 25_000, "received_ts_ms": 25_000}

    await service._handle_trade(first)
    await service._handle_trade(second)
    await service._handle_trade(third)

    candle_writes = [
        operation for pipeline in service.client.pipelines for operation in pipeline.operations
        if operation[0] == "set" and operation[1][0] == "market:candles:BTCUSDT:30s"
    ]
    assert len(candle_writes) == 1


async def test_trade_watermark_publishes_each_bucket_once_in_event_time_order() -> None:
    service = object.__new__(AggregatorService)
    service.client = TradeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(
        output_prefix="market",
        allowed_lateness_ms=5_000,
        primitive_stream="stream:primitives:v1",
        primitive_maxlen=100_000,
    )
    service._last_finalized_primitive_bucket = None
    service._max_trade_event_ts_ms = None

    def trade(event_id: str, timestamp: int, price: str = "100") -> dict:
        return {
            "event_id": event_id,
            "event_type": "trade",
            "venue": "binance",
            "instrument": "BTCUSDT",
            "price": price,
            "quantity": "1",
            "taker_side": "buy",
            "exchange_ts_ms": timestamp,
            "received_ts_ms": timestamp,
        }

    await service._handle_trade(trade("one", 10_000))
    await service._handle_trade(trade("two", 20_000))
    await service._handle_trade(trade("late-but-allowed", 19_000, "101"))
    await service._handle_trade(trade("advance", 25_000))
    await service._handle_trade(trade("too-late", 15_000, "999"))
    await service._handle_trade(trade("advance-again", 35_000))

    primitives = [
        orjson.loads(operation[1][1]["payload"])
        for pipeline in service.client.pipelines
        for operation in pipeline.operations
        if operation[0] == "xadd" and operation[1][0] == "stream:primitives:v1"
    ]
    assert [row["bucket_start_ts_ms"] for row in primitives] == [10_000, 20_000]
    assert primitives[0]["p_trade_mean"] == "100.5"
    assert all(row["p_trade_mean"] != "999" for row in primitives)


async def test_trade_checkpoint_output_and_ack_share_one_transaction() -> None:
    service = object.__new__(AggregatorService)
    service.client = FakeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(
        trade_stream="stream:ticks",
        output_prefix="market",
        allowed_lateness_ms=5_000,
        primitive_stream="stream:primitives:v1",
        primitive_maxlen=100_000,
    )
    service._last_finalized_primitive_bucket = None
    service._max_trade_event_ts_ms = None
    event = {
        "event_id": "one",
        "event_type": "trade",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "price": "100",
        "quantity": "1",
        "taker_side": "buy",
        "exchange_ts_ms": 10_000,
        "received_ts_ms": 10_000,
    }

    await service._process_entries(
        "stream:ticks",
        "group",
        [(b"10000-0", {b"payload": orjson.dumps(event)})],
        service._handle_trade,
    )

    operations = service.client.pipelines[0].operations
    keys = [operation[1][0] for operation in operations if operation[0] == "set"]
    assert "market:candle_state:BTCUSDT:10s" in keys
    assert "market:primitive_watermark:BTCUSDT:10s" in keys
    assert operations[-1] == ("xack", ("stream:ticks", "group", b"10000-0"))


async def test_stale_replayed_trade_is_acked_without_overwriting_live_state() -> None:
    service = object.__new__(AggregatorService)
    service.client = FakeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(
        trade_stream="stream:ticks",
        output_prefix="market",
        allowed_lateness_ms=5_000,
        replay_max_age_ms=5_000,
        primitive_stream="stream:primitives:v1",
        primitive_maxlen=100_000,
    )
    service._clock_ms = lambda: 100_000
    service._last_finalized_primitive_bucket = None
    service._max_trade_event_ts_ms = None
    event = {
        "event_id": "stale-replay",
        "event_type": "trade",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "price": "100",
        "quantity": "1",
        "taker_side": "buy",
        "exchange_ts_ms": 90_000,
        "received_ts_ms": 90_000,
    }

    await service._process_entries(
        "stream:ticks",
        "group",
        [(b"90000-0", {b"payload": orjson.dumps(event)})],
        service._handle_trade,
        replay=True,
    )

    assert service.state.trade_buckets == {}
    assert service.client.pipelines[0].operations == [
        ("xack", ("stream:ticks", "group", b"90000-0"))
    ]


async def test_stale_unread_trade_is_acked_without_blocking_live_processing() -> None:
    service = object.__new__(AggregatorService)
    service.client = FakeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(
        trade_stream="stream:ticks",
        output_prefix="market",
        allowed_lateness_ms=5_000,
        replay_max_age_ms=5_000,
        primitive_stream="stream:primitives:v1",
        primitive_maxlen=100_000,
    )
    service._clock_ms = lambda: 100_000
    service._last_finalized_primitive_bucket = None
    service._max_trade_event_ts_ms = None
    event = {
        "event_id": "delayed-live",
        "event_type": "trade",
        "venue": "bitstamp",
        "instrument": "BTCUSD",
        "price": "100",
        "quantity": "1",
        "taker_side": "buy",
        "exchange_ts_ms": 90_000,
        "received_ts_ms": 90_125,
    }

    await service._process_entries(
        "stream:ticks",
        "group",
        [(b"90000-0", {b"payload": orjson.dumps(event)})],
        service._handle_trade,
    )

    assert service.state.trade_buckets == {}
    assert service.client.pipelines[0].operations == [
        ("xack", ("stream:ticks", "group", b"90000-0"))
    ]


async def test_recent_redis_trade_still_updates_live_state() -> None:
    service = object.__new__(AggregatorService)
    service.client = TradeRedis()
    service.state = MarketAggregator()
    service.settings = SimpleNamespace(
        output_prefix="market",
        allowed_lateness_ms=5_000,
        replay_max_age_ms=5_000,
        primitive_stream="stream:primitives:v1",
        primitive_maxlen=100_000,
    )
    service._clock_ms = lambda: 100_000
    service._last_finalized_primitive_bucket = None
    service._max_trade_event_ts_ms = None
    event = {
        "event_id": "recent",
        "event_type": "trade",
        "venue": "binance",
        "instrument": "BTCUSDT",
        "price": "100",
        "quantity": "1",
        "taker_side": "buy",
        "exchange_ts_ms": 96_000,
        "received_ts_ms": 96_000,
    }

    assert await service._handle_trade(event, published_ts_ms=96_000) is True

    assert 90_000 in service.state.trade_buckets
    assert any(
        operation[0] == "set" and operation[1][0] == "market:spot:BTCUSDT:latest"
        for operation in service.client.pipelines[0].operations
    )
