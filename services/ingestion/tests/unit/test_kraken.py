import orjson

from ingestion.adapters import kraken as kraken_module
from ingestion.adapters.kraken import KrakenBook, KrakenFeed, KrakenMessageError, parse_kraken_message, trade_from_message


def test_parse_kraken_trade() -> None:
    messages = parse_kraken_message(
        '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","price":100.5,"qty":0.2,"side":"buy","trade_id":42,"timestamp":"2024-01-01T00:00:00.000000Z"}]}',
        received_ts_ms=2,
    )
    trade = trade_from_message(messages[0])
    assert trade.event_id == "kraken:BTC/USD:trade:42"
    assert trade.price == "100.5"
    assert trade.taker_side == "buy"


def test_parse_kraken_ignores_subscription_ack() -> None:
    assert parse_kraken_message('{"channel":"book","type":"subscribe","success":true}') == []


def test_parse_kraken_book_frame_keeps_frame_type() -> None:
    messages = parse_kraken_message(
        '{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[],"asks":[]}]}',
        received_ts_ms=2,
    )
    assert messages[0]["type"] == "snapshot"


def test_kraken_book_snapshot_and_update() -> None:
    book = KrakenBook("BTC/USD")
    snapshot = book.apply(
        {"type": "snapshot", "timestamp": "2024-01-01T00:00:00.000000Z", "bids": [{"price": "99", "qty": "1"}], "asks": [{"price": "101", "qty": "2"}]},
        2,
    )
    assert snapshot and snapshot.bids[0].price == "99"
    updated = book.apply(
        {"type": "update", "bids": [{"price": "100", "qty": "3"}], "asks": [{"price": "101", "qty": "0"}, {"price": "102", "qty": "1"}]},
        3,
    )
    assert updated and updated.bids[0].price == "100"
    assert updated.asks[0].price == "102"
    assert updated.depth <= 15


def test_kraken_parser_rejects_malformed_book_levels() -> None:
    book = KrakenBook("BTC/USD")
    try:
        book.apply({"type": "snapshot", "bids": [{"price": "not-a-price", "qty": "1"}], "asks": []}, 2)
    except KrakenMessageError as exc:
        assert "price" in str(exc)
    else:
        raise AssertionError("malformed Kraken price was accepted")


class _FakeWebSocket:
    def __init__(self, frames: list[bytes]) -> None:
        self.frames = iter(frames)
        self.sent: list[dict[str, object]] = []

    async def send(self, payload: bytes) -> None:
        self.sent.append(orjson.loads(payload))

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        try:
            return next(self.frames)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


class _FakeConnections:
    def __init__(self, websocket: _FakeWebSocket) -> None:
        self.websocket = websocket
        self.yielded = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> _FakeWebSocket:
        if self.yielded:
            raise StopAsyncIteration
        self.yielded = True
        return self.websocket


class _RecordingPipeline:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def put(self, event: object) -> None:
        self.events.append(event)


async def test_crossed_book_resubscribes_book_without_interrupting_trade(monkeypatch) -> None:
    initial_book = orjson.dumps({
        "channel": "book", "type": "snapshot", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": "99", "qty": "1"}],
            "asks": [{"price": "101", "qty": "1"}],
        }],
    })
    crossed_book = orjson.dumps({
        "channel": "book", "type": "update", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": "102", "qty": "1"}],
        }],
    })
    stale_delta = orjson.dumps({
        "channel": "book", "type": "update", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": "150", "qty": "1"}],
        }],
    })
    recovered_book = orjson.dumps({
        "channel": "book", "type": "snapshot", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": "98", "qty": "1"}],
            "asks": [{"price": "102", "qty": "1"}],
        }],
    })
    trade = orjson.dumps({
        "channel": "trade", "type": "update", "data": [{
            "symbol": "BTC/USD", "price": "100.5", "qty": "0.2",
            "side": "buy", "trade_id": 42,
            "timestamp": "2024-01-01T00:00:00.000000Z",
        }],
    })
    websocket = _FakeWebSocket([initial_book, crossed_book, stale_delta, recovered_book, trade])
    monkeypatch.setattr(
        kraken_module,
        "connect",
        lambda *args, **kwargs: _FakeConnections(websocket),
    )
    pipeline = _RecordingPipeline()
    feed = KrakenFeed("wss://unused", "BTC/USD", pipeline)  # type: ignore[arg-type]

    await feed.run()

    assert [event.event_type for event in pipeline.events] == ["book_snapshot", "book_snapshot", "trade"]
    assert pipeline.events[1].bids[0].price == "98"  # type: ignore[attr-defined]
    assert [message["method"] for message in websocket.sent] == [
        "subscribe", "subscribe", "unsubscribe", "subscribe",
    ]
    assert feed.health.last_error is None
    assert feed.health.last_event_received_ts_ms is not None
