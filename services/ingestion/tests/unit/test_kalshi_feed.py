import orjson
import pytest

from ingestion.kalshi_discovery import KalshiEventSet
from ingestion.kalshi_feed import CHANNELS, KalshiFeed
from ingestion.kalshi_state import KalshiRolloverState


class FakeDiscovery:
    def __init__(self, event_set: KalshiEventSet) -> None:
        self._event_set = event_set

    async def discover(self, series_ticker: str) -> KalshiEventSet:
        assert series_ticker == self._event_set.series_ticker
        return self._event_set


class FakeSettings:
    kalshi_series_ticker = "KXBTCD"


class FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, payload: bytes) -> None:
        self.sent.append(orjson.loads(payload))


def _feed(candidate: KalshiEventSet) -> KalshiFeed:
    feed = KalshiFeed.__new__(KalshiFeed)
    feed._settings = FakeSettings()
    feed._discovery = FakeDiscovery(candidate)
    feed._state = KalshiRolloverState(
        active=KalshiEventSet("KXBTCD", "old", ("old-1",), {}, ({},))
    )
    feed.last_error = None
    return feed


@pytest.mark.asyncio
async def test_rollover_waits_for_all_channel_sids() -> None:
    candidate = KalshiEventSet("KXBTCD", "new", ("new-1",), {}, ({},))
    feed = _feed(candidate)
    websocket = FakeWebSocket()

    next_id = await feed._check_rollover(
        websocket,
        {"ticker": 11, "trade": 12},
        message_context={},
        message_id=7,
    )

    assert next_id == 7
    assert websocket.sent == []
    assert feed._state.pending is None


@pytest.mark.asyncio
async def test_rollover_adds_markets_to_every_channel_sid() -> None:
    candidate = KalshiEventSet("KXBTCD", "new", ("new-1", "new-2"), {}, ({}, {}))
    feed = _feed(candidate)
    websocket = FakeWebSocket()
    channel_sids = dict(zip(CHANNELS, (11, 12, 13), strict=True))
    message_context = {}

    next_id = await feed._check_rollover(
        websocket,
        channel_sids,
        message_context=message_context,
        message_id=7,
    )

    assert next_id == 10
    assert message_context == {"new-1": candidate, "new-2": candidate}
    assert websocket.sent == [
        {
            "id": 7,
            "cmd": "update_subscription",
            "params": {
                "sids": [11],
                "market_tickers": ["new-1", "new-2"],
                "action": "add_markets",
            },
        },
        {
            "id": 8,
            "cmd": "update_subscription",
            "params": {
                "sids": [12],
                "market_tickers": ["new-1", "new-2"],
                "action": "add_markets",
            },
        },
        {
            "id": 9,
            "cmd": "update_subscription",
            "params": {
                "sids": [13],
                "market_tickers": ["new-1", "new-2"],
                "action": "add_markets",
            },
        },
    ]


@pytest.mark.asyncio
async def test_delete_markets_uses_every_channel_sid() -> None:
    feed = _feed(KalshiEventSet("KXBTCD", "new", ("new-1",), {}, ({},)))
    websocket = FakeWebSocket()
    channel_sids = dict(zip(CHANNELS, (11, 12, 13), strict=True))

    next_id = await feed._update_all_channels(
        websocket,
        channel_sids,
        "delete_markets",
        ("old-1", "old-2"),
        9,
    )

    assert next_id == 12
    assert [message["params"] for message in websocket.sent] == [
        {
        "sids": [11],
        "market_tickers": ["old-1", "old-2"],
        "action": "delete_markets",
        },
        {
        "sids": [12],
        "market_tickers": ["old-1", "old-2"],
        "action": "delete_markets",
        },
        {
        "sids": [13],
        "market_tickers": ["old-1", "old-2"],
        "action": "delete_markets",
        },
    ]
