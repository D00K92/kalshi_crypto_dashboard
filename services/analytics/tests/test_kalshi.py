from kalshi_crypto_analytics.kalshi import (
    CachedMetadataProvider,
    KalshiRestClient,
    parse_market,
)


def test_metadata_requires_explicit_above_strike_and_expiry():
    market = {"ticker": "KXBTCD-E-T70000", "strike_type": "greater", "floor_strike": 70000,
              "expiration_time": "2026-09-05T01:00:00Z", "status": "open"}
    parsed = parse_market(market, "KXBTCD-E")
    assert parsed and parsed.strike == 70000
    assert parse_market({**market, "strike_type": "less"}, "KXBTCD-E") is None
    assert parse_market({**market, "floor_strike": 70001}, "KXBTCD-E") is None
    assert parse_market({key: value for key, value in market.items() if key != "expiration_time"}, "KXBTCD-E") is None


class Client:
    def __init__(self): self.calls = 0
    async def event_markets(self, series):
        self.calls += 1
        return [{"event_ticker": "E", "markets": [{"ticker": "E-T100", "strike_type": "greater", "floor_strike": 100,
                                                       "expiration_time": "2026-09-05T01:00:00Z", "status": "open"}]}]


async def test_metadata_cache_refresh_and_rollover():
    client = Client(); provider = CachedMetadataProvider(client, refresh_ms=15_000)
    assert "E-T100" in await provider.markets("KXBTCD", 10_000)
    await provider.markets("KXBTCD", 20_000)
    await provider.markets("KXBTCD", 26_000)
    assert client.calls == 2


async def test_rest_client_falls_back_to_market_endpoint_when_events_are_not_nested(monkeypatch):
    client = KalshiRestClient("https://example.invalid", "key", "pem")

    def fake_get(path, params):
        if path.endswith("events"):
            return {"events": [{"event_ticker": "E"}]}
        return {"markets": [{"ticker": "E-T100"}]}

    monkeypatch.setattr(client, "_get", fake_get)
    events = await client.event_markets("KXBTCD")
    assert events[0]["markets"] == [{"ticker": "E-T100"}]


def test_metadata_prefers_expected_expiration_over_administrative_expiration():
    market = {"ticker": "KXBTCD-E-T70000", "strike_type": "greater", "floor_strike": 70000,
              "expected_expiration_time": "2026-09-07T03:05:00Z",
              "expiration_time": "2026-09-14T03:00:00Z", "status": "open"}
    parsed = parse_market(market, "KXBTCD-E")
    assert parsed and parsed.expiry_ts_ms == 1788750300000
