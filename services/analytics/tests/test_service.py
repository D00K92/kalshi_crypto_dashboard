import pytest

from kalshi_crypto_analytics.schemas import (
    FeatureObservation,
    MarketMetadata,
    Spot,
    Ticker,
    VolatilitySnapshot,
)
from kalshi_crypto_analytics.service import AnalyticsService

NOW = 1_800_000_000_000
VOLS = {"1m": .2, "5m": .21, "15m": .22, "30m": .23, "1h": .24}


def ticker(market="KXBTCD-E-T100", event="KXBTCD-E", ts=NOW):
    return Ticker(market, event, "KXBTCD", ".4", ".5", ts)


class Data:
    def __init__(self, bootstrap=(), entries=()): self.bootstrap, self.entries, self.acked = list(bootstrap), list(entries), []
    async def ensure_group(self): pass
    async def bootstrap_tickers(self): return self.bootstrap
    async def consume_tickers(self): result, self.entries = self.entries, []; return result
    async def acknowledge(self, ids): self.acked += ids
    async def read_spot(self): return Spot(101, NOW)
    async def read_features(self): return FeatureObservation({"x": 1}, NOW)


class Meta:
    def __init__(self, values): self.values = values
    async def markets(self, series, now): return self.values


class Forecast:
    ready = True
    async def forecast(self, observation, now): return VolatilitySnapshot(VOLS, observation.event_timestamp_ms, now, {h: h for h in VOLS})


class Publisher:
    def __init__(self): self.prices, self.unavailable_reasons, self.active = [], [], None
    async def publish_volatility(self, snapshot): self.vol = snapshot
    async def publish_price(self, ticker, payload): self.prices.append(payload)
    async def unavailable(self, ticker, reason, now): self.unavailable_reasons.append((ticker, reason))
    async def expire_inactive(self, active, now): self.active = active
    async def ping(self): return True


async def test_restart_bootstrap_and_missed_pubsub_recovery_prices_market():
    item = ticker()
    data, out = Data(bootstrap=[item]), Publisher()
    metadata = {item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "open")}
    service = AnalyticsService(data, Meta(metadata), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert service.ready is True
    assert out.prices[0]["model_probability"] > .5


@pytest.mark.parametrize(("delta", "reason"), [(-60_001, "stale_ticker"), (2_001, "stale_ticker")])
async def test_stale_timestamp_and_future_skew_cleanup(delta, reason):
    item = ticker(ts=NOW + delta); out = Publisher()
    service = AnalyticsService(Data(bootstrap=[item]), Meta({item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "open")}), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert out.unavailable_reasons == [(item.market_ticker, reason)]


async def test_rollover_removes_old_and_prices_new_event():
    old, new = ticker("KXBTCD-OLD-T100", "KXBTCD-OLD"), ticker("KXBTCD-NEW-T100", "KXBTCD-NEW")
    out = Publisher(); data = Data(bootstrap=[old], entries=[("2-0", new)])
    metadata = {new.market_ticker: MarketMetadata(new.market_ticker, new.event_ticker, 100, NOW + 300_000, "open")}
    service = AnalyticsService(data, Meta(metadata), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert out.active == {new.market_ticker}
    assert (old.market_ticker, "missing_market_metadata") in out.unavailable_reasons
    assert data.acked == ["2-0"]


async def test_non_open_market_is_unavailable():
    item = ticker(); out = Publisher()
    metadata = {item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "closed")}
    service = AnalyticsService(Data(bootstrap=[item]), Meta(metadata), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert out.unavailable_reasons == [(item.market_ticker, "unsupported_contract")]
