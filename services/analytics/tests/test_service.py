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
VOLS = {"5m": .21, "15m": .22, "30m": .23, "1h": .24}


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


class CountingForecast(Forecast):
    def __init__(self): self.calls = 0
    async def forecast(self, observation, now):
        self.calls += 1
        return await super().forecast(observation, now)


class Publisher:
    def __init__(self): self.prices, self.unavailable_reasons, self.active = [], [], None
    async def publish_volatility(self, snapshot): self.vol = snapshot
    async def publish_kalshi_iv(self, payload): self.kalshi_iv = payload
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


async def test_forecast_runs_once_per_feature_availability_timestamp():
    item = ticker()
    data, out, forecasts = Data(bootstrap=[item]), Publisher(), CountingForecast()
    metadata = {item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "open")}
    service = AnalyticsService(data, Meta(metadata), forecasts, out, clock_ms=lambda: NOW)
    await service.start()
    await service.cycle()
    await service.cycle()
    assert forecasts.calls == 1


async def test_feature_freshness_uses_completion_timestamp_not_event_start():
    item = ticker()
    data, out = Data(bootstrap=[item]), Publisher()

    async def read_features():
        return FeatureObservation({"x": 1}, NOW - 100_000, NOW)

    data.read_features = read_features
    metadata = {item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "open")}
    service = AnalyticsService(data, Meta(metadata), Forecast(), out, clock_ms=lambda: NOW, feature_max_age_ms=10)
    await service.start()
    await service.cycle()
    assert service.ready is True


@pytest.mark.parametrize("delta", [-60_001, 2_001])
async def test_stale_quote_still_publishes_fair_price_without_edges(delta):
    item = ticker(ts=NOW + delta); out = Publisher()
    service = AnalyticsService(Data(bootstrap=[item]), Meta({item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "open")}), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert out.unavailable_reasons == []
    assert out.prices[0]["model_value_dollars"] > .5
    assert out.prices[0]["time_to_expiry_minutes"] == pytest.approx(5)
    assert out.prices[0]["volatility_bracket"] == ["5m", "15m"]
    assert out.prices[0]["market_mid_probability"] is None


async def test_rollover_removes_old_and_prices_new_event():
    old, new = ticker("KXBTCD-OLD-T100", "KXBTCD-OLD"), ticker("KXBTCD-NEW-T100", "KXBTCD-NEW")
    out = Publisher(); data = Data(bootstrap=[old], entries=[("2-0", new)])
    metadata = {new.market_ticker: MarketMetadata(new.market_ticker, new.event_ticker, 100, NOW + 300_000, "open")}
    service = AnalyticsService(data, Meta(metadata), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert out.active == {new.market_ticker}
    assert (old.market_ticker, "missing_market_metadata") in out.unavailable_reasons
    assert data.acked == ["2-0"]


async def test_kalshi_iv_uses_seven_near_atm_contract_mids():
    items = [ticker(f"KXBTCD-E-T{strike}") for strike in range(98, 105)]
    items = [Ticker(item.market_ticker, item.event_ticker, item.series_ticker, ".45", ".55", item.exchange_ts_ms) for item in items]
    metadata = {
        item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, float(item.market_ticker.rsplit("T", 1)[1]), NOW + 300_000, "open")
        for item in items
    }
    out = Publisher()
    service = AnalyticsService(Data(bootstrap=items), Meta(metadata), Forecast(), out, clock_ms=lambda: NOW)

    await service.start(); await service.cycle()

    assert out.kalshi_iv["contracts_used"] == 7
    assert out.kalshi_iv["event_ticker"] == "KXBTCD-E"


async def test_future_contract_is_retained_until_it_enters_pricing_window():
    item = ticker()
    clock = [NOW]
    out = Publisher()
    metadata = {
        item.market_ticker: MarketMetadata(
            item.market_ticker, item.event_ticker, 100, NOW + 3_601_000, "open"
        )
    }
    service = AnalyticsService(
        Data(bootstrap=[item]), Meta(metadata), Forecast(), out,
        clock_ms=lambda: clock[0],
    )
    await service.start()

    await service.cycle()
    assert out.unavailable_reasons == [(item.market_ticker, "outside_supported_lifetime")]
    assert item.market_ticker in service.tickers

    clock[0] += 2_000
    await service.cycle()
    assert out.prices[-1]["time_to_expiry_seconds"] == pytest.approx(3_599)
    assert service.ready is True


async def test_non_open_market_is_unavailable():
    item = ticker(); out = Publisher()
    metadata = {item.market_ticker: MarketMetadata(item.market_ticker, item.event_ticker, 100, NOW + 300_000, "closed")}
    service = AnalyticsService(Data(bootstrap=[item]), Meta(metadata), Forecast(), out, clock_ms=lambda: NOW)
    await service.start(); await service.cycle()
    assert out.unavailable_reasons == [(item.market_ticker, "unsupported_contract")]
