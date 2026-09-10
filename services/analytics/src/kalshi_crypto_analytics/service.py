from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import replace

from .core import (
    DISTRIBUTION_MODEL,
    INTERPOLATION_METHOD,
    gaussian_probability,
    interpolate_volatility,
    quote_edges,
    validate_fresh,
    validate_term_structure,
)
from .ports import (
    ForecastProvider,
    MarketDataSource,
    MetadataProvider,
    PricingPublisher,
)
from .schemas import (
    MarketMetadata,
    PricingResult,
    PricingUnavailable,
    Spot,
    Ticker,
    UnavailableReason,
    VolatilitySnapshot,
)

LOGGER = logging.getLogger(__name__)
OPEN_STATUSES = {"open", "active"}


class AnalyticsService:
    def __init__(self, market_data: MarketDataSource, metadata: MetadataProvider, forecasts: ForecastProvider, publisher: PricingPublisher, *, clock_ms: Callable[[], int] | None = None,
                 spot_max_age_ms: int = 5_000, ticker_max_age_ms: int = 60_000, feature_max_age_ms: int = 60_000,
                 volatility_max_age_ms: int = 60_000, future_skew_ms: int = 2_000) -> None:
        self.market_data, self.metadata, self.forecasts, self.publisher = market_data, metadata, forecasts, publisher
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.spot_max_age_ms, self.ticker_max_age_ms = spot_max_age_ms, ticker_max_age_ms
        self.feature_max_age_ms, self.volatility_max_age_ms, self.future_skew_ms = feature_max_age_ms, volatility_max_age_ms, future_skew_ms
        self.tickers: dict[str, Ticker] = {}
        self.ready = False
        self._last_forecast_available_ts_ms: int | None = None
        self._cached_volatility: VolatilitySnapshot | None = None

    async def start(self) -> None:
        await self.market_data.ensure_group()
        self.tickers = {ticker.market_ticker: ticker for ticker in await self.market_data.bootstrap_tickers()}

    async def cycle(self) -> None:
        now_ms = self.clock_ms()
        entries = await self.market_data.consume_tickers()
        for _, ticker in entries:
            self.tickers[ticker.market_ticker] = ticker
        try:
            spot = await self.market_data.read_spot()
            validate_fresh(spot.generated_ts_ms, now_ms, self.spot_max_age_ms, UnavailableReason.STALE_SPOT, self.future_skew_ms)
            features = await self.market_data.read_features()
            available_timestamp_ms = features.available_timestamp_ms or features.event_timestamp_ms
            validate_fresh(available_timestamp_ms, now_ms, self.feature_max_age_ms, UnavailableReason.STALE_FEATURES, self.future_skew_ms)
            if self._last_forecast_available_ts_ms == available_timestamp_ms and self._cached_volatility is not None:
                volatility = replace(self._cached_volatility, generated_ts_ms=now_ms)
            else:
                volatility = await self.forecasts.forecast(features, now_ms)
                self._last_forecast_available_ts_ms = available_timestamp_ms
                self._cached_volatility = volatility
            validate_term_structure(volatility.annualized_volatility)
            validate_fresh(volatility.generated_ts_ms, now_ms, self.volatility_max_age_ms, UnavailableReason.STALE_VOLATILITY, self.future_skew_ms)
            await self.publisher.publish_volatility(volatility)
        except PricingUnavailable as exc:
            for ticker in self.tickers.values():
                await self.publisher.unavailable(ticker.market_ticker, exc.reason.value, now_ms)
            self.ready = False
            if entries:
                await self.market_data.acknowledge([entry_id for entry_id, _ in entries])
            return

        series = next((ticker.series_ticker for ticker in self.tickers.values()), "KXBTCD")
        try:
            metadata = await self.metadata.markets(series, now_ms)
        except Exception:
            LOGGER.exception("kalshi_metadata_refresh_failed")
            metadata = {}
        known_events = {item.event_ticker for item in metadata.values()}
        active: set[str] = set()
        for ticker in list(self.tickers.values()):
            market_metadata = metadata.get(ticker.market_ticker)
            result = await self._price(ticker, market_metadata, spot, volatility, now_ms)
            if isinstance(result, PricingResult):
                active.add(ticker.market_ticker)
                await self.publisher.publish_price(ticker.market_ticker, result.payload)
                if result.quote_reason is not None:
                    LOGGER.info("pricing_quote_unavailable reason=%s market=%s", result.quote_reason.value, ticker.market_ticker)
            else:
                await self.publisher.unavailable(ticker.market_ticker, result.value, now_ms)
                expired = market_metadata is not None and market_metadata.expiry_ts_ms <= now_ms
                inactive_event = bool(metadata) and ticker.event_ticker not in known_events
                if expired or inactive_event:
                    self.tickers.pop(ticker.market_ticker, None)
        await self.publisher.expire_inactive(active, now_ms)
        if entries:
            await self.market_data.acknowledge([entry_id for entry_id, _ in entries])
        self.ready = bool(active) and self.forecasts.ready and await self.publisher.ping()

    async def _price(self, ticker: Ticker, metadata: MarketMetadata | None, spot: Spot,
                     volatility: VolatilitySnapshot, now_ms: int) -> PricingResult | UnavailableReason:
        try:
            if metadata is None:
                raise PricingUnavailable(UnavailableReason.MISSING_MARKET_METADATA)
            if (ticker.series_ticker != "KXBTCD" or metadata.event_ticker != ticker.event_ticker
                    or metadata.status.lower() not in OPEN_STATUSES):
                raise PricingUnavailable(UnavailableReason.UNSUPPORTED_CONTRACT)
            tau_seconds = (metadata.expiry_ts_ms - now_ms) / 1000
            sigma, bracket = interpolate_volatility(tau_seconds, volatility.annualized_volatility)
            probability = gaussian_probability(spot.price, metadata.strike, sigma, tau_seconds)
        except PricingUnavailable as exc:
            return exc.reason
        try:
            validate_fresh(ticker.exchange_ts_ms, now_ms, self.ticker_max_age_ms, UnavailableReason.STALE_TICKER, self.future_skew_ms)
            edges = quote_edges(ticker.yes_bid_dollars, ticker.yes_ask_dollars, probability)
            quote_reason = UnavailableReason.INVALID_QUOTE if edges["market_mid_probability"] is None else None
        except PricingUnavailable:
            edges = quote_edges(None, None, probability)
            quote_reason = UnavailableReason.STALE_TICKER
        payload = {
            "schema_version": 1, "event_type": "kalshi_model_price", "status": "available",
            "market_ticker": ticker.market_ticker, "event_ticker": ticker.event_ticker, "asset": "BTCUSD",
            "spot_price": spot.price, "spot_generated_ts_ms": spot.generated_ts_ms, "strike": metadata.strike,
            "expiry_ts_ms": metadata.expiry_ts_ms, "pricing_asof_ts_ms": now_ms,
            "time_to_expiry_seconds": tau_seconds, "time_to_expiry_minutes": tau_seconds / 60,
            "annualized_volatility": sigma, "volatility_bracket": list(bracket),
            "volatility_generated_ts_ms": volatility.generated_ts_ms, "interpolation_method": INTERPOLATION_METHOD,
            "feature_event_timestamp_ms": volatility.feature_asof_ts_ms,
            "inference_asof_timestamp_ms": volatility.feature_available_ts_ms or volatility.feature_asof_ts_ms,
            "distribution_model": DISTRIBUTION_MODEL, "model_probability": probability,
            "model_value_dollars": probability, "model_value_cents": 100 * probability, **edges,
            "model_version": volatility.model_version, "generated_ts_ms": now_ms,
        }
        return PricingResult(payload, quote_reason)

    async def run(self, stop: asyncio.Event) -> None:
        await self.start()
        while not stop.is_set():
            try:
                await self.cycle()
            except Exception:
                self.ready = False
                LOGGER.exception("analytics_cycle_failed")
                await asyncio.sleep(1)
