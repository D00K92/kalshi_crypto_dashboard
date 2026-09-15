from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import replace

from .core import (
    DISTRIBUTION_MODEL,
    INTERPOLATION_METHOD,
    KALSHI_IV_METHOD,
    calibrate_kalshi_iv,
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
QUOTE_LOG_INTERVAL_MS = 60_000
PRICING_WINDOW_LOWER = 6
PRICING_WINDOW_UPPER = 6
MIN_CYCLE_INTERVAL_SECONDS = 1.0


def _pricing_window(
    tickers: list[Ticker], metadata: dict[str, MarketMetadata], spot: float,
) -> list[Ticker]:
    """Select the freshest event and the same 13-strike window shown by the dashboard."""
    candidates = [
        ticker for ticker in tickers
        if ticker.series_ticker == "KXBTCD" and ticker.market_ticker in metadata
    ]
    if not candidates:
        return []
    active_event = max(candidates, key=lambda ticker: ticker.exchange_ts_ms).event_ticker
    event = sorted(
        (ticker for ticker in candidates if ticker.event_ticker == active_event),
        key=lambda ticker: metadata[ticker.market_ticker].strike,
    )
    atm_index = min(
        range(len(event)),
        key=lambda index: abs(metadata[event[index].market_ticker].strike - spot),
    )
    start = max(0, atm_index - PRICING_WINDOW_LOWER)
    return event[start:atm_index + PRICING_WINDOW_UPPER + 1]


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
        self._last_quote_summary_log_ts_ms: int | None = None
        self._published_tickers: set[str] = set()

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
            for market_ticker in self._published_tickers:
                await self.publisher.unavailable(market_ticker, exc.reason.value, now_ms)
            self._published_tickers.clear()
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
        await self._publish_kalshi_iv(metadata, spot, now_ms)
        active: set[str] = set()
        unavailable_quote_counts: dict[str, int] = {}
        for ticker in list(self.tickers.values()):
            market_metadata = metadata.get(ticker.market_ticker)
            expired = market_metadata is not None and market_metadata.expiry_ts_ms <= now_ms
            inactive_event = bool(metadata) and ticker.event_ticker not in known_events
            if expired or inactive_event:
                reason = (
                    UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME
                    if expired else UnavailableReason.MISSING_MARKET_METADATA
                )
                await self.publisher.unavailable(ticker.market_ticker, reason.value, now_ms)
                self.tickers.pop(ticker.market_ticker, None)

        published: list[tuple[str, dict[str, object]]] = []
        for ticker in _pricing_window(list(self.tickers.values()), metadata, spot.price):
            market_metadata = metadata.get(ticker.market_ticker)
            result = await self._price(ticker, market_metadata, spot, volatility, now_ms)
            if isinstance(result, PricingResult):
                active.add(ticker.market_ticker)
                published.append((ticker.market_ticker, result.payload))
                if result.quote_reason is not None:
                    reason = result.quote_reason.value
                    unavailable_quote_counts[reason] = unavailable_quote_counts.get(reason, 0) + 1
            else:
                await self.publisher.unavailable(ticker.market_ticker, result.value, now_ms)
        await self.publisher.publish_prices(published)
        if unavailable_quote_counts and (
            self._last_quote_summary_log_ts_ms is None
            or now_ms - self._last_quote_summary_log_ts_ms >= QUOTE_LOG_INTERVAL_MS
        ):
            LOGGER.info("pricing_quotes_unavailable counts=%s", unavailable_quote_counts)
            self._last_quote_summary_log_ts_ms = now_ms
        await self.publisher.expire_inactive(active, now_ms)
        self._published_tickers = active
        if entries:
            await self.market_data.acknowledge([entry_id for entry_id, _ in entries])
        self.ready = bool(active) and self.forecasts.ready and await self.publisher.ping()

    async def _publish_kalshi_iv(self, metadata: dict[str, MarketMetadata], spot: Spot, now_ms: int) -> None:
        candidates: list[tuple[Ticker, MarketMetadata]] = []
        for ticker in self.tickers.values():
            market = metadata.get(ticker.market_ticker)
            if market is None or market.event_ticker != ticker.event_ticker or market.status.lower() not in OPEN_STATUSES:
                continue
            tau_seconds = (market.expiry_ts_ms - now_ms) / 1000
            if ticker.series_ticker == "KXBTCD" and 0 < tau_seconds <= 3600:
                candidates.append((ticker, market))
        if not candidates:
            await self.publisher.publish_kalshi_iv(None)
            return
        # A single event has a common expiry. Prefer its freshest/nearest active set.
        event_ticker = max(candidates, key=lambda item: item[0].exchange_ts_ms)[0].event_ticker
        event = [(ticker, market) for ticker, market in candidates if ticker.event_ticker == event_ticker]
        tau_seconds = (event[0][1].expiry_ts_ms - now_ms) / 1000
        try:
            iv, selected = calibrate_kalshi_iv(
                spot.price, tau_seconds,
                [(market.strike, ticker.yes_bid_dollars, ticker.yes_ask_dollars, ticker.open_interest) for ticker, market in event],
            )
        except PricingUnavailable:
            await self.publisher.publish_kalshi_iv(None)
            return
        await self.publisher.publish_kalshi_iv({
            "schema_version": 1, "event_type": "kalshi_implied_volatility", "status": "available",
            "asset": "BTCUSD", "event_ticker": event_ticker, "annualized_implied_volatility": iv,
            "time_to_expiry_minutes": tau_seconds / 60, "contracts_used": len(selected),
            "strikes": [strike for strike, _, _ in selected], "method": KALSHI_IV_METHOD,
            "spot_price": spot.price, "generated_ts_ms": now_ms,
        })

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
            started = time.monotonic()
            try:
                await self.cycle()
            except Exception:
                self.ready = False
                LOGGER.exception("analytics_cycle_failed")
                await asyncio.sleep(1)
                continue
            remaining = MIN_CYCLE_INTERVAL_SECONDS - (time.monotonic() - started)
            if remaining > 0:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass
