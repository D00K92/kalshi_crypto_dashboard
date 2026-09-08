from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

SECONDS_PER_MINUTE = 60
SUPPORTED_FREQUENCY = "1m"


@dataclass(frozen=True, slots=True)
class FeatureRow:
    asset: str
    event_timestamp_ms: int
    available_timestamp_ms: int
    created_timestamp_ms: int
    synthetic_price: float
    log_return: float | None
    venue_count: int
    ewma_variance: float | None

    def payload(self) -> dict[str, Any]:
        values: dict[str, Any] = {
            "synthetic_price": self.synthetic_price,
            "log_return": self.log_return,
            "venue_count": self.venue_count,
        }
        if self.ewma_variance is not None:
            values["ewma_state"] = {"frequency": SUPPORTED_FREQUENCY, "variance": self.ewma_variance}
        return {
            "schema_version": 1,
            "event_type": "market_features",
            "feature_set": "market_features",
            "feature_version": "v1",
            "asset": self.asset,
            "event_timestamp": datetime.fromtimestamp(self.event_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "event_timestamp_ms": self.event_timestamp_ms,
            "available_timestamp": datetime.fromtimestamp(self.available_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "available_timestamp_ms": self.available_timestamp_ms,
            "created_timestamp": datetime.fromtimestamp(self.created_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "values": values,
        }


class V1FeatureComputer:
    """Compute the exact v1 SQL contract from completed 1-minute venue bars.

    The SQL source averages each venue's ``p_trade_mean`` first, then applies
    LAG and natural log. The bar producer supplies those venue means in
    ``venue_prices`` so this computation does not silently substitute a
    volume-weighted or cross-venue price.
    """

    def __init__(
        self,
        *,
        asset: str = "BTCUSD",
        ewma_decay: float = 0.96,
        max_bar_age_ms: int = 120_000,
    ) -> None:
        if not 0.0 < ewma_decay < 1.0:
            raise ValueError("EWMA decay must be between zero and one")
        if max_bar_age_ms <= 0:
            raise ValueError("maximum bar age must be positive")
        self.asset = asset
        self.ewma_decay = ewma_decay
        self.max_bar_age_ms = max_bar_age_ms
        self._last_timestamp_ms: int | None = None
        self._last_price: float | None = None
        self._ewma_variance: float | None = None

    def compute(self, bar: dict[str, Any], *, now_ms: int) -> FeatureRow | None:
        if bar.get("event_type") != "market_bar" or bar.get("frequency") != SUPPORTED_FREQUENCY:
            return None
        timestamp = int(bar["bucket_start_ts_ms"])
        bucket_end = int(bar.get("bucket_end_ts_ms", timestamp + SECONDS_PER_MINUTE * 1000))
        if now_ms - bucket_end > self.max_bar_age_ms:
            # Aggregator restarts can replay completed historical bars. They
            # must not become the latest live feature snapshot.
            raise ValueError("stale feature bar")
        if self._last_timestamp_ms is not None and timestamp <= self._last_timestamp_ms:
            # Redis redelivery is harmless; an older bar cannot mutate state.
            if timestamp == self._last_timestamp_ms:
                return None
            raise ValueError("out-of-order feature bar")
        raw_prices = bar.get("venue_prices")
        if not isinstance(raw_prices, dict):
            raise ValueError("1m bar missing venue_prices")
        prices = []
        for value in raw_prices.values():
            try:
                price = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError("venue price must be numeric") from exc
            if math.isfinite(price) and price > 0:
                prices.append(price)
        if not prices:
            raise ValueError("1m bar has no valid venue prices")
        synthetic = math.fsum(prices) / len(prices)
        log_return = None
        if self._last_price is not None:
            ratio = synthetic / self._last_price
            if ratio <= 0 or not math.isfinite(ratio):
                raise ValueError("invalid synthetic price ratio")
            log_return = math.log(ratio)
            if not math.isfinite(log_return):
                raise ValueError("non-finite log return")
        # The offline EWMA benchmark forecasts from prior returns (shift(1)).
        # Expose the pre-update state, then advance it with this bar's return.
        forecast_variance = self._ewma_variance
        if log_return is not None:
            squared = log_return * log_return
            self._ewma_variance = squared if self._ewma_variance is None else (
                self.ewma_decay * self._ewma_variance + (1.0 - self.ewma_decay) * squared
            )
        self._last_timestamp_ms = timestamp
        self._last_price = synthetic
        return FeatureRow(self.asset, timestamp, bucket_end, now_ms, synthetic, log_return, len(prices), forecast_variance)

    def snapshot(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "last_timestamp_ms": self._last_timestamp_ms,
            "last_price": self._last_price,
            "ewma_variance": self._ewma_variance,
        }

    def restore(self, state: dict[str, Any]) -> None:
        if state.get("schema_version") != 1:
            raise ValueError("unsupported feature state")
        timestamp = state.get("last_timestamp_ms")
        price = state.get("last_price")
        variance = state.get("ewma_variance")
        if timestamp is not None:
            timestamp = int(timestamp)
        if price is not None:
            price = float(price)
            if not math.isfinite(price) or price <= 0:
                raise ValueError("invalid persisted feature price")
        if variance is not None:
            variance = float(variance)
            if not math.isfinite(variance) or variance <= 0:
                raise ValueError("invalid persisted EWMA variance")
        self._last_timestamp_ms, self._last_price, self._ewma_variance = timestamp, price, variance
