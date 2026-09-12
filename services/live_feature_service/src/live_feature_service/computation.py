from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

INTERVAL_MS = 10_000
SUPPORTED_FREQUENCY = "10s"
EWMA_FREQUENCIES = {"5m": 300_000, "15m": 900_000, "30m": 1_800_000}
REALIZED_VOL_WINDOWS = {
    "30s": 3,
    "1m": 6,
    "5m": 30,
    "15m": 90,
    "30m": 180,
    "1h": 360,
    "3h": 1_080,
}
SECONDS_PER_YEAR = 365 * 24 * 60 * 60
FEATURE_SET = "market_features"
FEATURE_VERSION = "v2_10s"
DEFAULT_HISTORY_BARS = 450
V3_HISTORY_BARS = 1_100


@dataclass(frozen=True, slots=True)
class FeatureRow:
    asset: str
    feature_version: str
    event_timestamp_ms: int
    available_timestamp_ms: int
    created_timestamp_ms: int
    synthetic_price: float
    log_return: float | None
    venue_count: int
    realized_volatilities: dict[str, float]
    ewma_variances: dict[str, float]
    legacy_ewma_variance: float | None

    def payload(self) -> dict[str, Any]:
        values: dict[str, Any] = {"synthetic_price": self.synthetic_price, "log_return": self.log_return, "venue_count": self.venue_count}
        values.update({f"realized_vol_{window}": value for window, value in self.realized_volatilities.items()})
        if self.ewma_variances:
            values["ewma_states"] = {
                frequency: {"frequency": frequency, "variance": variance}
                for frequency, variance in self.ewma_variances.items()
            }
        if self.legacy_ewma_variance is not None:
            # Kept for one compatibility release while the sampled states warm
            # and while pods using the prior model-serving contract roll out.
            values["ewma_state"] = {"frequency": SUPPORTED_FREQUENCY, "variance": self.legacy_ewma_variance}
        return {
            "schema_version": 1, "event_type": "market_features", "feature_set": FEATURE_SET, "feature_version": self.feature_version,
            "asset": self.asset, "event_timestamp": datetime.fromtimestamp(self.event_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "event_timestamp_ms": self.event_timestamp_ms,
            "available_timestamp": datetime.fromtimestamp(self.available_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "available_timestamp_ms": self.available_timestamp_ms,
            "created_timestamp": datetime.fromtimestamp(self.created_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "values": values,
        }


class V2TenSecondFeatureComputer:
    """Build the live half of market_features/v2_10s from completed primitives."""

    def __init__(self, *, asset: str = "BTCUSD", ewma_decay: float = 0.96,
                 max_bar_age_ms: int = 120_000, history_bars: int = DEFAULT_HISTORY_BARS,
                 feature_version: str = FEATURE_VERSION) -> None:
        if not 0.0 < ewma_decay < 1.0:
            raise ValueError("EWMA decay must be between zero and one")
        if max_bar_age_ms <= 0 or history_bars < 361:
            raise ValueError("history must retain at least 361 10s bars")
        self.asset, self.ewma_decay, self.max_bar_age_ms, self.history_bars = asset, ewma_decay, max_bar_age_ms, history_bars
        self.feature_version = feature_version
        self._history: dict[str, deque[tuple[int, float]]] = defaultdict(lambda: deque(maxlen=history_bars))
        self._pending: dict[int, dict[str, float]] = {}
        self._last_seen_by_venue: dict[str, int] = {}
        self._last_timestamp_ms: int | None = None
        self._last_price: float | None = None
        self._ewma_variances: dict[str, float | None] = {frequency: None for frequency in EWMA_FREQUENCIES}
        self._sample_prices: dict[str, float | None] = {frequency: None for frequency in EWMA_FREQUENCIES}
        self._legacy_ewma_variance: float | None = None
        self._returns: deque[float] = deque(maxlen=history_bars - 1)

    @property
    def required_history_bars(self) -> int:
        return 1_081 if self.feature_version == "v3_10s" else 361

    @property
    def history_count(self) -> int:
        return min((len(values) for values in self._history.values()), default=0)

    def compute(self, bar: dict[str, Any], *, now_ms: int, replay: bool = False) -> FeatureRow | None:
        if bar.get("event_type") != "primitive_bar" or bar.get("frequency") != SUPPORTED_FREQUENCY:
            return None
        venue = str(bar.get("venue", "")).strip()
        if not venue:
            raise ValueError("primitive bar missing venue")
        timestamp = int(bar["bucket_start_ts_ms"])
        bucket_end = int(bar.get("bucket_end_ts_ms", timestamp + INTERVAL_MS))
        if timestamp % INTERVAL_MS:
            raise ValueError("10s primitive timestamp is not bucket aligned")
        if not replay and now_ms - bucket_end > self.max_bar_age_ms:
            raise ValueError("stale feature bar")
        previous_seen = self._last_seen_by_venue.get(venue)
        if previous_seen is not None and timestamp <= previous_seen:
            if timestamp == previous_seen:
                return None
            raise ValueError("out-of-order feature bar")
        if self._last_timestamp_ms is not None and timestamp <= self._last_timestamp_ms:
            # A newly observed or lagging venue must not revise a feature row
            # that has already advanced the global EWMA state.
            raise ValueError("out-of-order feature bar")
        raw_price = bar.get("p_trade_mean", bar.get("p_trade", bar.get("p_close")))
        try:
            price = float(raw_price)
        except (TypeError, ValueError) as exc:
            raise ValueError("primitive bar price must be numeric") from exc
        if not math.isfinite(price) or price <= 0:
            raise ValueError("primitive bar price must be positive and finite")
        self._last_seen_by_venue[venue] = timestamp
        self._history[venue].append((timestamp, price))
        self._pending.setdefault(timestamp, {})[venue] = price
        ready = sorted(key for key in self._pending if key < timestamp)
        output: FeatureRow | None = None
        for completed in ready:
            prices = self._pending.pop(completed)
            synthetic = math.fsum(prices.values()) / len(prices)
            log_return = None if self._last_price is None else math.log(synthetic / self._last_price)
            forecast_variances = {
                frequency: variance for frequency, variance in self._ewma_variances.items()
                if variance is not None
            }
            legacy_forecast_variance = self._legacy_ewma_variance
            if log_return is not None:
                squared = log_return * log_return
                self._returns.append(log_return)
                self._legacy_ewma_variance = squared if self._legacy_ewma_variance is None else self.ewma_decay * self._legacy_ewma_variance + (1.0 - self.ewma_decay) * squared
            realized_volatilities = self._realized_volatilities()
            bucket_end = completed + INTERVAL_MS
            for frequency, interval_ms in EWMA_FREQUENCIES.items():
                # A sampled close is available only after its full interval has
                # completed. Forecasts use the previous variance, then update.
                if bucket_end % interval_ms:
                    continue
                previous_sample = self._sample_prices[frequency]
                if previous_sample is not None:
                    sample_return = math.log(synthetic / previous_sample)
                    squared = sample_return * sample_return
                    prior = self._ewma_variances[frequency]
                    self._ewma_variances[frequency] = squared if prior is None else self.ewma_decay * prior + (1.0 - self.ewma_decay) * squared
                self._sample_prices[frequency] = synthetic
            self._last_timestamp_ms, self._last_price = completed, synthetic
            output = FeatureRow(self.asset, self.feature_version, completed, completed + INTERVAL_MS, now_ms, synthetic, log_return, len(prices), realized_volatilities, forecast_variances, legacy_forecast_variance)
            if self.feature_version == "v3_10s" and "3h" not in realized_volatilities:
                output = None
        return None if replay else output

    def _realized_volatilities(self) -> dict[str, float]:
        values = list(self._returns)
        result: dict[str, float] = {}
        for window, observations in REALIZED_VOL_WINDOWS.items():
            if len(values) < observations:
                continue
            sum_squared = math.fsum(value * value for value in values[-observations:])
            result[window] = math.sqrt(sum_squared * SECONDS_PER_YEAR / (observations * 10))
        return result

    def snapshot(self) -> dict[str, Any]:
        return {"schema_version": 5, "last_timestamp_ms": self._last_timestamp_ms, "last_price": self._last_price,
                "ewma_variance": self._legacy_ewma_variance, "ewma_variances": self._ewma_variances, "sample_prices": self._sample_prices,
                "returns": list(self._returns),
                "history": {venue: list(values) for venue, values in self._history.items()},
                "last_seen_by_venue": dict(self._last_seen_by_venue),
                "pending": {str(timestamp): dict(prices) for timestamp, prices in self._pending.items()}}

    def restore(self, state: dict[str, Any]) -> None:
        if state.get("schema_version") not in (1, 2, 3, 4, 5):
            raise ValueError("unsupported feature state")
        timestamp, price = state.get("last_timestamp_ms"), state.get("last_price")
        timestamp = None if timestamp is None else int(timestamp)
        if price is not None:
            price = float(price)
            if not math.isfinite(price) or price <= 0: raise ValueError("invalid persisted feature price")
        self._last_timestamp_ms, self._last_price = timestamp, price
        legacy_variance = state.get("ewma_variance")
        if legacy_variance is not None:
            legacy_variance = float(legacy_variance)
            if not math.isfinite(legacy_variance) or legacy_variance < 0: raise ValueError("invalid persisted EWMA variance")
        self._legacy_ewma_variance = legacy_variance
        raw_variances = state.get("ewma_variances") or {}
        raw_samples = state.get("sample_prices") or {}
        self._ewma_variances = {}
        self._sample_prices = {}
        for frequency in EWMA_FREQUENCIES:
            variance = raw_variances.get(frequency)
            sample = raw_samples.get(frequency)
            if variance is not None:
                variance = float(variance)
                if not math.isfinite(variance) or variance < 0: raise ValueError("invalid persisted EWMA variance")
            if sample is not None:
                sample = float(sample)
                if not math.isfinite(sample) or sample <= 0: raise ValueError("invalid persisted EWMA sample price")
            self._ewma_variances[frequency] = variance
            self._sample_prices[frequency] = sample
        self._history.clear()
        for venue, values in (state.get("history") or {}).items():
            history = deque(maxlen=self.history_bars)
            for item in values:
                if not isinstance(item, (list, tuple)) or len(item) != 2: raise ValueError("invalid persisted bar history")
                history.append((int(item[0]), float(item[1])))
            self._history[str(venue)] = history
        self._returns = deque(maxlen=self.history_bars - 1)
        if state.get("schema_version") == 5:
            for value in state.get("returns") or []:
                value = float(value)
                if not math.isfinite(value):
                    raise ValueError("invalid persisted return")
                self._returns.append(value)
        else:
            prices_by_timestamp: dict[int, list[float]] = defaultdict(list)
            for values in self._history.values():
                for item_timestamp, item_price in values:
                    prices_by_timestamp[item_timestamp].append(item_price)
            prior: float | None = None
            for item_timestamp in sorted(prices_by_timestamp):
                synthetic = math.fsum(prices_by_timestamp[item_timestamp]) / len(prices_by_timestamp[item_timestamp])
                if prior is not None:
                    self._returns.append(math.log(synthetic / prior))
                prior = synthetic
        self._last_seen_by_venue = {str(k): int(v) for k, v in (state.get("last_seen_by_venue") or {}).items()}
        pending = state.get("pending") or {}
        if state.get("schema_version") in (1, 2):
            # Older checkpoints omitted the in-flight bucket. Reconstruct it
            # from accepted history entries newer than the last completed row.
            reconstructed: dict[str, dict[str, float]] = {}
            for venue, values in self._history.items():
                for item_timestamp, item_price in values:
                    if self._last_timestamp_ms is None or item_timestamp > self._last_timestamp_ms:
                        reconstructed.setdefault(str(item_timestamp), {})[venue] = item_price
            pending = reconstructed
        self._pending = {}
        for raw_timestamp, raw_prices in pending.items():
            timestamp = int(raw_timestamp)
            prices = {str(venue): float(value) for venue, value in raw_prices.items()}
            if any(not math.isfinite(value) or value <= 0 for value in prices.values()):
                raise ValueError("invalid persisted pending price")
            self._pending[timestamp] = prices


class V3TenSecondFeatureComputer(V2TenSecondFeatureComputer):
    """HAR feature producer retaining the full three-hour lookback."""

    def __init__(self, *, asset: str = "BTCUSD", ewma_decay: float = 0.96,
                 max_bar_age_ms: int = 120_000, history_bars: int = V3_HISTORY_BARS) -> None:
        if history_bars < 1_081:
            raise ValueError("v3 history must retain at least 1081 10s bars")
        super().__init__(asset=asset, ewma_decay=ewma_decay, max_bar_age_ms=max_bar_age_ms,
                         history_bars=history_bars, feature_version="v3_10s")
