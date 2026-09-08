from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

INTERVAL_MS = 10_000
SUPPORTED_FREQUENCY = "10s"
DEFAULT_HISTORY_BARS = 450


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
        values: dict[str, Any] = {"synthetic_price": self.synthetic_price, "log_return": self.log_return, "venue_count": self.venue_count}
        if self.ewma_variance is not None:
            values["ewma_state"] = {"frequency": SUPPORTED_FREQUENCY, "variance": self.ewma_variance}
        return {
            "schema_version": 1, "event_type": "market_features", "feature_set": "market_features", "feature_version": "v1",
            "asset": self.asset, "event_timestamp": datetime.fromtimestamp(self.event_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "event_timestamp_ms": self.event_timestamp_ms,
            "available_timestamp": datetime.fromtimestamp(self.available_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "available_timestamp_ms": self.available_timestamp_ms,
            "created_timestamp": datetime.fromtimestamp(self.created_timestamp_ms / 1000, tz=timezone.utc).isoformat(),
            "values": values,
        }


class V1FeatureComputer:
    """Build live v1 features from per-venue completed 10s primitives."""

    def __init__(self, *, asset: str = "BTCUSD", ewma_decay: float = 0.96,
                 max_bar_age_ms: int = 120_000, history_bars: int = DEFAULT_HISTORY_BARS) -> None:
        if not 0.0 < ewma_decay < 1.0:
            raise ValueError("EWMA decay must be between zero and one")
        if max_bar_age_ms <= 0 or history_bars < 361:
            raise ValueError("history must retain at least 361 10s bars")
        self.asset, self.ewma_decay, self.max_bar_age_ms, self.history_bars = asset, ewma_decay, max_bar_age_ms, history_bars
        self._history: dict[str, deque[tuple[int, float]]] = defaultdict(lambda: deque(maxlen=history_bars))
        self._pending: dict[int, dict[str, float]] = {}
        self._last_seen_by_venue: dict[str, int] = {}
        self._last_timestamp_ms: int | None = None
        self._last_price: float | None = None
        self._ewma_variance: float | None = None

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
            forecast_variance = self._ewma_variance
            if log_return is not None:
                squared = log_return * log_return
                self._ewma_variance = squared if self._ewma_variance is None else self.ewma_decay * self._ewma_variance + (1.0 - self.ewma_decay) * squared
            self._last_timestamp_ms, self._last_price = completed, synthetic
            output = FeatureRow(self.asset, completed, completed + INTERVAL_MS, now_ms, synthetic, log_return, len(prices), forecast_variance)
        return None if replay else output

    def snapshot(self) -> dict[str, Any]:
        return {"schema_version": 2, "last_timestamp_ms": self._last_timestamp_ms, "last_price": self._last_price,
                "ewma_variance": self._ewma_variance, "history": {venue: list(values) for venue, values in self._history.items()},
                "last_seen_by_venue": self._last_seen_by_venue}

    def restore(self, state: dict[str, Any]) -> None:
        if state.get("schema_version") not in (1, 2):
            raise ValueError("unsupported feature state")
        timestamp, price, variance = state.get("last_timestamp_ms"), state.get("last_price"), state.get("ewma_variance")
        timestamp = None if timestamp is None else int(timestamp)
        if price is not None:
            price = float(price)
            if not math.isfinite(price) or price <= 0: raise ValueError("invalid persisted feature price")
        if variance is not None:
            variance = float(variance)
            if not math.isfinite(variance) or variance <= 0: raise ValueError("invalid persisted EWMA variance")
        self._last_timestamp_ms, self._last_price, self._ewma_variance = timestamp, price, variance
        self._history.clear()
        for venue, values in (state.get("history") or {}).items():
            history = deque(maxlen=self.history_bars)
            for item in values:
                if not isinstance(item, (list, tuple)) or len(item) != 2: raise ValueError("invalid persisted bar history")
                history.append((int(item[0]), float(item[1])))
            self._history[str(venue)] = history
        self._last_seen_by_venue = {str(k): int(v) for k, v in (state.get("last_seen_by_venue") or {}).items()}
