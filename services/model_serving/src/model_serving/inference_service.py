from __future__ import annotations

import math
from dataclasses import dataclass

HORIZONS = ("1m", "5m", "15m", "30m", "1h")
HORIZON_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1_800, "1h": 3_600}
SECONDS_PER_YEAR = 365 * 24 * 60 * 60
SUPPORTED_FREQUENCIES = {"1s": 1, "5s": 5, "1m": 60, "5m": 300, "10m": 600, "15m": 900, "30m": 1_800, "1h": 3_600}


@dataclass(frozen=True, slots=True)
class ForecastRequest:
    feature_set: str
    feature_version: str
    event_timestamp_ms: int
    values: dict[str, object]
    source_timestamps_ms: dict[str, int]


@dataclass(frozen=True, slots=True)
class ForecastResponse:
    annualized_volatility: dict[str, float]
    feature_asof_ts_ms: int
    generated_ts_ms: int
    model_resources: dict[str, str]
    model_version: str


class EWMAProvider:
    """Deterministic champion provider using the live EWMA variance state."""

    def __init__(self, *, model_version: str = "v1", decay: float = 0.96) -> None:
        if not 0.0 < decay < 1.0:
            raise ValueError("EWMA decay must be between zero and one")
        self.model_version = model_version
        self.decay = decay

    def forecast(self, request: ForecastRequest, now_ms: int, *, max_age_ms: int, future_skew_ms: int) -> ForecastResponse:
        if request.feature_set != "market_features" or request.feature_version != "v1":
            raise ValueError("unsupported feature contract")
        for field in ("synthetic_price", "venue_count"):
            if field not in request.values or request.values[field] is None:
                raise ValueError(f"feature payload missing {field}")
        try:
            if not math.isfinite(float(request.values["synthetic_price"])):
                raise ValueError("synthetic_price must be finite")
            if int(request.values["venue_count"]) < 1:
                raise ValueError("venue_count must be positive")
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid market feature values") from exc
        if request.event_timestamp_ms > now_ms + future_skew_ms:
            raise ValueError("feature timestamp is in the future")
        if now_ms - request.event_timestamp_ms > max_age_ms:
            raise ValueError("feature timestamp is stale")
        state = request.values.get("ewma_state")
        if not isinstance(state, dict):
            raise ValueError("ewma_state is required")
        frequency = state.get("frequency")
        variance = state.get("variance")
        frequency_seconds = SUPPORTED_FREQUENCIES.get(frequency) if isinstance(frequency, str) else None
        if frequency_seconds is None:
            raise ValueError("unsupported EWMA frequency")
        try:
            variance_value = float(variance)
        except (TypeError, ValueError) as exc:
            raise ValueError("EWMA variance must be numeric") from exc
        if not math.isfinite(variance_value) or variance_value <= 0:
            raise ValueError("EWMA variance must be positive and finite")
        source_timestamps = request.source_timestamps_ms
        if not source_timestamps:
            raise ValueError("source timestamps are required")
        if any(not isinstance(timestamp, int) for timestamp in source_timestamps.values()):
            raise ValueError("source timestamps must be integers")
        if source_timestamps and min(source_timestamps.values()) < now_ms - max_age_ms:
            raise ValueError("source input is stale")

        # EWMA variance is expressed per base period. Annualization preserves
        # the same implied volatility across the requested forecast horizons.
        annualized = math.sqrt(variance_value * SECONDS_PER_YEAR / frequency_seconds)
        if not math.isfinite(annualized) or annualized <= 0:
            raise ValueError("EWMA forecast is invalid")
        outputs = {horizon: annualized for horizon in HORIZONS}
        resources = {horizon: f"ewma/{self.model_version}/{horizon}" for horizon in HORIZONS}
        return ForecastResponse(outputs, request.event_timestamp_ms, now_ms, resources, self.model_version)
