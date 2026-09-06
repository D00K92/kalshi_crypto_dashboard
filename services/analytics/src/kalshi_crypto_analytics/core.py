from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from .schemas import HORIZON_SECONDS, HORIZONS, PricingUnavailable, UnavailableReason

SECONDS_PER_YEAR = 365 * 24 * 60 * 60
INTERPOLATION_METHOD = "linear_total_variance_v1"
DISTRIBUTION_MODEL = "zero_log_return_gaussian_v1"


def _positive_finite(value: Any, name: str, reason: UnavailableReason) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise PricingUnavailable(reason, f"{name} must be numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise PricingUnavailable(reason, f"{name} must be finite and positive")
    return parsed


def validate_term_structure(volatilities: Mapping[str, Any]) -> dict[str, float]:
    if set(volatilities) != set(HORIZONS):
        raise PricingUnavailable(UnavailableReason.INCOMPLETE_TERM_STRUCTURE)
    return {
        horizon: _positive_finite(volatilities[horizon], horizon, UnavailableReason.INCOMPLETE_TERM_STRUCTURE)
        for horizon in HORIZONS
    }


def interpolate_volatility(tau_seconds: Any, volatilities: Mapping[str, Any]) -> tuple[float, tuple[str, str]]:
    tau = _positive_finite(tau_seconds, "tau_seconds", UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    if tau > 3600:
        raise PricingUnavailable(UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    vols = validate_term_structure(volatilities)
    if tau < 60:
        return vols["1m"], ("1m", "1m")
    if tau == 3600:
        return vols["1h"], ("1h", "1h")
    seconds = [HORIZON_SECONDS[horizon] for horizon in HORIZONS]
    upper_index = next(index for index, value in enumerate(seconds) if tau < value)
    lower, upper = HORIZONS[upper_index - 1], HORIZONS[upper_index]
    h0, h1 = HORIZON_SECONDS[lower], HORIZON_SECONDS[upper]
    variance0 = vols[lower] ** 2 * h0 / SECONDS_PER_YEAR
    variance1 = vols[upper] ** 2 * h1 / SECONDS_PER_YEAR
    if variance1 < variance0:
        raise PricingUnavailable(UnavailableReason.NON_MONOTONE_TOTAL_VARIANCE)
    weight = (tau - h0) / (h1 - h0)
    variance_tau = (1 - weight) * variance0 + weight * variance1
    return math.sqrt(variance_tau / (tau / SECONDS_PER_YEAR)), (lower, upper)


def gaussian_probability(spot: Any, strike: Any, sigma: Any, tau_seconds: Any) -> float:
    spot_value = _positive_finite(spot, "spot", UnavailableReason.STALE_SPOT)
    strike_value = _positive_finite(strike, "strike", UnavailableReason.INVALID_STRIKE)
    sigma_value = _positive_finite(sigma, "annualized_volatility", UnavailableReason.INCOMPLETE_TERM_STRUCTURE)
    tau = _positive_finite(tau_seconds, "tau_seconds", UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    if tau > 3600:
        raise PricingUnavailable(UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    z = math.log(spot_value / strike_value) / (sigma_value * math.sqrt(tau / SECONDS_PER_YEAR))
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def quote_edges(bid: Any, ask: Any, model_probability: float) -> dict[str, float | None]:
    empty = {"kalshi_yes_bid_dollars": None, "kalshi_yes_ask_dollars": None,
             "market_mid_probability": None, "edge_vs_mid_probability": None,
             "buy_yes_edge_probability": None, "sell_yes_edge_probability": None}
    try:
        bid_value, ask_value = float(bid), float(ask)
    except (TypeError, ValueError):
        return empty
    if not all(math.isfinite(value) and 0 <= value <= 1 for value in (bid_value, ask_value)) or bid_value > ask_value:
        return empty
    midpoint = (bid_value + ask_value) / 2
    return {"kalshi_yes_bid_dollars": bid_value, "kalshi_yes_ask_dollars": ask_value,
            "market_mid_probability": midpoint, "edge_vs_mid_probability": model_probability - midpoint,
            "buy_yes_edge_probability": model_probability - ask_value,
            "sell_yes_edge_probability": bid_value - model_probability}


def validate_fresh(timestamp_ms: Any, now_ms: int, max_age_ms: int, reason: UnavailableReason, future_skew_ms: int) -> int:
    try:
        timestamp = int(timestamp_ms)
    except (TypeError, ValueError) as exc:
        raise PricingUnavailable(reason, "invalid timestamp") from exc
    age = now_ms - timestamp
    if age > max_age_ms or age < -future_skew_ms:
        raise PricingUnavailable(reason)
    return timestamp
