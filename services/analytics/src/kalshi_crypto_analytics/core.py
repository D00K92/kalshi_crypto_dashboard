from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from .schemas import HORIZON_SECONDS, HORIZONS, PricingUnavailable, UnavailableReason

SECONDS_PER_YEAR = 365 * 24 * 60 * 60
INTERPOLATION_METHOD = "linear_annualized_volatility_v1"
DISTRIBUTION_MODEL = "zero_log_return_gaussian_v1"
KALSHI_IV_METHOD = "robust_black_digital_mid_iv_v1"


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
    if tau < 300:
        return vols["5m"], ("5m", "5m")
    if tau == 3600:
        return vols["1h"], ("1h", "1h")
    seconds = [HORIZON_SECONDS[horizon] for horizon in HORIZONS]
    upper_index = next(index for index, value in enumerate(seconds) if tau < value)
    lower, upper = HORIZONS[upper_index - 1], HORIZONS[upper_index]
    h0, h1 = HORIZON_SECONDS[lower], HORIZON_SECONDS[upper]
    weight = (tau - h0) / (h1 - h0)
    sigma = (1 - weight) * vols[lower] + weight * vols[upper]
    return sigma, (lower, upper)


def gaussian_probability(spot: Any, strike: Any, sigma: Any, tau_seconds: Any) -> float:
    spot_value = _positive_finite(spot, "spot", UnavailableReason.STALE_SPOT)
    strike_value = _positive_finite(strike, "strike", UnavailableReason.INVALID_STRIKE)
    sigma_value = _positive_finite(sigma, "annualized_volatility", UnavailableReason.INCOMPLETE_TERM_STRUCTURE)
    tau = _positive_finite(tau_seconds, "tau_seconds", UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    if tau > 3600:
        raise PricingUnavailable(UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    z = math.log(spot_value / strike_value) / (sigma_value * math.sqrt(tau / SECONDS_PER_YEAR))
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


def black_digital_probability(spot: Any, strike: Any, sigma: Any, tau_seconds: Any) -> float:
    """Zero-rate Black digital-call probability used for Kalshi market IV."""
    spot_value = _positive_finite(spot, "spot", UnavailableReason.STALE_SPOT)
    strike_value = _positive_finite(strike, "strike", UnavailableReason.INVALID_STRIKE)
    sigma_value = _positive_finite(sigma, "annualized_volatility", UnavailableReason.INCOMPLETE_TERM_STRUCTURE)
    tau = _positive_finite(tau_seconds, "tau_seconds", UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    total_volatility = sigma_value * math.sqrt(tau / SECONDS_PER_YEAR)
    d2 = math.log(spot_value / strike_value) / total_volatility - total_volatility / 2
    return 0.5 * (1 + math.erf(d2 / math.sqrt(2)))


def calibrate_kalshi_iv(spot: Any, tau_seconds: Any, quotes: list[tuple[Any, Any, Any, Any]]) -> tuple[float, list[tuple[float, float, float]]]:
    """Fit one annualized Black-digital IV from 5–7 liquid KXBTCD mids."""
    spot_value = _positive_finite(spot, "spot", UnavailableReason.STALE_SPOT)
    tau = _positive_finite(tau_seconds, "tau_seconds", UnavailableReason.OUTSIDE_SUPPORTED_LIFETIME)
    selected: list[tuple[float, float, float]] = []
    for strike, bid, ask, open_interest in quotes:
        strike_value = _positive_finite(strike, "strike", UnavailableReason.INVALID_STRIKE)
        try:
            bid_value, ask_value = float(bid), float(ask)
        except (TypeError, ValueError):
            continue
        if not (math.isfinite(bid_value) and math.isfinite(ask_value) and 0 < bid_value <= ask_value < 1):
            continue
        midpoint = (bid_value + ask_value) / 2
        if not .01 <= midpoint <= .99:
            continue
        try:
            open_interest_value = max(0.0, float(open_interest))
        except (TypeError, ValueError):
            open_interest_value = 0.0
        selected.append((strike_value, midpoint, open_interest_value))
    selected.sort(key=lambda item: abs(math.log(item[0] / spot_value)))
    selected = selected[:7]
    if len(selected) < 5:
        raise PricingUnavailable(UnavailableReason.INVALID_QUOTE, "at least five valid near-ATM Kalshi quotes are required")
    total_weight = sum(open_interest + 1 for _, _, open_interest in selected)
    selected = [(strike, midpoint, (open_interest + 1) / total_weight) for strike, midpoint, open_interest in selected]

    def loss(sigma: float) -> float:
        result = 0.0
        for strike, midpoint, weight in selected:
            residual = black_digital_probability(spot_value, strike, sigma, tau) - midpoint
            # Huber loss prevents one stale/dislocated quote dominating the fit.
            huber = .5 * residual * residual if abs(residual) <= .02 else .02 * (abs(residual) - .01)
            result += weight * huber
        return result

    lower, upper, steps = .01, 5.0, 160
    grid = [lower + (upper - lower) * index / steps for index in range(steps + 1)]
    best = min(range(len(grid)), key=lambda index: loss(grid[index]))
    left, right = grid[max(0, best - 1)], grid[min(steps, best + 1)]
    # Golden-section refinement keeps this dependency-free and deterministic.
    ratio = (math.sqrt(5) - 1) / 2
    x1, x2 = right - ratio * (right - left), left + ratio * (right - left)
    for _ in range(36):
        if loss(x1) <= loss(x2):
            right, x2 = x2, x1
            x1 = right - ratio * (right - left)
        else:
            left, x1 = x1, x2
            x2 = left + ratio * (right - left)
    return (left + right) / 2, selected


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
