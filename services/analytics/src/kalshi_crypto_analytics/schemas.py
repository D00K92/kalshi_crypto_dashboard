from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

HORIZONS = ("5m", "15m", "30m", "1h")
HORIZON_SECONDS = {"5m": 300, "15m": 900, "30m": 1800, "1h": 3600}


class UnavailableReason(StrEnum):
    STALE_SPOT = "stale_spot"
    STALE_TICKER = "stale_ticker"
    STALE_FEATURES = "stale_features"
    STALE_VOLATILITY = "stale_volatility"
    MISSING_MARKET_METADATA = "missing_market_metadata"
    UNSUPPORTED_CONTRACT = "unsupported_contract"
    INVALID_STRIKE = "invalid_strike"
    OUTSIDE_SUPPORTED_LIFETIME = "outside_supported_lifetime"
    MODEL_INFERENCE_FAILED = "model_inference_failed"
    INCOMPLETE_TERM_STRUCTURE = "incomplete_term_structure"
    NON_MONOTONE_TOTAL_VARIANCE = "non_monotone_total_variance"
    INVALID_QUOTE = "invalid_quote"


class PricingUnavailable(ValueError):
    def __init__(self, reason: UnavailableReason, detail: str = "") -> None:
        super().__init__(detail or reason.value)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class Spot:
    price: float
    generated_ts_ms: int


@dataclass(frozen=True, slots=True)
class FeatureObservation:
    values: dict[str, Any]
    event_timestamp_ms: int
    available_timestamp_ms: int | None = None
    feature_set: str = "market_features"
    feature_version: str = "v2_10s"


@dataclass(frozen=True, slots=True)
class Ticker:
    market_ticker: str
    event_ticker: str
    series_ticker: str
    yes_bid_dollars: Any
    yes_ask_dollars: Any
    exchange_ts_ms: int
    open_interest: Any = None


@dataclass(frozen=True, slots=True)
class MarketMetadata:
    market_ticker: str
    event_ticker: str
    strike: float
    expiry_ts_ms: int
    status: str
    settlement_rules: str | None = None


@dataclass(frozen=True, slots=True)
class VolatilitySnapshot:
    annualized_volatility: dict[str, float]
    feature_asof_ts_ms: int
    generated_ts_ms: int
    model_resources: dict[str, str]
    model_version: str = "v2_10s"
    feature_available_ts_ms: int | None = None

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": 1, "event_type": "volatility_term_structure",
            "asset": "BTCUSD", "model_version": self.model_version,
            "model_resources": self.model_resources,
            "feature_asof_ts_ms": self.feature_asof_ts_ms,
            "feature_available_ts_ms": self.feature_available_ts_ms or self.feature_asof_ts_ms,
            "generated_ts_ms": self.generated_ts_ms,
            "annualized_volatility": self.annualized_volatility,
        }


@dataclass(frozen=True, slots=True)
class PricingResult:
    payload: dict[str, Any]
    quote_reason: UnavailableReason | None = None
