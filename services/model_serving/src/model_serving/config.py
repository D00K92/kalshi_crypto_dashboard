from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    model_version: str = "v1"
    max_feature_age_ms: int = 60_000
    allowed_future_skew_ms: int = 2_000
    ewma_decay: float = 0.96

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            model_version=os.getenv("MODEL_VERSION", "v1").strip() or "v1",
            max_feature_age_ms=_positive_int("MODEL_MAX_FEATURE_AGE_MS", 90_000),
            allowed_future_skew_ms=_positive_int("MODEL_ALLOWED_FUTURE_SKEW_MS", 2_000),
            ewma_decay=_bounded_float("EWMA_DECAY", 0.96, 0.0, 1.0),
        )


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _bounded_float(name: str, default: float, lower: float, upper: float) -> float:
    value = float(os.getenv(name, str(default)))
    if not lower < value < upper:
        raise ValueError(f"{name} must be between {lower} and {upper}")
    return value
