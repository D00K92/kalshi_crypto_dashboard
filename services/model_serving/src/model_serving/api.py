from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from .config import Settings
from .inference_service import EWMAProvider, ForecastRequest, HORIZONS


class ForecastPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    feature_set: str
    feature_version: str
    event_timestamp_ms: int = Field(gt=0)
    values: dict[str, Any]
    source_timestamps_ms: dict[str, int]


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    provider = EWMAProvider(model_version=settings.model_version, decay=settings.ewma_decay)
    app = FastAPI(title="Kalshi Crypto Model Serving", version="1")
    app.state.ready = True

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict[str, str]:
        if not app.state.ready:
            raise HTTPException(status_code=503, detail="model provider is not ready")
        return {"status": "ready"}

    @app.post("/v1/forecast")
    def forecast(payload: ForecastPayload) -> dict[str, Any]:
        now_ms = time.time_ns() // 1_000_000
        request = ForecastRequest(
            feature_set=payload.feature_set,
            feature_version=payload.feature_version,
            event_timestamp_ms=payload.event_timestamp_ms,
            values=payload.values,
            source_timestamps_ms=payload.source_timestamps_ms,
        )
        try:
            result = provider.forecast(
                request,
                now_ms,
                max_age_ms=settings.max_feature_age_ms,
                future_skew_ms=settings.allowed_future_skew_ms,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if set(result.annualized_volatility) != set(HORIZONS):
            raise HTTPException(status_code=500, detail="incomplete forecast")
        return {
            "annualized_volatility": result.annualized_volatility,
            "feature_asof_ts_ms": result.feature_asof_ts_ms,
            "generated_ts_ms": result.generated_ts_ms,
            "model_resources": result.model_resources,
            "model_version": result.model_version,
        }

    return app


app = create_app()
