from __future__ import annotations

import asyncio
import json
import logging
import math
import urllib.error
import urllib.request
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol

import joblib
import pandas as pd

from .schemas import (
    HORIZONS,
    FeatureObservation,
    PricingUnavailable,
    UnavailableReason,
    VolatilitySnapshot,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ArtifactBundle:
    resource_name: str
    artifact_uri: str
    labels: dict[str, str]
    metadata: dict[str, Any]
    model: Any


class ArtifactResolver(Protocol):
    async def resolve(self, resource_name: str) -> ArtifactBundle: ...


class VertexGCSResolver:
    def __init__(self, *, project: str, location: str) -> None:
        self.project, self.location = project, location

    async def resolve(self, resource_name: str) -> ArtifactBundle:
        return await asyncio.to_thread(self._resolve, resource_name)

    def _resolve(self, resource_name: str) -> ArtifactBundle:
        from google.cloud import aiplatform, storage

        aiplatform.init(project=self.project, location=self.location)
        vertex_model = aiplatform.Model(resource_name)
        uri = vertex_model.uri.rstrip("/")
        if not uri.startswith("gs://"):
            raise ValueError("Vertex artifact URI must use gs://")
        bucket_name, prefix = uri[5:].split("/", 1)
        bucket = storage.Client(project=self.project).bucket(bucket_name)
        metadata = json.loads(bucket.blob(f"{prefix}/metadata.json").download_as_bytes())
        model = joblib.load(BytesIO(bucket.blob(f"{prefix}/model.joblib").download_as_bytes()))
        return ArtifactBundle(resource_name, uri, dict(vertex_model.labels or {}), metadata, model)


class ConfiguredForecastProvider:
    def __init__(self, resources: dict[str, str], resolver: ArtifactResolver, *, model_version: str = "v2_10s", feature_version: str = "v2_10s") -> None:
        if set(resources) != set(HORIZONS) or any(not value for value in resources.values()):
            raise ValueError("exactly four non-empty horizon model resources are required")
        self.resources, self.resolver, self.model_version, self.feature_version = resources, resolver, model_version, feature_version
        self._bundles: dict[str, ArtifactBundle] = {}

    @property
    def ready(self) -> bool:
        return set(self._bundles) == set(HORIZONS)

    async def load(self) -> None:
        resolved = await asyncio.gather(*(self.resolver.resolve(self.resources[h]) for h in HORIZONS))
        bundles: dict[str, ArtifactBundle] = {}
        for horizon, bundle in zip(HORIZONS, resolved, strict=True):
            if bundle.resource_name != self.resources[horizon]:
                raise ValueError(f"resolved resource mismatch for {horizon}")
            metadata = bundle.metadata
            label_horizon = bundle.labels.get("horizon")
            label_version = bundle.labels.get("version")
            if metadata.get("horizon") != horizon or (label_horizon and label_horizon != horizon):
                raise ValueError(f"artifact horizon mismatch for {horizon}")
            if label_version and label_version != self.model_version:
                raise ValueError(f"artifact version mismatch for {horizon}")
            columns = metadata.get("feature_columns")
            if not isinstance(columns, list) or not columns or not all(isinstance(column, str) for column in columns):
                raise ValueError(f"invalid feature columns for {horizon}")
            artifact_feature_version = metadata.get("feature_version")
            if artifact_feature_version and artifact_feature_version != self.feature_version:
                raise ValueError(f"artifact feature contract mismatch for {horizon}")
            bundles[horizon] = bundle
        self._bundles = bundles

    async def forecast(self, observation: FeatureObservation, now_ms: int) -> VolatilitySnapshot:
        if not self.ready:
            raise PricingUnavailable(UnavailableReason.MODEL_INFERENCE_FAILED, "models are not loaded")
        if observation.feature_set != "market_features" or observation.feature_version != self.feature_version:
            raise PricingUnavailable(UnavailableReason.UNSUPPORTED_CONTRACT)
        try:
            outputs: dict[str, float] = {}
            for horizon in HORIZONS:
                try:
                    bundle = self._bundles[horizon]
                    columns = bundle.metadata["feature_columns"]
                    row = [float(observation.values[column]) for column in columns]
                    if not all(math.isfinite(value) for value in row):
                        raise ValueError("non-finite feature")
                    frame = pd.DataFrame([row], columns=columns, dtype=float)
                    prediction = bundle.model.predict(frame)
                    value = max(float(prediction[0]), 0.0)
                    if not math.isfinite(value) or value <= 0:
                        raise ValueError("volatility prediction must be positive")
                    outputs[horizon] = value
                except Exception as exc:
                    LOGGER.exception("model_inference_failed horizon=%s detail=%s", horizon, exc)
                    raise
        except Exception as exc:
            raise PricingUnavailable(UnavailableReason.MODEL_INFERENCE_FAILED) from exc
        return VolatilitySnapshot(
            outputs,
            observation.event_timestamp_ms,
            now_ms,
            dict(self.resources),
            self.model_version,
            observation.available_timestamp_ms,
        )


class HttpForecastProvider:
    """Forecast provider backed by the internal model-serving API."""

    def __init__(self, *, base_url: str, timeout_ms: int, model_version: str = "v2_10s", feature_version: str = "v2_10s", transport=None) -> None:
        if not base_url.strip():
            raise ValueError("model-serving URL must not be empty")
        if timeout_ms <= 0:
            raise ValueError("model-serving timeout must be positive")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_ms / 1000
        self.model_version, self.feature_version = model_version, feature_version
        self._transport = transport or _urlopen_json
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    async def load(self) -> None:
        try:
            payload = await asyncio.to_thread(self._transport, "GET", f"{self.base_url}/readyz", None, self.timeout_seconds)
            if payload.get("status") != "ready":
                raise ValueError("model-serving is not ready")
        except Exception as exc:
            self._ready = False
            raise RuntimeError("model-serving readiness check failed") from exc
        self._ready = True

    async def forecast(self, observation: FeatureObservation, now_ms: int) -> VolatilitySnapshot:
        if not self.ready:
            raise PricingUnavailable(UnavailableReason.MODEL_INFERENCE_FAILED, "model-serving is not ready")
        if observation.feature_set != "market_features" or observation.feature_version != self.feature_version:
            raise PricingUnavailable(UnavailableReason.UNSUPPORTED_CONTRACT)
        request = {
            "feature_set": observation.feature_set,
            "feature_version": observation.feature_version,
            "event_timestamp_ms": observation.event_timestamp_ms,
            "available_timestamp_ms": observation.available_timestamp_ms or observation.event_timestamp_ms,
            "values": observation.values,
            "source_timestamps_ms": {"features": observation.available_timestamp_ms or observation.event_timestamp_ms},
        }
        try:
            payload = await asyncio.to_thread(self._transport, "POST", f"{self.base_url}/v1/forecast", request, self.timeout_seconds)
            volatility = payload.get("annualized_volatility")
            if not isinstance(volatility, dict) or set(volatility) != set(HORIZONS):
                raise ValueError("model-serving returned an incomplete term structure")
            values = {horizon: float(volatility[horizon]) for horizon in HORIZONS}
            if any(not math.isfinite(value) or value <= 0 for value in values.values()):
                raise ValueError("model-serving returned invalid volatility")
            if int(payload.get("feature_asof_ts_ms")) != observation.event_timestamp_ms:
                raise ValueError("model-serving feature timestamp mismatch")
            available_timestamp_ms = observation.available_timestamp_ms or observation.event_timestamp_ms
            if int(payload.get("feature_available_ts_ms", available_timestamp_ms)) != available_timestamp_ms:
                raise ValueError("model-serving feature availability timestamp mismatch")
            if payload.get("model_version") != self.model_version:
                raise ValueError("model-serving version mismatch")
            resources = payload.get("model_resources")
            if not isinstance(resources, dict) or set(resources) != set(HORIZONS):
                raise ValueError("model-serving returned invalid model resources")
            return VolatilitySnapshot(values, observation.event_timestamp_ms, int(payload["generated_ts_ms"]), resources, self.model_version, available_timestamp_ms)
        except Exception as exc:
            LOGGER.exception("model_inference_failed via model-serving: %s", exc)
            raise PricingUnavailable(UnavailableReason.MODEL_INFERENCE_FAILED) from exc


def _urlopen_json(method: str, url: str, payload: dict[str, Any] | None, timeout_seconds: float) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    request = urllib.request.Request(url, data=body, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            decoded = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError("model-serving request failed") from exc
    if not isinstance(decoded, dict):
        raise ValueError("model-serving response must be an object")
    return decoded
