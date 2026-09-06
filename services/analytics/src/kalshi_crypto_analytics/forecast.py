from __future__ import annotations

import asyncio
import json
import logging
import math
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
    def __init__(self, resources: dict[str, str], resolver: ArtifactResolver, *, model_version: str = "v1") -> None:
        if set(resources) != set(HORIZONS) or any(not value for value in resources.values()):
            raise ValueError("exactly five non-empty horizon model resources are required")
        self.resources, self.resolver, self.model_version = resources, resolver, model_version
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
            bundles[horizon] = bundle
        self._bundles = bundles

    async def forecast(self, observation: FeatureObservation, now_ms: int) -> VolatilitySnapshot:
        if not self.ready:
            raise PricingUnavailable(UnavailableReason.MODEL_INFERENCE_FAILED, "models are not loaded")
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
        return VolatilitySnapshot(outputs, observation.event_timestamp_ms, now_ms, dict(self.resources), self.model_version)
