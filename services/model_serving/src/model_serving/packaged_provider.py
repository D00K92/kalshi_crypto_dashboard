from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from .inference_service import HORIZONS, EWMAProvider, ForecastRequest, ForecastResponse


class PackagedHybridProvider:
    """Serve one packaged 1h champion with EWMA for the other horizons."""

    def __init__(
        self,
        *,
        ewma: EWMAProvider,
        model: Any,
        feature_columns: tuple[str, ...],
        model_resource: str,
    ) -> None:
        self.ewma = ewma
        self.model = model
        self.feature_columns = feature_columns
        self.model_resource = model_resource
        self.ready = True

    @classmethod
    def load(
        cls,
        manifest_path: str | Path,
        *,
        model_version: str,
        feature_version: str,
        decay: float,
    ) -> PackagedHybridProvider:
        manifest_file = Path(manifest_path).resolve()
        manifest = _load_json(manifest_file)
        if manifest.get("schema_version") != 1:
            raise ValueError("unsupported model bundle schema")
        if manifest.get("model_version") != model_version:
            raise ValueError("model bundle version mismatch")
        if manifest.get("feature_version") != feature_version:
            raise ValueError("model bundle feature contract mismatch")

        horizons = manifest.get("horizons")
        if not isinstance(horizons, dict) or set(horizons) != set(HORIZONS):
            raise ValueError("model bundle must configure exactly five horizons")
        for horizon in HORIZONS[:-1]:
            expected_resource = f"ewma/{model_version}/{horizon}"
            entry = horizons[horizon]
            if not isinstance(entry, dict) or entry.get("kind") != "ewma" or entry.get("resource") != expected_resource:
                raise ValueError(f"invalid EWMA bundle entry for {horizon}")

        entry = horizons["1h"]
        if not isinstance(entry, dict) or entry.get("kind") != "xgboost":
            raise ValueError("1h bundle entry must be xgboost")
        resource = entry.get("resource")
        artifact_uri = entry.get("artifact_uri")
        if not isinstance(resource, str) or not resource.startswith("projects/") or "@" not in resource:
            raise ValueError("1h model resource must be an immutable Vertex version")
        if not isinstance(artifact_uri, str) or not artifact_uri.startswith("gs://"):
            raise ValueError("1h artifact URI must use gs://")

        root = manifest_file.parent
        model_file = _bundle_file(root, entry.get("model_path"))
        metadata_file = _bundle_file(root, entry.get("metadata_path"))
        _verify_sha256(model_file, entry.get("model_sha256"))
        _verify_sha256(metadata_file, entry.get("metadata_sha256"))

        metadata = _load_json(metadata_file)
        if metadata.get("horizon") != "1h":
            raise ValueError("packaged model horizon mismatch")
        if metadata.get("feature_set") != "market_features" or metadata.get("feature_version") != feature_version:
            raise ValueError("packaged model feature contract mismatch")
        columns = metadata.get("feature_columns")
        if columns != ["log_return", "venue_count"]:
            raise ValueError("packaged model has unexpected feature columns")

        model = joblib.load(model_file)
        if not callable(getattr(model, "predict", None)):
            raise ValueError("packaged model does not implement predict")
        provider = cls(
            ewma=EWMAProvider(model_version=model_version, feature_version=feature_version, decay=decay),
            model=model,
            feature_columns=tuple(columns),
            model_resource=resource,
        )
        provider._predict({"log_return": 0.0, "venue_count": 1})
        return provider

    def forecast(
        self,
        request: ForecastRequest,
        now_ms: int,
        *,
        max_age_ms: int,
        future_skew_ms: int,
    ) -> ForecastResponse:
        baseline = self.ewma.forecast(
            request,
            now_ms,
            max_age_ms=max_age_ms,
            future_skew_ms=future_skew_ms,
        )
        outputs = dict(baseline.annualized_volatility)
        outputs["1h"] = self._predict(request.values)
        resources = dict(baseline.model_resources)
        resources["1h"] = self.model_resource
        return ForecastResponse(
            outputs,
            baseline.feature_asof_ts_ms,
            baseline.generated_ts_ms,
            resources,
            baseline.model_version,
            baseline.feature_available_ts_ms,
        )

    def _predict(self, values: dict[str, object]) -> float:
        try:
            row = [float(values[column]) for column in self.feature_columns]
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("feature payload missing valid 1h model inputs") from exc
        if not all(math.isfinite(value) for value in row):
            raise ValueError("1h model inputs must be finite")
        prediction = self.model.predict(pd.DataFrame([row], columns=self.feature_columns, dtype=float))
        value = max(float(prediction[0]), 0.0)
        if not math.isfinite(value) or value <= 0:
            raise ValueError("1h model prediction must be positive and finite")
        return value


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid model bundle JSON: {path.name}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"model bundle JSON must be an object: {path.name}")
    return value


def _bundle_file(root: Path, raw_path: object) -> Path:
    if not isinstance(raw_path, str) or not raw_path:
        raise ValueError("model bundle path is missing")
    path = (root / raw_path).resolve()
    if root != path and root not in path.parents:
        raise ValueError("model bundle path escapes bundle root")
    if not path.is_file():
        raise ValueError(f"model bundle file is missing: {raw_path}")
    return path


def _verify_sha256(path: Path, expected: object) -> None:
    if not isinstance(expected, str) or len(expected) != 64:
        raise ValueError(f"invalid checksum for {path.name}")
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError(f"checksum mismatch for {path.name}")
