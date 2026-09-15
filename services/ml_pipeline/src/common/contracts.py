"""Immutable model input contracts used by loading, training, and artifacts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from src.common.generated_feature_contracts import (
    CONTRACT_DEFINITIONS,
    CURRENT_CONTRACT_VERSION,
    FEATURE_SET,
    HORIZON_SECONDS,
)

HORIZONS = tuple(HORIZON_SECONDS)


@dataclass(frozen=True, slots=True)
class ModelFeatureContract:
    feature_set: str
    feature_version: str
    feature_view: str
    feature_service: str
    offline_table: str
    label_version: str
    feature_columns: tuple[str, ...]
    default_architecture: str = "xgboost"
    horizon_feature_columns: tuple[tuple[str, tuple[str, ...]], ...] = ()

    def columns_for(self, horizon: str) -> tuple[str, ...]:
        """Return the ordered model inputs for one forecast horizon."""
        if horizon not in HORIZONS:
            raise ValueError(f"unsupported forecast horizon: {horizon}")
        configured = dict(self.horizon_feature_columns)
        return configured.get(horizon, self.feature_columns)


def _build_contract(version: str, definition: dict) -> ModelFeatureContract:
    inputs = definition["model_inputs"]
    used_columns = {column for columns in inputs.values() for column in columns}
    feature_columns = tuple(column for column in definition["fields"] if column in used_columns)
    horizon_columns = () if "default" in inputs else tuple(
        (horizon, tuple(inputs[horizon])) for horizon in HORIZONS
    )
    return ModelFeatureContract(
        feature_set=FEATURE_SET,
        feature_version=version,
        feature_view=definition["feature_view"],
        feature_service=definition["feature_service"],
        offline_table=definition["offline_table"],
        label_version=definition["label_version"],
        feature_columns=feature_columns,
        default_architecture=definition["default_architecture"],
        horizon_feature_columns=horizon_columns,
    )


CONTRACTS = {
    version: _build_contract(version, definition)
    for version, definition in CONTRACT_DEFINITIONS.items()
}


def resolve_contract(version: str = CURRENT_CONTRACT_VERSION) -> ModelFeatureContract:
    try:
        return CONTRACTS[version]
    except KeyError as exc:
        raise ValueError(f"unsupported model feature contract: {version}") from exc


def feature_contract_hash(*, feature_set: str, feature_version: str,
                          horizon: str, feature_columns: tuple[str, ...] | list[str]) -> str:
    """Return the stable identity of one model's ordered input contract."""
    payload = {
        "feature_columns": list(feature_columns),
        "feature_set": feature_set,
        "feature_version": feature_version,
        "horizon": horizon,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()
