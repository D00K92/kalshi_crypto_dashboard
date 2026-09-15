"""Declarative live-feature contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .generated_feature_contracts import CONTRACT_DEFINITIONS, FEATURE_SET


@dataclass(frozen=True)
class FeatureSpec:
    feature_set: str
    version: str
    feature_view: str
    push_source: str
    fields: tuple[str, ...]
    required_fields: tuple[str, ...]

    def validate(self, values: dict[str, Any]) -> None:
        missing = [name for name in self.required_fields if name not in values or values[name] is None]
        if missing:
            raise ValueError(f"feature payload missing fields: {missing}")


FEATURE_REGISTRY: dict[tuple[str, str], FeatureSpec] = {
    (FEATURE_SET, version): FeatureSpec(
        feature_set=FEATURE_SET,
        version=version,
        feature_view=definition["feature_view"],
        push_source=f'{definition["feature_view"]}_push',
        fields=tuple(definition["fields"]),
        required_fields=tuple(definition["required_fields"]),
    )
    for version, definition in CONTRACT_DEFINITIONS.items()
}


def resolve_feature_spec(feature_set: str, version: str) -> FeatureSpec:
    try:
        return FEATURE_REGISTRY[(feature_set, version)]
    except KeyError as exc:
        raise ValueError(f"unsupported feature contract: {feature_set}/{version}") from exc
