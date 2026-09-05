"""Versioned feature specifications used by producers and Feast writers."""

from .feature_specs import FEATURE_REGISTRY, FeatureSpec, resolve_feature_spec

__all__ = ["FEATURE_REGISTRY", "FeatureSpec", "resolve_feature_spec"]
