"""Point-in-time historical feature retrieval for KFP training."""
from __future__ import annotations

import pandas as pd
from feast import FeatureStore


def fetch_historical_features(repo_path: str, entities: pd.DataFrame) -> pd.DataFrame:
    """Retrieve features without future leakage.

    ``entities`` must contain ``asset``, ``frequency``, and ``event_timestamp``.
    Targets are joined after this call.
    """
    required = {"asset", "frequency", "event_timestamp"}
    missing = required.difference(entities.columns)
    if missing:
        raise ValueError(f"missing entity columns: {sorted(missing)}")
    store = FeatureStore(repo_path=repo_path)
    return store.get_historical_features(
        entity_df=entities[["asset", "frequency", "event_timestamp"]].sort_values("event_timestamp"),
        features=["v1_market_features"],
    ).to_df()
