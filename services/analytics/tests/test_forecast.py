from io import BytesIO

import joblib
import pandas as pd
import pytest
from xgboost import XGBRegressor

from kalshi_crypto_analytics.forecast import ArtifactBundle, ConfiguredForecastProvider
from kalshi_crypto_analytics.schemas import (
    HORIZONS,
    FeatureObservation,
    PricingUnavailable,
    UnavailableReason,
)


class Model:
    def __init__(self, value=.2, fail=False): self.value, self.fail, self.rows = value, fail, []
    def predict(self, rows):
        self.rows.extend(rows.values.tolist())
        if self.fail: raise RuntimeError("boom")
        return [self.value]


class Resolver:
    def __init__(self, fail_horizon=None): self.fail_horizon = fail_horizon; self.bundles = {}
    async def resolve(self, resource):
        horizon = resource.removeprefix("resource-")
        model = Model(.2, horizon == self.fail_horizon)
        bundle = ArtifactBundle(resource, f"gs://bucket/{horizon}", {"horizon": horizon, "version": "v1"},
                                {"horizon": horizon, "feature_columns": ["b", "a"]}, model)
        self.bundles[horizon] = bundle
        return bundle


def resources(): return {h: f"resource-{h}" for h in HORIZONS}


async def test_metadata_feature_order_and_atomic_five_horizon_inference():
    resolver = Resolver()
    provider = ConfiguredForecastProvider(resources(), resolver)
    await provider.load()
    snapshot = await provider.forecast(FeatureObservation({"a": 1, "b": 2}, 100), 110)
    assert set(snapshot.annualized_volatility) == set(HORIZONS)
    assert all(bundle.model.rows == [[2, 1]] for bundle in resolver.bundles.values())


async def test_partial_model_failure_publishes_no_snapshot():
    provider = ConfiguredForecastProvider(resources(), Resolver("15m"))
    await provider.load()
    with pytest.raises(PricingUnavailable) as exc:
        await provider.forecast(FeatureObservation({"a": 1, "b": 2}, 100), 110)
    assert exc.value.reason == UnavailableReason.MODEL_INFERENCE_FAILED


def test_exact_resource_set_is_required():
    with pytest.raises(ValueError, match="exactly five"):
        ConfiguredForecastProvider({"1m": "latest"}, Resolver())


def test_training_joblib_xgboost_format_round_trips_in_runtime():
    columns = ["b", "a"]
    frame = pd.DataFrame([[0.0, 0.0], [1.0, 1.0], [2.0, 1.0]], columns=columns)
    model = XGBRegressor(n_estimators=2, max_depth=1, n_jobs=1).fit(frame, [0.1, 0.2, 0.3])
    encoded = BytesIO()
    joblib.dump(model, encoded)
    encoded.seek(0)
    restored = joblib.load(encoded)
    result = restored.predict(pd.DataFrame([[1.0, 2.0]], columns=columns))
    assert float(result[0]) > 0
