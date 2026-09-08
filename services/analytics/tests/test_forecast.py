from io import BytesIO

import joblib
import pandas as pd
import pytest
from xgboost import XGBRegressor

from kalshi_crypto_analytics.forecast import ArtifactBundle, ConfiguredForecastProvider, HttpForecastProvider
from kalshi_crypto_analytics.schemas import (
    HORIZONS,
    FeatureObservation,
    PricingUnavailable,
    UnavailableReason,
)


class Model:
    def __init__(self, value=.2, fail=False): self.value, self.fail, self.rows = value, fail, []
    def predict(self, rows):
        assert all(pd.api.types.is_numeric_dtype(dtype) for dtype in rows.dtypes)
        self.rows.extend(rows.values.tolist())
        if self.fail: raise RuntimeError("boom")
        return [self.value]


class Resolver:
    def __init__(self, fail_horizon=None, columns=None): self.fail_horizon = fail_horizon; self.columns = columns or ["b", "a"]; self.bundles = {}
    async def resolve(self, resource):
        horizon = resource.removeprefix("resource-")
        model = Model(.2, horizon == self.fail_horizon)
        bundle = ArtifactBundle(resource, f"gs://bucket/{horizon}", {"horizon": horizon, "version": "v1"},
                                {"horizon": horizon, "feature_columns": self.columns}, model)
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


async def test_live_string_feature_representation_is_cast_to_numeric():
    resolver = Resolver(columns=["log_return", "venue_count"])
    provider = ConfiguredForecastProvider(resources(), resolver)
    await provider.load()
    snapshot = await provider.forecast(FeatureObservation({"log_return": "0", "venue_count": 6}, 100), 110)
    assert set(snapshot.annualized_volatility) == set(HORIZONS)
    assert all(bundle.model.rows == [[0.0, 6.0]] for bundle in resolver.bundles.values())


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


async def test_http_provider_maps_and_validates_complete_response():
    calls = []

    def transport(method, url, payload, timeout):
        calls.append((method, url, payload, timeout))
        if method == "GET":
            return {"status": "ready"}
        return {
            "annualized_volatility": {horizon: 0.2 for horizon in HORIZONS},
            "feature_asof_ts_ms": 100,
            "generated_ts_ms": 110,
            "model_resources": {horizon: f"ewma/v1/{horizon}" for horizon in HORIZONS},
            "model_version": "v1",
        }

    provider = HttpForecastProvider(base_url="http://model-serving:8080/", timeout_ms=500, transport=transport)
    await provider.load()
    snapshot = await provider.forecast(FeatureObservation({"log_return": 0.0, "venue_count": 2}, 100), 110)
    assert snapshot.annualized_volatility == {horizon: 0.2 for horizon in HORIZONS}
    assert calls[0][1] == "http://model-serving:8080/readyz"
    assert calls[1][2]["source_timestamps_ms"] == {"features": 100}


async def test_http_provider_fails_closed_on_incomplete_response():
    def transport(method, url, payload, timeout):
        return {"status": "ready"} if method == "GET" else {"annualized_volatility": {"1m": 0.2}}

    provider = HttpForecastProvider(base_url="http://model-serving:8080", timeout_ms=500, transport=transport)
    await provider.load()
    with pytest.raises(PricingUnavailable) as exc:
        await provider.forecast(FeatureObservation({"log_return": 0.0, "venue_count": 2}, 100), 110)
    assert exc.value.reason == UnavailableReason.MODEL_INFERENCE_FAILED
