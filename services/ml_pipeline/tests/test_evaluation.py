import pandas as pd

from src.common.benchmarks import ewma_annualized_volatility
from src.common.evaluation import retrain_decision


def test_ewma_accepts_feast_training_schema() -> None:
    table = pd.DataFrame({
        "timestamp": pd.date_range("2026-01-01", periods=3, freq="min", tz="UTC"),
        "frequency": ["1m", "1m", "1m"],
        "log_return": [0.001, -0.002, 0.0015],
    })

    forecast = ewma_annualized_volatility(table, "15m")

    assert len(forecast) == len(table)
    assert (forecast > 0).all()


def test_retrain_requires_two_degraded_windows() -> None:
    current = {"metrics": {"rmse": 1.2, "mae": 1.2}}
    champion = {"metrics": {"rmse": 1.0, "mae": 1.0}}
    assert not retrain_decision(current, champion)["trigger_retraining"]
    assert retrain_decision(current, champion, prior_failures=1)["trigger_retraining"]
