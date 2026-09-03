import pandas as pd

from src.common.evaluation import retrain_decision


def test_retrain_requires_two_degraded_windows() -> None:
    current = {"metrics": {"rmse": 1.2, "mae": 1.2}}
    champion = {"metrics": {"rmse": 1.0, "mae": 1.0}}
    assert not retrain_decision(current, champion)["trigger_retraining"]
    assert retrain_decision(current, champion, prior_failures=1)["trigger_retraining"]
