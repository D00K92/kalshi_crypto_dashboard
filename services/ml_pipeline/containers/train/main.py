"""Container entrypoint for one-horizon model training."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd

from src.common.contracts import CURRENT_CONTRACT_VERSION
from src.common.modeling import SUPPORTED_ARCHITECTURES, train_horizon


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True)
    p.add_argument("--horizon", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--feature-version", default=CURRENT_CONTRACT_VERSION)
    p.add_argument("--architecture", choices=SUPPORTED_ARCHITECTURES)
    a = p.parse_args()
    model, metadata = train_horizon(
        pd.read_parquet(a.dataset), a.horizon, feature_version=a.feature_version,
        architecture=a.architecture,
    )
    root = Path(a.output)
    root.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, root / "model.joblib")
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata["metrics"] | {"horizon": a.horizon}), flush=True)


if __name__ == "__main__":
    main()
