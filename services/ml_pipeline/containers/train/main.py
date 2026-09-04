"""Container entrypoint for one-horizon model training."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd

from src.common.modeling import train_horizon


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dataset", required=True)
    p.add_argument("--horizon", required=True)
    p.add_argument("--output", required=True)
    a = p.parse_args()
    model, metadata = train_horizon(pd.read_parquet(a.dataset), a.horizon)
    root = Path(a.output)
    root.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, root / "model.joblib")
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(json.dumps(metadata["metrics"] | {"horizon": a.horizon}), flush=True)


if __name__ == "__main__":
    main()
