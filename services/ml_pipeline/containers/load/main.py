"""Container entrypoint for BigQuery training-data assembly."""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from src.common.contracts import CURRENT_CONTRACT_VERSION
from src.common.data_io import DEFAULT_TARGET_TABLE, load_training_table


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    p.add_argument("--feature-version", default=CURRENT_CONTRACT_VERSION)
    p.add_argument("--start-date", required=True, type=date.fromisoformat)
    p.add_argument("--end-date", required=True, type=date.fromisoformat)
    p.add_argument("--output", required=True)
    p.add_argument("--project", required=True)
    a = p.parse_args()
    table = load_training_table(
        project=a.project,
        start=a.start_date,
        end=a.end_date,
        target_table=a.target_table,
        feature_version=a.feature_version,
    )
    output = Path(a.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(output, index=False)
    print(f"loaded rows={len(table)} columns={len(table.columns)}", flush=True)


if __name__ == "__main__":
    main()
