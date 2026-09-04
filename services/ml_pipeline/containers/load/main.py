"""Container entrypoint for point-in-time training-data assembly."""
from __future__ import annotations

import argparse
from datetime import date

import gcsfs

from src.common.data_io import load_training_table


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--feature-root", required=True)
    p.add_argument("--target-root", required=True)
    p.add_argument("--start-date", required=True, type=date.fromisoformat)
    p.add_argument("--end-date", required=True, type=date.fromisoformat)
    p.add_argument("--output", required=True)
    p.add_argument("--project")
    a = p.parse_args()
    fs = gcsfs.GCSFileSystem(project=a.project)
    load_training_table(fs, a.feature_root, a.target_root, a.start_date, a.end_date).to_parquet(a.output, index=False)


if __name__ == "__main__":
    main()
