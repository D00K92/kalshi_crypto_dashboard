"""Container entrypoint for point-in-time training-data assembly."""
from __future__ import annotations

import argparse
from datetime import date
import logging
from pathlib import Path

from src.common.data_io import load_training_table, load_training_table_from_feast

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--feature-root")
    p.add_argument("--target-root")
    p.add_argument("--feast-repo")
    p.add_argument("--target-table")
    p.add_argument("--start-date", required=True, type=date.fromisoformat)
    p.add_argument("--end-date", required=True, type=date.fromisoformat)
    p.add_argument("--output", required=True)
    p.add_argument("--project")
    a = p.parse_args()
    source = "feast" if a.feast_repo else "gcs"
    logger.info("training_loader_started source=%s start=%s end=%s", source, a.start_date, a.end_date)
    try:
        if a.feast_repo:
            table = load_training_table_from_feast(
                project=a.project, feast_repo=a.feast_repo, start=a.start_date,
                end=a.end_date, target_table=a.target_table or "kalshi-crypto-506614.training_labels.future_realized_volatility_v1",
            )
        else:
            if not a.feature_root or not a.target_root:
                p.error("--feature-root and --target-root are required without --feast-repo")
            import gcsfs

            fs = gcsfs.GCSFileSystem(project=a.project)
            table = load_training_table(fs, a.feature_root, a.target_root, a.start_date, a.end_date)

        output = Path(a.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        table.to_parquet(output, index=False)
        logger.info("training_loader_completed rows=%d columns=%d", len(table), len(table.columns))
    except Exception:
        logger.exception("training_loader_failed source=%s", source)
        raise


if __name__ == "__main__":
    main()
