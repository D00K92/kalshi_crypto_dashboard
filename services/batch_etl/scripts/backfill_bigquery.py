"""Backfill BigQuery bars, features, and labels over an hourly range."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    if parsed.minute or parsed.second or parsed.microsecond:
        raise argparse.ArgumentTypeError("hours must be aligned to UTC hour")
    return parsed


def _hours(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(hours=1)


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {"hours": {}}
    try:
        state = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"cannot read state file {path}: {exc}") from exc
    if not isinstance(state, dict) or not isinstance(state.get("hours", {}), dict):
        raise SystemExit(f"invalid backfill state file: {path}")
    return state


def _save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _run_phase(command: list[str], *, retries: int) -> None:
    for attempt in range(1, retries + 2):
        print(f"running (attempt {attempt}/{retries + 1})", " ".join(command), flush=True)
        try:
            subprocess.run(command, check=True)
            return
        except subprocess.CalledProcessError:
            if attempt > retries:
                raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-hour", required=True, type=_hour)
    parser.add_argument("--end-hour", required=True, type=_hour)
    parser.add_argument("--project", default=os.getenv("GCP_PROJECT_ID", "kalshi-crypto-506614"))
    parser.add_argument("--bucket", default=os.getenv("GCS_BUCKET_NAME", "kalshi-crypto-tick-data"))
    parser.add_argument("--venues", default=os.getenv("BATCH_ETL_VENUES"))
    parser.add_argument("--frequencies", default=os.getenv("BATCH_ETL_FREQUENCIES"))
    parser.add_argument("--parallelism", type=int, default=int(os.getenv("BATCH_ETL_PARALLELISM", "3")),
                        help="venue workers used inside each resampling hour")
    parser.add_argument("--hour-parallelism", type=int, default=int(os.getenv("BATCH_ETL_HOUR_PARALLELISM", "2")),
                        help="maximum number of independent UTC hours per phase")
    parser.add_argument("--state-file", type=Path, default=Path(os.getenv("BATCH_ETL_BACKFILL_STATE", "backfill_bigquery_state.json")))
    parser.add_argument("--lock-file", type=Path, default=Path(os.getenv("BATCH_ETL_BACKFILL_LOCK", "/tmp/kalshi-bigquery-resample.lock")),
                        help="exclusive local lock preventing duplicate backfill writers")
    parser.add_argument("--resume", action="store_true", help="skip phases already marked complete in --state-file")
    parser.add_argument("--retries", type=int, default=2, help="retries per failed hourly phase")
    phases = parser.add_mutually_exclusive_group()
    phases.add_argument("--resample-only", action="store_true")
    phases.add_argument("--features-only", action="store_true")
    phases.add_argument("--targets-only", action="store_true")
    args = parser.parse_args()
    if args.end_hour < args.start_hour:
        parser.error("end-hour must not precede start-hour")
    if args.parallelism < 1 or args.hour_parallelism < 1 or args.retries < 0:
        parser.error("parallelism values must be positive and retries must not be negative")

    args.lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = args.lock_file.open("w")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise SystemExit(f"another BigQuery backfill is already active ({args.lock_file})") from exc

    state = _load_state(args.state_file) if args.resume else {"hours": {}}
    failures: list[tuple[str, str, str]] = []
    stamps = [current.strftime("%Y-%m-%dT%H:00:00Z") for current in _hours(args.start_hour, args.end_hour)]

    root = Path(__file__).resolve().parent
    resample = root / "run_bigquery_hourly.py"
    feature = root / "run_bigquery_features.py"
    target = root / "run_bigquery_targets.py"
    common = ["--project", args.project, "--parallelism", str(args.parallelism)]
    if args.bucket:
        common += ["--bucket", args.bucket]
    if args.venues:
        common += ["--venues", args.venues]
    if args.frequencies:
        common += ["--frequencies", args.frequencies]

    phase_commands: dict[str, tuple[Path, list[str]]] = {}
    if not args.features_only and not args.targets_only:
        phase_commands["resample"] = (resample, common)
    if not args.resample_only and not args.targets_only:
        phase_commands["features"] = (feature, ["--project", args.project])
    if not args.resample_only and not args.features_only:
        phase_commands["targets"] = (target, ["--project", args.project])
    phase_order = list(phase_commands)
    prerequisites = {phase_order[index]: phase_order[index - 1] for index in range(1, len(phase_order))}

    for phase in phase_order:
        script, base_command = phase_commands[phase]
        pending: list[tuple[str, list[str]]] = []
        for stamp in stamps:
            hour_state = state["hours"].setdefault(stamp, {})
            if args.resume and hour_state.get(phase) == "complete":
                print(f"skipping complete phase={phase} target={stamp}", flush=True)
                continue
            prerequisite = prerequisites.get(phase)
            if prerequisite and hour_state.get(prerequisite) != "complete":
                hour_state[phase] = "blocked"
                failures.append((stamp, phase, f"prerequisite {prerequisite} did not complete"))
                print(f"phase blocked target={stamp} phase={phase} prerequisite={prerequisite}", flush=True)
                continue
            pending.append((stamp, [sys.executable, str(script), *base_command, "--target-hour", stamp]))

        # Raw tables are shared across every venue and hour.  Serialize this
        # phase to avoid BigQuery DML serialization conflicts. Downstream
        # feature/target phases retain their caller-configured concurrency.
        workers = 1 if phase == "resample" else args.hour_parallelism
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_run_phase, command, retries=args.retries): stamp for stamp, command in pending}
            for future in as_completed(futures):
                stamp = futures[future]
                hour_state = state["hours"][stamp]
                try:
                    future.result()
                except subprocess.CalledProcessError as exc:
                    hour_state[phase] = "failed"
                    failures.append((stamp, phase, str(exc)))
                    print(f"phase failed target={stamp} phase={phase}; continuing", flush=True)
                else:
                    hour_state[phase] = "complete"
                    print(f"phase complete target={stamp} phase={phase}", flush=True)
                _save_state(args.state_file, state)

    if failures:
        print(f"backfill completed with {len(failures)} failed or blocked phase(s); see {args.state_file}", file=sys.stderr, flush=True)
        raise SystemExit(1)
    print(f"backfill completed successfully; state saved to {args.state_file}", flush=True)


if __name__ == "__main__":
    main()
