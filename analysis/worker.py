"""Idle local worker that launches the PySpark process for queued jobs."""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from backend.job_store import JobStore
from backend.load_results import load_results


LOGGER = logging.getLogger("analysis-worker")


def analysis_command(job_id: str, store: JobStore) -> list[str]:
    command = [
        sys.executable,
        os.environ.get("ANALYSIS_SCRIPT", "/app/analysis/main.py"),
        "--input",
        os.environ.get("ANALYSIS_INPUT", "/input/twitter_combined.txt"),
        "--output",
        os.environ.get("RESULT_DIR", "/data/results-root/published"),
        "--stats-output",
        str(store.stats_path(job_id)),
    ]
    master = os.environ.get("SPARK_MASTER")
    if master:
        command.extend(["--master", master])
    return command


def process_job(
    job: dict[str, Any],
    store: JobStore,
    *,
    run_process: Any = subprocess.run,
) -> None:
    job_id = str(job["job_id"])
    log_path = store.log_path(job_id)
    stats_path = store.stats_path(job_id)
    stats_path.unlink(missing_ok=True)
    LOGGER.info("Starting analysis job %s", job_id)

    try:
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"Starting analysis job {job_id}\n")
            log.flush()
            completed = run_process(
                analysis_command(job_id, store),
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
                text=True,
            )
        if completed.returncode != 0:
            raise RuntimeError(f"PySpark process exited with status {completed.returncode}")

        with stats_path.open(encoding="utf-8") as handle:
            raw_stats = json.load(handle)
        stats = {key: int(value) for key, value in raw_stats.items()}
        imported = load_results(
            os.environ.get("RESULT_DIR", "/data/results-root/published"),
            os.environ.get("DB_PATH", "/data/state/users.db"),
            expected_rows=stats.get("users"),
        )
        stats["imported_users"] = imported
        store.finish(job_id, "succeeded", stats=stats)
        LOGGER.info("Analysis job %s succeeded with %d users", job_id, imported)
    except Exception as exc:
        LOGGER.exception("Analysis job %s failed", job_id)
        store.finish(job_id, "failed", error=str(exc)[:1000])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = JobStore(os.environ.get("JOB_DIR", "/data/state/jobs"))
    recovered = store.recover_interrupted()
    for job_id in recovered:
        LOGGER.warning("Marked interrupted job %s as failed", job_id)

    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    poll_seconds = max(0.1, float(os.environ.get("JOB_POLL_SECONDS", "1")))
    LOGGER.info("Analysis worker ready; Spark is stopped while the queue is empty")

    while not stopping:
        job = store.claim_next()
        if job is None:
            time.sleep(poll_seconds)
            continue
        process_job(job, store)
    LOGGER.info("Analysis worker stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
