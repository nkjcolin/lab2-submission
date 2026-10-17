"""Persistent, process-safe analysis job records for the local stack."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ACTIVE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed"}
VALID_STATUSES = ACTIVE_STATUSES | TERMINAL_STATUSES


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class JobNotFoundError(KeyError):
    """Raised when an analysis job ID does not exist."""


class JobStore:
    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)

    def _ensure_directory(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _lock(self) -> Iterator[None]:
        self._ensure_directory()
        lock_path = self.directory / ".lock"
        with lock_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _job_path(self, job_id: str) -> Path:
        if not job_id or any(character not in "0123456789abcdef" for character in job_id):
            raise JobNotFoundError(job_id)
        return self.directory / f"{job_id}.json"

    def log_path(self, job_id: str) -> Path:
        self._job_path(job_id)
        self._ensure_directory()
        return self.directory / f"{job_id}.log"

    def stats_path(self, job_id: str) -> Path:
        self._job_path(job_id)
        self._ensure_directory()
        return self.directory / f"{job_id}.stats.json"

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle)
        if not isinstance(value, dict):
            raise ValueError(f"expected a JSON object in {path}")
        return value

    @staticmethod
    def _atomic_write(path: Path, value: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(value, handle, separators=(",", ":"), sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, path)
        except Exception:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    def _get_unlocked(self, job_id: str) -> dict[str, Any]:
        path = self._job_path(job_id)
        try:
            return self._read_json(path)
        except FileNotFoundError as exc:
            raise JobNotFoundError(job_id) from exc

    def get(self, job_id: str) -> dict[str, Any]:
        with self._lock():
            return self._get_unlocked(job_id)

    def _job_records_unlocked(self) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for path in self.directory.glob("*.json"):
            if path.name.endswith(".stats.json"):
                continue
            try:
                job = self._read_json(path)
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            if job.get("status") in VALID_STATUSES and isinstance(job.get("job_id"), str):
                jobs.append(job)
        return jobs

    def latest(self) -> dict[str, Any] | None:
        with self._lock():
            jobs = self._job_records_unlocked()
            if not jobs:
                return None
            return max(jobs, key=lambda job: str(job.get("created_at", "")))

    def create(self) -> tuple[dict[str, Any], bool]:
        """Create a queued job, or return the existing active job as a duplicate."""
        with self._lock():
            active_jobs = [
                job for job in self._job_records_unlocked() if job["status"] in ACTIVE_STATUSES
            ]
            if active_jobs:
                active = min(active_jobs, key=lambda job: str(job["created_at"]))
                return active, True

            job_id = uuid.uuid4().hex
            job: dict[str, Any] = {
                "job_id": job_id,
                "status": "queued",
                "created_at": utc_now(),
                "started_at": None,
                "finished_at": None,
                "error": None,
                "stats": None,
            }
            self._atomic_write(self._job_path(job_id), job)
            return job, False

    def claim_next(self) -> dict[str, Any] | None:
        with self._lock():
            queued = [
                job for job in self._job_records_unlocked() if job["status"] == "queued"
            ]
            if not queued:
                return None
            job = min(queued, key=lambda item: str(item["created_at"]))
            job["status"] = "running"
            job["started_at"] = utc_now()
            job["finished_at"] = None
            job["error"] = None
            self._atomic_write(self._job_path(job["job_id"]), job)
            return job

    def finish(
        self,
        job_id: str,
        status: str,
        *,
        error: str | None = None,
        stats: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        if status not in TERMINAL_STATUSES:
            raise ValueError(f"invalid terminal status: {status}")
        with self._lock():
            job = self._get_unlocked(job_id)
            job["status"] = status
            job["finished_at"] = utc_now()
            job["error"] = error
            job["stats"] = stats
            self._atomic_write(self._job_path(job_id), job)
            return job

    def recover_interrupted(self) -> list[str]:
        """Mark jobs left running by a stopped worker as failed."""
        recovered: list[str] = []
        with self._lock():
            for job in self._job_records_unlocked():
                if job["status"] != "running":
                    continue
                job["status"] = "failed"
                job["finished_at"] = utc_now()
                job["error"] = "Analysis worker restarted before the job completed."
                job["stats"] = None
                self._atomic_write(self._job_path(job["job_id"]), job)
                recovered.append(job["job_id"])
        return recovered
