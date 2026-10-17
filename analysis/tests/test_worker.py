import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from analysis.worker import process_job
from backend.job_store import JobStore
from backend.load_results import load_results


def write_result(directory: Path, rows: list[list[int]]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "part-00000.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["user_id", "followers", "followees"])
        writer.writerows(rows)


def rows(database: Path) -> list[tuple[int, int, int]]:
    with sqlite3.connect(database) as connection:
        return connection.execute(
            "SELECT user_id, followers, followees FROM user_counts ORDER BY user_id"
        ).fetchall()


class WorkerTest(unittest.TestCase):
    def test_success_loads_sqlite_and_persists_stats(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            store = JobStore(root / "jobs")
            queued, _ = store.create()
            job = store.claim_next()
            results = root / "results"
            database = root / "users.db"

            def successful(command, **_kwargs):
                stats_path = Path(command[command.index("--stats-output") + 1])
                write_result(results, [[10, 5, 6], [20, 0, 1]])
                stats_path.write_text(
                    json.dumps({"users": 2, "distinct_non_self_edges": 6}),
                    encoding="utf-8",
                )
                return SimpleNamespace(returncode=0)

            with patch.dict(
                "os.environ",
                {"RESULT_DIR": str(results), "DB_PATH": str(database)},
                clear=False,
            ):
                process_job(job, store, run_process=successful)

            completed = store.get(queued["job_id"])
            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(completed["stats"]["imported_users"], 2)
            self.assertEqual(rows(database), [(10, 5, 6), (20, 0, 1)])

    def test_failed_process_retains_previous_valid_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            results = root / "results"
            database = root / "users.db"
            write_result(results, [[7, 8, 9]])
            load_results(results, database)
            store = JobStore(root / "jobs")
            queued, _ = store.create()
            job = store.claim_next()

            with patch.dict(
                "os.environ",
                {"RESULT_DIR": str(results), "DB_PATH": str(database)},
                clear=False,
            ):
                process_job(
                    job,
                    store,
                    run_process=lambda *_args, **_kwargs: SimpleNamespace(returncode=17),
                )

            failed = store.get(queued["job_id"])
            self.assertEqual(failed["status"], "failed")
            self.assertIn("status 17", failed["error"])
            self.assertEqual(rows(database), [(7, 8, 9)])


if __name__ == "__main__":
    unittest.main()
