import tempfile
import unittest
from pathlib import Path

from backend.job_store import JobStore


class JobStoreTest(unittest.TestCase):
    def test_duplicate_submission_returns_the_active_job(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            store = JobStore(Path(temporary_directory) / "jobs")

            first, duplicate = store.create()
            second, second_duplicate = store.create()

            self.assertFalse(duplicate)
            self.assertTrue(second_duplicate)
            self.assertEqual(second["job_id"], first["job_id"])
            self.assertEqual(second["status"], "queued")

    def test_status_survives_new_store_and_interrupted_run_becomes_failed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "jobs"
            store = JobStore(path)
            queued, _ = store.create()
            running = store.claim_next()
            self.assertEqual(running["job_id"], queued["job_id"])
            self.assertEqual(running["status"], "running")

            restarted_store = JobStore(path)
            self.assertEqual(restarted_store.recover_interrupted(), [queued["job_id"]])
            recovered = restarted_store.get(queued["job_id"])
            self.assertEqual(recovered["status"], "failed")
            self.assertIn("restarted", recovered["error"])

            replacement, duplicate = restarted_store.create()
            self.assertFalse(duplicate)
            self.assertNotEqual(replacement["job_id"], queued["job_id"])


if __name__ == "__main__":
    unittest.main()
