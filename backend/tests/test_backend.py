import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from backend.main import create_app
from backend.tests.test_load_results import write_part


class ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        root = Path(self.temporary_directory.name)
        self.database = root / "users.db"
        self.jobs = root / "jobs"
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                """
                CREATE TABLE user_counts (
                    user_id INTEGER PRIMARY KEY,
                    followers INTEGER NOT NULL,
                    followees INTEGER NOT NULL
                )
                """
            )
            connection.executemany(
                "INSERT INTO user_counts VALUES (?, ?, ?)",
                [(10, 5, 6), (2, 1, 3), (100, 8, 9)],
            )
        self.client = TestClient(
            create_app(
                self.database,
                load_on_startup=False,
                job_dir=self.jobs,
                analysis_api_token="test-token",
            )
        )

    def tearDown(self) -> None:
        self.client.close()
        self.temporary_directory.cleanup()

    def test_health_and_single_user_return_numeric_values(self) -> None:
        health = self.client.get("/api/health")
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json(), {"status": "ok", "users": 3})

        response = self.client.get("/api/users/10")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(), {"user_id": 10, "followers": 5, "followees": 6}
        )
        self.assertIsInstance(response.json()["user_id"], int)

    def test_users_are_paginated_in_numeric_id_order(self) -> None:
        response = self.client.get("/api/users", params={"limit": 2, "offset": 1})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "items": [
                    {"user_id": 10, "followers": 5, "followees": 6},
                    {"user_id": 100, "followers": 8, "followees": 9},
                ],
                "total": 3,
                "limit": 2,
                "offset": 1,
            },
        )

    def test_unknown_user_has_clear_404(self) -> None:
        response = self.client.get("/api/users/999")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "User 999 not found"})

    def test_invalid_pagination_is_rejected(self) -> None:
        self.assertEqual(self.client.get("/api/users?limit=0").status_code, 422)
        self.assertEqual(self.client.get("/api/users?offset=-1").status_code, 422)

    def test_startup_loads_published_results(self) -> None:
        root = Path(self.temporary_directory.name)
        results = root / "results"
        results.mkdir()
        write_part(results / "part-00000.csv", [[214328887, 180, 214]])
        loaded_database = root / "loaded.db"

        with TestClient(create_app(loaded_database, results, load_on_startup=True)) as client:
            response = client.get("/api/users/214328887")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["followers"], 180)
        self.assertEqual(response.json()["followees"], 214)

    def test_optional_origin_secret_protects_user_routes(self) -> None:
        protected = TestClient(
            create_app(
                self.database,
                load_on_startup=False,
                origin_verify_secret="cloudfront-only",
            )
        )
        try:
            self.assertEqual(protected.get("/api/health").status_code, 200)
            self.assertEqual(protected.get("/api/users/10").status_code, 403)
            response = protected.get(
                "/api/users/10",
                headers={"X-Origin-Verify": "cloudfront-only"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["user_id"], 10)
        finally:
            protected.close()

    def test_health_stays_ready_before_results_exist(self) -> None:
        empty_database = Path(self.temporary_directory.name) / "not-created.db"
        with TestClient(create_app(empty_database, load_on_startup=False)) as client:
            response = client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "users": 0})

    def test_analysis_submission_is_protected_and_deduplicated(self) -> None:
        self.assertEqual(self.client.post("/api/analysis/jobs").status_code, 403)

        headers = {"X-Analysis-Token": "test-token"}
        first = self.client.post("/api/analysis/jobs", headers=headers)
        duplicate = self.client.post("/api/analysis/jobs", headers=headers)

        self.assertEqual(first.status_code, 202)
        self.assertEqual(duplicate.status_code, 200)
        self.assertFalse(first.json()["duplicate"])
        self.assertTrue(duplicate.json()["duplicate"])
        self.assertEqual(duplicate.json()["job_id"], first.json()["job_id"])
        self.assertEqual(
            self.client.get("/api/analysis/jobs/latest").json()["job_id"],
            first.json()["job_id"],
        )
        self.assertEqual(
            self.client.get(f"/api/analysis/jobs/{first.json()['job_id']}").json()["status"],
            "queued",
        )


if __name__ == "__main__":
    unittest.main()
