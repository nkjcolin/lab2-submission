import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from backend.load_results import ResultImportError
from backend.s3_results import (
    S3ResultNotFound,
    load_latest_results_from_s3,
    load_results_from_s3,
    parse_s3_uri,
)


class FakePaginator:
    def __init__(self, pages: list[dict[str, object]]) -> None:
        self.pages = pages

    def paginate(self, **kwargs: str):
        self.request = kwargs
        return iter(self.pages)


class FakeS3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class FakeS3Client:
    def __init__(self, objects: dict[str, str | bytes]) -> None:
        self.objects = {
            key: value.encode("utf-8") if isinstance(value, str) else value
            for key, value in objects.items()
        }
        self.paginator = FakePaginator(
            [{"Contents": [{"Key": key} for key in reversed(list(objects))]}]
        )

    def get_paginator(self, name: str) -> FakePaginator:
        if name != "list_objects_v2":
            raise AssertionError(name)
        return self.paginator

    def download_file(self, bucket: str, key: str, destination: str) -> None:
        if bucket != "results-bucket":
            raise AssertionError(bucket)
        try:
            Path(destination).write_bytes(self.objects[key])
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey") from exc

    def get_object(self, *, Bucket: str, Key: str):
        if Bucket != "results-bucket":
            raise AssertionError(Bucket)
        try:
            return {"Body": io.BytesIO(self.objects[Key])}
        except KeyError as exc:
            raise FakeS3Error("NoSuchKey") from exc


def canonical_json(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def completed_objects() -> tuple[dict[str, bytes], str]:
    job_id = "a" * 32
    prefix = f"results/jobs/{job_id}"
    pointer_key = "results/latest.json"
    part_key = f"{prefix}/part-00000.csv"
    stats_key = f"{prefix}/stats.json"
    manifest_key = f"{prefix}/manifest.json"
    success_key = f"{prefix}/_SUCCESS"
    part = b"user_id,followers,followees\n2,0,1\n10,1,0\n"
    stats = {
        "users": 2,
        "distinct_non_self_edges": 1,
        "followers_sum": 1,
        "followees_sum": 1,
    }
    stats_body = canonical_json(stats)
    completed_at = "2026-10-16T00:00:00Z"
    manifest = {
        "version": 1,
        "job_id": job_id,
        "bucket": "results-bucket",
        "prefix": prefix,
        "created_at": completed_at,
        "input": {
            "bucket": "results-bucket",
            "key": "input/twitter_combined.txt",
            "etag": "input-etag",
            "size": 123,
            "sha256": "b" * 64,
        },
        **stats,
        "parts": [
            {
                "key": part_key,
                "size": len(part),
                "sha256": hashlib.sha256(part).hexdigest(),
                "etag": "part-etag",
                "rows": 2,
            }
        ],
        "stats_key": stats_key,
        "stats_sha256": hashlib.sha256(stats_body).hexdigest(),
        "success_key": success_key,
    }
    pointer = {
        "version": 1,
        "job_id": job_id,
        "bucket": "results-bucket",
        "prefix": prefix,
        "manifest_key": manifest_key,
        "stats_key": stats_key,
        "success_key": success_key,
        "users": 2,
        "completed_at": completed_at,
    }
    return {
        pointer_key: canonical_json(pointer),
        manifest_key: canonical_json(manifest),
        stats_key: stats_body,
        success_key: b"",
        part_key: part,
    }, pointer_key


def database_rows(path: Path) -> list[tuple[int, int, int]]:
    with sqlite3.connect(path) as connection:
        return connection.execute(
            "SELECT user_id, followers, followees FROM user_counts ORDER BY user_id"
        ).fetchall()


class S3ResultsTest(unittest.TestCase):
    def test_parse_s3_uri_requires_bucket_and_prefix(self) -> None:
        self.assertEqual(
            parse_s3_uri("s3://results-bucket/analysis/published/"),
            ("results-bucket", "analysis/published"),
        )
        for invalid in ("https://example.com/results", "s3://bucket", "s3:///prefix"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ResultImportError):
                    parse_s3_uri(invalid)

    def test_downloads_all_parts_and_imports_sqlite(self) -> None:
        header = "user_id,followers,followees\n"
        client = FakeS3Client(
            {
                "analysis/published/part-00001.csv": header + "10,4,2\n",
                "analysis/published/_SUCCESS": "",
                "analysis/published/part-00000.csv": header + "2,0,1\n",
            }
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "users.db"
            imported = load_results_from_s3(
                "s3://results-bucket/analysis/published", database, s3_client=client
            )

            self.assertEqual(imported, 2)
            self.assertEqual(database_rows(database), [(2, 0, 1), (10, 4, 2)])
            self.assertEqual(
                client.paginator.request,
                {"Bucket": "results-bucket", "Prefix": "analysis/published/"},
            )

    def test_missing_parts_does_not_replace_existing_database(self) -> None:
        client = FakeS3Client({"analysis/published/_SUCCESS": ""})
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "users.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_counts "
                    "(user_id INTEGER PRIMARY KEY, followers INTEGER, followees INTEGER)"
                )
                connection.execute("INSERT INTO user_counts VALUES (7, 8, 9)")

            with self.assertRaisesRegex(ResultImportError, r"no part-\*\.csv"):
                load_results_from_s3(
                    "s3://results-bucket/analysis/published",
                    database,
                    s3_client=client,
                )

            self.assertEqual(database_rows(database), [(7, 8, 9)])

    def test_strict_latest_manifest_imports_verified_parts(self) -> None:
        objects, pointer_key = completed_objects()
        client = FakeS3Client(objects)
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "users.db"
            completed = load_latest_results_from_s3(
                "results-bucket",
                pointer_key,
                database,
                expected_job_id="a" * 32,
                s3_client=client,
            )

            self.assertEqual(completed.job_id, "a" * 32)
            self.assertEqual(completed.imported_users, 2)
            self.assertEqual(completed.stats["distinct_non_self_edges"], 1)
            self.assertEqual(database_rows(database), [(2, 0, 1), (10, 1, 0)])

    def test_tampered_part_does_not_replace_existing_database(self) -> None:
        objects, pointer_key = completed_objects()
        part_key = next(key for key in objects if key.endswith(".csv"))
        objects[part_key] += b"11,0,0\n"
        client = FakeS3Client(objects)
        with tempfile.TemporaryDirectory() as temporary_directory:
            database = Path(temporary_directory) / "users.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE user_counts "
                    "(user_id INTEGER PRIMARY KEY, followers INTEGER, followees INTEGER)"
                )
                connection.execute("INSERT INTO user_counts VALUES (7, 8, 9)")

            with self.assertRaisesRegex(ResultImportError, "size does not match"):
                load_latest_results_from_s3(
                    "results-bucket", pointer_key, database, s3_client=client
                )

            self.assertEqual(database_rows(database), [(7, 8, 9)])

    def test_missing_latest_pointer_is_distinct_from_invalid_publication(self) -> None:
        client = FakeS3Client({})
        with tempfile.TemporaryDirectory() as temporary_directory:
            with self.assertRaises(S3ResultNotFound):
                load_latest_results_from_s3(
                    "results-bucket",
                    "results/latest.json",
                    Path(temporary_directory) / "users.db",
                    s3_client=client,
                )


if __name__ == "__main__":
    unittest.main()
