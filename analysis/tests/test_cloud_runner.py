import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from analysis.cloud_runner import CloudConfig, run_cloud_job


TOY_STATS = {
    "raw_rows": 7,
    "malformed_rows": 1,
    "self_follows": 1,
    "repeated_non_self_edges": 1,
    "distinct_non_self_edges": 4,
    "users": 5,
    "followers_sum": 4,
    "followees_sum": 4,
}
TEST_JOB_ID = "a" * 32


class FakeS3Client:
    def __init__(self, input_body: bytes = b"toy input\n", fail_key: str | None = None) -> None:
        self.input_body = input_body
        self.fail_key = fail_key
        self.objects: dict[str, bytes] = {}
        self.put_calls: list[dict[str, object]] = []
        self.download_args: dict[str, object] | None = None

    def head_object(self, *, Bucket: str, Key: str):
        self.head_request = {"Bucket": Bucket, "Key": Key}
        return {
            "ContentLength": len(self.input_body),
            "ETag": '"input-etag"',
            "VersionId": "input-version-7",
        }

    def download_file(self, bucket: str, key: str, destination: str, **kwargs):
        self.download_args = {"bucket": bucket, "key": key, **kwargs}
        Path(destination).write_bytes(self.input_body)

    def put_object(self, **kwargs):
        key = str(kwargs["Key"])
        if key == self.fail_key:
            raise RuntimeError(f"injected upload failure for {key}")
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise RuntimeError(f"immutable object already exists: {key}")
        body = kwargs["Body"]
        data = body.read() if hasattr(body, "read") else bytes(body)
        self.objects[key] = data
        self.put_calls.append({**kwargs, "Body": data})
        return {"ETag": f'"{hashlib.md5(data, usedforsecurity=False).hexdigest()}"'}


class FakeJobTable:
    def __init__(self) -> None:
        self.status = "running"
        self.calls: list[dict[str, object]] = []

    def update_item(self, **kwargs):
        values = kwargs["ExpressionAttributeValues"]
        required = values[":running"]
        if self.status != required:
            raise RuntimeError(f"conditional state mismatch: {self.status} != {required}")
        self.calls.append(kwargs)
        return {}


def config() -> CloudConfig:
    return CloudConfig(
        job_id=TEST_JOB_ID,
        aws_region="ap-southeast-2",
        job_table="analysis-jobs",
        data_bucket="private-data",
        input_key="input/twitter_combined.txt",
        result_prefix_root="results/jobs",
        result_pointer_key="results/latest.json",
        spark_master="local[2]",
        analysis_script="/app/analysis/main.py",
    )


def successful_process(command: list[str], *, check: bool, env: dict[str, str]):
    if check:
        raise AssertionError("cloud runner should inspect the subprocess return code")
    result_dir = Path(command[command.index("--output") + 1])
    stats_path = Path(command[command.index("--stats-output") + 1])
    result_dir.mkdir(parents=True)
    (result_dir / "part-00001.csv").write_text(
        "user_id,followers,followees\n3,1,0\n4,0,1\n5,0,0\n", encoding="utf-8"
    )
    (result_dir / "part-00000.csv").write_text(
        "user_id,followers,followees\n1,2,2\n2,1,1\n", encoding="utf-8"
    )
    stats_path.write_text(json.dumps(TOY_STATS), encoding="utf-8")
    return SimpleNamespace(returncode=0)


class CloudRunnerTest(unittest.TestCase):
    def test_environment_contract_and_region_default(self) -> None:
        parsed = CloudConfig.from_environ(
            {
                "JOB_ID": TEST_JOB_ID,
                "AWS_JOB_TABLE": "jobs",
                "DATA_BUCKET": "bucket",
            }
        )
        self.assertEqual(parsed.aws_region, "ap-southeast-2")
        self.assertEqual(parsed.input_key, "input/twitter_combined.txt")
        self.assertEqual(parsed.result_prefix, f"results/jobs/{TEST_JOB_ID}")
        self.assertEqual(parsed.result_pointer_key, "results/latest.json")

        for job_id in ("../escape", "with/slash", "__active__", "A" * 32):
            with self.subTest(job_id=job_id):
                with self.assertRaises(ValueError):
                    CloudConfig.from_environ(
                        {
                            "JOB_ID": job_id,
                            "AWS_JOB_TABLE": "jobs",
                            "DATA_BUCKET": "bucket",
                        }
                    )

        with self.assertRaisesRegex(ValueError, "outside the immutable"):
            CloudConfig.from_environ(
                {
                    "JOB_ID": TEST_JOB_ID,
                    "AWS_JOB_TABLE": "jobs",
                    "DATA_BUCKET": "bucket",
                    "RESULT_POINTER_KEY": f"results/jobs/{TEST_JOB_ID}/latest.json",
                }
            )

    def test_success_publishes_verified_immutable_objects_before_latest_pointer(self) -> None:
        s3 = FakeS3Client()
        table = FakeJobTable()
        timestamps = iter(
            ["2026-10-16T01:00:00Z", "2026-10-16T01:01:00Z", "2026-10-16T01:01:01Z"]
        )
        seen_command: list[str] = []

        def run_process(command: list[str], *, check: bool, env: dict[str, str]):
            seen_command.extend(command)
            self.assertIn("org.apache.hadoop:hadoop-aws:3.5.0", env["PYSPARK_SUBMIT_ARGS"])
            self.assertIn("IAMInstanceCredentialsProvider", env["PYSPARK_SUBMIT_ARGS"])
            self.assertIn("fs.s3a.endpoint.region=ap-southeast-2", env["PYSPARK_SUBMIT_ARGS"])
            return successful_process(command, check=check, env=env)

        with patch.dict("analysis.cloud_runner.EXPECTED_STATS", TOY_STATS, clear=True):
            pointer = run_cloud_job(
                config(),
                s3_client=s3,
                job_table=table,
                run_process=run_process,
                clock=lambda: next(timestamps),
            )

        expected_order = [
            f"results/jobs/{TEST_JOB_ID}/part-00000.csv",
            f"results/jobs/{TEST_JOB_ID}/part-00001.csv",
            f"results/jobs/{TEST_JOB_ID}/stats.json",
            f"results/jobs/{TEST_JOB_ID}/manifest.json",
            f"results/jobs/{TEST_JOB_ID}/_SUCCESS",
            "results/latest.json",
        ]
        self.assertEqual([call["Key"] for call in s3.put_calls], expected_order)
        self.assertTrue(all(call["IfNoneMatch"] == "*" for call in s3.put_calls[:-1]))
        self.assertNotIn("IfNoneMatch", s3.put_calls[-1])
        self.assertEqual(table.status, "running")
        self.assertEqual(len(table.calls), 2)
        self.assertIn("analysis_validated_at", table.calls[-1]["UpdateExpression"])
        self.assertNotIn(":succeeded", table.calls[-1]["ExpressionAttributeValues"])
        self.assertEqual(s3.download_args["ExtraArgs"], {"VersionId": "input-version-7"})
        self.assertEqual(
            seen_command[seen_command.index("--input") + 1],
            "s3a://private-data/input/twitter_combined.txt",
        )
        self.assertEqual(seen_command[seen_command.index("--master") + 1], "local[2]")

        manifest = json.loads(s3.objects[pointer["manifest_key"]])
        self.assertEqual(manifest["job_id"], TEST_JOB_ID)
        self.assertEqual(manifest["users"], 5)
        self.assertEqual([part["key"] for part in manifest["parts"]], expected_order[:2])
        self.assertEqual(sum(part["rows"] for part in manifest["parts"]), 5)
        self.assertTrue(all(len(part["sha256"]) == 64 for part in manifest["parts"]))
        self.assertEqual(s3.objects[f"results/jobs/{TEST_JOB_ID}/_SUCCESS"], b"")
        self.assertEqual(json.loads(s3.objects["results/latest.json"]), pointer)

    def test_invariant_failure_annotates_running_job_without_publishing(self) -> None:
        s3 = FakeS3Client()
        table = FakeJobTable()

        def wrong_stats(command: list[str], *, check: bool, env: dict[str, str]):
            completed = successful_process(command, check=check, env=env)
            stats_path = Path(command[command.index("--stats-output") + 1])
            values = dict(TOY_STATS)
            values["users"] = 6
            stats_path.write_text(json.dumps(values), encoding="utf-8")
            return completed

        with patch.dict("analysis.cloud_runner.EXPECTED_STATS", TOY_STATS, clear=True):
            with self.assertRaisesRegex(ValueError, "users=6, expected 5"):
                run_cloud_job(
                    config(),
                    s3_client=s3,
                    job_table=table,
                    run_process=wrong_stats,
                    clock=lambda: "2026-10-16T01:00:00Z",
                )

        self.assertEqual(table.status, "running")
        self.assertEqual(len(table.calls), 2)
        self.assertIn("runner_error", table.calls[-1]["UpdateExpression"])
        self.assertEqual(s3.put_calls, [])

    def test_success_marker_failure_leaves_latest_pointer_untouched(self) -> None:
        success_key = f"results/jobs/{TEST_JOB_ID}/_SUCCESS"
        s3 = FakeS3Client(fail_key=success_key)
        table = FakeJobTable()

        with patch.dict("analysis.cloud_runner.EXPECTED_STATS", TOY_STATS, clear=True):
            with self.assertRaisesRegex(RuntimeError, "injected upload failure"):
                run_cloud_job(
                    config(),
                    s3_client=s3,
                    job_table=table,
                    run_process=successful_process,
                    clock=lambda: "2026-10-16T01:00:00Z",
                )

        self.assertEqual(table.status, "running")
        self.assertIn("runner_error", table.calls[-1]["UpdateExpression"])
        self.assertNotIn(success_key, s3.objects)
        self.assertNotIn("results/latest.json", s3.objects)
        self.assertIn(f"results/jobs/{TEST_JOB_ID}/manifest.json", s3.objects)

    def test_latest_pointer_failure_is_annotated_after_validated_artifacts(self) -> None:
        s3 = FakeS3Client(fail_key="results/latest.json")
        table = FakeJobTable()

        with patch.dict("analysis.cloud_runner.EXPECTED_STATS", TOY_STATS, clear=True):
            with self.assertRaisesRegex(RuntimeError, "injected upload failure"):
                run_cloud_job(
                    config(),
                    s3_client=s3,
                    job_table=table,
                    run_process=successful_process,
                    clock=lambda: "2026-10-16T01:00:00Z",
                )

        self.assertEqual(table.status, "running")
        self.assertEqual(len(table.calls), 3)
        self.assertIn("analysis_validated_at", table.calls[-2]["UpdateExpression"])
        self.assertIn("runner_error", table.calls[-1]["UpdateExpression"])
        self.assertIn(f"results/jobs/{TEST_JOB_ID}/_SUCCESS", s3.objects)
        self.assertNotIn("results/latest.json", s3.objects)


if __name__ == "__main__":
    unittest.main()
