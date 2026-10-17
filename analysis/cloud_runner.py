"""Run one cloud analysis job and publish its validated output to private S3."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping


LOGGER = logging.getLogger("cloud-analysis-runner")
JOB_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
EXPECTED_HEADER = ["user_id", "followers", "followees"]
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.5.0"
S3A_CREDENTIALS_PROVIDER = "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider"
EXPECTED_STATS: dict[str, int] = {
    "raw_rows": 2_426_143,
    "malformed_rows": 0,
    "self_follows": 22,
    "repeated_non_self_edges": 657_986,
    "distinct_non_self_edges": 1_768_135,
    "users": 81_306,
    "followers_sum": 1_768_135,
    "followees_sum": 1_768_135,
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _normalise_key(name: str, value: str) -> str:
    key = value.strip().strip("/")
    if not key or any(part in {"", ".", ".."} for part in PurePosixPath(key).parts):
        raise ValueError(f"{name} must be a non-empty safe S3 object key")
    return key


@dataclass(frozen=True)
class CloudConfig:
    job_id: str
    aws_region: str
    job_table: str
    data_bucket: str
    input_key: str
    result_prefix_root: str
    result_pointer_key: str
    spark_master: str
    analysis_script: str

    def __post_init__(self) -> None:
        if JOB_ID_PATTERN.fullmatch(self.job_id) is None:
            raise ValueError("JOB_ID must be the backend-issued 32-character hexadecimal ID")
        if self.result_pointer_key.startswith(f"{self.result_prefix}/"):
            raise ValueError("RESULT_POINTER_KEY must be outside the immutable per-job prefix")

    @property
    def result_prefix(self) -> str:
        return f"{self.result_prefix_root}/{self.job_id}"

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None) -> "CloudConfig":
        values = os.environ if environ is None else environ

        def required(name: str) -> str:
            value = values.get(name, "").strip()
            if not value:
                raise ValueError(f"missing required environment variable {name}")
            return value

        job_id = required("JOB_ID")
        return cls(
            job_id=job_id,
            aws_region=values.get("AWS_REGION", "ap-southeast-2").strip()
            or "ap-southeast-2",
            job_table=required("AWS_JOB_TABLE"),
            data_bucket=required("DATA_BUCKET"),
            input_key=_normalise_key(
                "INPUT_KEY", values.get("INPUT_KEY", "input/twitter_combined.txt")
            ),
            result_prefix_root=_normalise_key(
                "RESULT_PREFIX_ROOT", values.get("RESULT_PREFIX_ROOT", "results/jobs")
            ),
            result_pointer_key=_normalise_key(
                "RESULT_POINTER_KEY", values.get("RESULT_POINTER_KEY", "results/latest.json")
            ),
            spark_master=values.get("SPARK_MASTER", "local[*]").strip() or "local[*]",
            analysis_script=values.get(
                "ANALYSIS_SCRIPT", str(Path(__file__).with_name("main.py"))
            ),
        )


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_and_validate_stats(path: Path) -> dict[str, int]:
    with path.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError("analysis stats must be a JSON object")

    stats: dict[str, int] = {}
    for key, expected in EXPECTED_STATS.items():
        value = raw.get(key)
        if type(value) is not int:
            raise ValueError(f"analysis stat {key} must be an integer")
        stats[key] = value
        if value != expected:
            raise ValueError(f"analysis stat {key}={value}, expected {expected}")
    return stats


def _validate_result_parts(result_dir: Path, stats: Mapping[str, int]) -> list[dict[str, Any]]:
    paths = sorted(path for path in result_dir.glob("part-*.csv") if path.is_file())
    if not paths:
        raise ValueError("analysis produced no part-*.csv files")

    seen_users: set[int] = set()
    follower_sum = 0
    followee_sum = 0
    parts: list[dict[str, Any]] = []
    for path in paths:
        rows = 0
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != EXPECTED_HEADER:
                raise ValueError(f"unexpected CSV header in {path.name}: {reader.fieldnames}")
            for record in reader:
                try:
                    user_id = int(record["user_id"])
                    followers = int(record["followers"])
                    followees = int(record["followees"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(f"invalid CSV record in {path.name}") from exc
                if user_id < 0 or followers < 0 or followees < 0:
                    raise ValueError(f"negative CSV value in {path.name}")
                if user_id in seen_users:
                    raise ValueError(f"duplicate user_id {user_id} across result parts")
                seen_users.add(user_id)
                follower_sum += followers
                followee_sum += followees
                rows += 1
        parts.append(
            {
                "name": path.name,
                "path": path,
                "rows": rows,
                "size": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )

    if len(seen_users) != stats["users"]:
        raise ValueError(
            f"result row count {len(seen_users)} does not match users={stats['users']}"
        )
    if follower_sum != stats["distinct_non_self_edges"]:
        raise ValueError(
            f"CSV follower sum {follower_sum} does not match distinct edge count"
        )
    if followee_sum != stats["distinct_non_self_edges"]:
        raise ValueError(
            f"CSV followee sum {followee_sum} does not match distinct edge count"
        )
    return parts


def _put_immutable(
    s3_client: Any,
    *,
    bucket: str,
    key: str,
    body: Any,
    content_type: str,
) -> str:
    response = s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        CacheControl="public,max-age=31536000,immutable",
        IfNoneMatch="*",
    )
    return str(response.get("ETag", "")).strip('"')


def _assert_running(table: Any, config: CloudConfig, timestamp: str) -> None:
    """Confirm backend dispatch owns this running job without changing its state."""
    table.update_item(
        Key={"job_id": config.job_id},
        UpdateExpression=("SET runner_started_at = :now, updated_at = :now REMOVE runner_error"),
        ConditionExpression="#status = :running",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":running": "running", ":now": timestamp},
    )


def _record_validated(
    table: Any,
    config: CloudConfig,
    timestamp: str,
    stats: Mapping[str, int],
    pointer: Mapping[str, Any],
) -> None:
    table.update_item(
        Key={"job_id": config.job_id},
        UpdateExpression=(
            "SET analysis_validated_at = :now, updated_at = :now, stats = :stats, "
            "result_prefix = :prefix, result_pointer_key = :pointer, manifest_key = :manifest "
            "REMOVE runner_error"
        ),
        ConditionExpression="#status = :running",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":running": "running",
            ":now": timestamp,
            ":stats": dict(stats),
            ":prefix": str(pointer["result_prefix"]),
            ":pointer": config.result_pointer_key,
            ":manifest": str(pointer["manifest_key"]),
        },
    )


def _record_runner_error(table: Any, config: CloudConfig, timestamp: str, error: str) -> None:
    """Leave terminal state and active-lock release to backend reconciliation."""
    table.update_item(
        Key={"job_id": config.job_id},
        UpdateExpression=(
            "SET analysis_failed_at = :now, updated_at = :now, runner_error = :error"
        ),
        ConditionExpression="#status = :running",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":running": "running",
            ":now": timestamp,
            ":error": error[:1000],
        },
    )


def _analysis_command(config: CloudConfig, result_dir: Path, stats_path: Path) -> list[str]:
    return [
        sys.executable,
        config.analysis_script,
        "--input",
        f"s3a://{config.data_bucket}/{config.input_key}",
        "--output",
        str(result_dir),
        "--stats-output",
        str(stats_path),
        "--master",
        config.spark_master,
    ]


def _spark_environment(config: CloudConfig) -> dict[str, str]:
    environment = dict(os.environ)
    environment["PYSPARK_SUBMIT_ARGS"] = " ".join(
        [
            "--packages",
            HADOOP_AWS_PACKAGE,
            "--conf",
            f"spark.hadoop.fs.s3a.aws.credentials.provider={S3A_CREDENTIALS_PROVIDER}",
            "--conf",
            f"spark.hadoop.fs.s3a.endpoint.region={config.aws_region}",
            "pyspark-shell",
        ]
    )
    return environment


def run_cloud_job(
    config: CloudConfig,
    *,
    s3_client: Any | None = None,
    job_table: Any | None = None,
    run_process: Callable[..., Any] = subprocess.run,
    clock: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Execute one job. Incomplete uploads never receive a success marker or pointer."""
    if s3_client is None or job_table is None:
        import boto3

        session = boto3.Session(region_name=config.aws_region)
        if s3_client is None:
            s3_client = session.client("s3")
        if job_table is None:
            job_table = session.resource("dynamodb").Table(config.job_table)

    _assert_running(job_table, config, clock())
    pointer_published = False
    try:
        with tempfile.TemporaryDirectory(prefix=f"analysis-{config.job_id}-") as directory:
            work_dir = Path(directory)
            input_path = work_dir / "twitter_combined.txt"
            result_dir = work_dir / "results"
            stats_path = work_dir / "stats.json"

            input_head = s3_client.head_object(Bucket=config.data_bucket, Key=config.input_key)
            download_args: dict[str, str] = {}
            if input_head.get("VersionId"):
                download_args["VersionId"] = str(input_head["VersionId"])
            download_kwargs: dict[str, Any] = {}
            if download_args:
                download_kwargs["ExtraArgs"] = download_args
            s3_client.download_file(
                config.data_bucket, config.input_key, str(input_path), **download_kwargs
            )
            input_size = input_path.stat().st_size
            expected_size = int(input_head.get("ContentLength", input_size))
            if input_size != expected_size:
                raise ValueError(
                    f"downloaded input size {input_size} does not match S3 size {expected_size}"
                )

            completed = run_process(
                _analysis_command(config, result_dir, stats_path),
                check=False,
                env=_spark_environment(config),
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"PySpark analysis exited with status {completed.returncode}"
                )

            input_after = s3_client.head_object(
                Bucket=config.data_bucket, Key=config.input_key
            )
            for field in ("VersionId", "ETag", "ContentLength"):
                if input_head.get(field) != input_after.get(field):
                    raise RuntimeError(
                        f"S3 input changed during analysis ({field}); refusing publication"
                    )

            stats = _load_and_validate_stats(stats_path)
            local_parts = _validate_result_parts(result_dir, stats)
            published_parts: list[dict[str, Any]] = []
            for part in local_parts:
                key = f"{config.result_prefix}/{part['name']}"
                with part["path"].open("rb") as body:
                    etag = _put_immutable(
                        s3_client,
                        bucket=config.data_bucket,
                        key=key,
                        body=body,
                        content_type="text/csv",
                    )
                published_parts.append(
                    {
                        "key": key,
                        "rows": part["rows"],
                        "size": part["size"],
                        "sha256": part["sha256"],
                        "etag": etag,
                    }
                )

            stats_key = f"{config.result_prefix}/stats.json"
            stats_body = _canonical_json(stats)
            _put_immutable(
                s3_client,
                bucket=config.data_bucket,
                key=stats_key,
                body=stats_body,
                content_type="application/json",
            )

            success_key = f"{config.result_prefix}/_SUCCESS"
            manifest_key = f"{config.result_prefix}/manifest.json"
            completed_at = clock()
            input_record: dict[str, Any] = {
                "bucket": config.data_bucket,
                "key": config.input_key,
                "etag": str(input_head.get("ETag", "")).strip('"'),
                "size": input_size,
                "sha256": _sha256_file(input_path),
            }
            if input_head.get("VersionId"):
                input_record["version_id"] = str(input_head["VersionId"])
            manifest: dict[str, Any] = {
                "version": 1,
                "job_id": config.job_id,
                "bucket": config.data_bucket,
                "prefix": config.result_prefix,
                "created_at": completed_at,
                "input": input_record,
                "users": stats["users"],
                "distinct_non_self_edges": stats["distinct_non_self_edges"],
                "followers_sum": stats["followers_sum"],
                "followees_sum": stats["followees_sum"],
                "parts": published_parts,
                "stats_key": stats_key,
                "stats_sha256": hashlib.sha256(stats_body).hexdigest(),
                "success_key": success_key,
            }
            _put_immutable(
                s3_client,
                bucket=config.data_bucket,
                key=manifest_key,
                body=_canonical_json(manifest),
                content_type="application/json",
            )

            _put_immutable(
                s3_client,
                bucket=config.data_bucket,
                key=success_key,
                body=b"",
                content_type="application/octet-stream",
            )

            pointer: dict[str, Any] = {
                "version": 1,
                "job_id": config.job_id,
                "bucket": config.data_bucket,
                "prefix": config.result_prefix,
                "result_prefix": config.result_prefix,
                "manifest_key": manifest_key,
                "stats_key": stats_key,
                "success_key": success_key,
                "users": stats["users"],
                "completed_at": completed_at,
            }
            _record_validated(job_table, config, clock(), stats, pointer)
            s3_client.put_object(
                Bucket=config.data_bucket,
                Key=config.result_pointer_key,
                Body=_canonical_json(pointer),
                ContentType="application/json",
                CacheControl="no-store",
            )
            pointer_published = True
            LOGGER.info(
                "Cloud analysis job %s published; awaiting backend validation", config.job_id
            )
            return pointer
    except Exception as exc:
        if not pointer_published:
            try:
                _record_runner_error(job_table, config, clock(), str(exc))
            except Exception:
                LOGGER.exception("Could not annotate cloud analysis job %s error", config.job_id)
        raise


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_cloud_job(CloudConfig.from_environ())
    except Exception:
        LOGGER.exception("Cloud analysis job failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
