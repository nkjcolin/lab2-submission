"""Download published Spark CSV parts from S3 and load them into SQLite."""

from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from backend.load_results import ResultImportError, load_results


PART_NAME = re.compile(r"part-.*\.csv$")
JOB_ID = re.compile(r"[0-9a-f]{32}")
SHA256 = re.compile(r"[0-9a-f]{64}")
DEFAULT_AWS_REGION = "ap-southeast-2"


class S3ResultNotFound(ResultImportError):
    """Raised when no latest completed-result pointer has been published yet."""


@dataclass(frozen=True)
class CompletedS3Result:
    job_id: str
    imported_users: int
    stats: dict[str, int]
    completed_at: str


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ResultImportError(f"invalid S3 results URI: {uri}")
    prefix = parsed.path.lstrip("/").rstrip("/")
    if not prefix:
        raise ResultImportError("S3 results URI must include an object prefix")
    return parsed.netloc, prefix


def load_results_from_s3(
    s3_uri: str,
    db_path: str | Path,
    *,
    s3_client: Any | None = None,
) -> int:
    """Download every Spark CSV part under ``s3_uri`` and import atomically."""
    bucket, prefix = parse_s3_uri(s3_uri)
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for item in page.get("Contents", []):
            key = item.get("Key", "")
            if PART_NAME.fullmatch(PurePosixPath(key).name):
                keys.append(key)

    keys.sort()
    if not keys:
        raise ResultImportError(f"no part-*.csv objects found at {s3_uri}")

    with tempfile.TemporaryDirectory(prefix="twitter-results-") as temporary_directory:
        result_dir = Path(temporary_directory)
        for index, key in enumerate(keys):
            destination = result_dir / f"part-{index:05d}.csv"
            s3_client.download_file(bucket, key, str(destination))
        return load_results(result_dir, db_path)


def _error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
    return code if isinstance(code, str) else None


def _object_bytes(
    client: Any,
    bucket: str,
    key: str,
    *,
    pointer: bool = False,
) -> bytes:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        value = body.read() if hasattr(body, "read") else body
        if not isinstance(value, bytes):
            raise TypeError("S3 object body is not bytes")
        return value
    except Exception as exc:
        if pointer and _error_code(exc) in {"NoSuchKey", "NotFound", "404"}:
            raise S3ResultNotFound(f"no completed result pointer at s3://{bucket}/{key}") from exc
        if isinstance(exc, (ResultImportError, S3ResultNotFound)):
            raise
        raise ResultImportError(f"could not read s3://{bucket}/{key}: {exc}") from exc


def _json_object(raw: bytes, description: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResultImportError(f"{description} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ResultImportError(f"{description} must be a JSON object")
    return value


def _required_string(value: dict[str, Any], field: str, description: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise ResultImportError(f"{description}.{field} must be a non-empty string")
    return result


def _required_integer(value: dict[str, Any], field: str, description: str) -> int:
    result = value.get(field)
    if isinstance(result, bool) or not isinstance(result, int) or result < 0:
        raise ResultImportError(f"{description}.{field} must be a non-negative integer")
    return result


def _safe_prefix(prefix: str, job_id: str) -> bool:
    path = PurePosixPath(prefix)
    return (
        not prefix.startswith("/")
        and ".." not in path.parts
        and job_id in path.parts
        and prefix.rstrip("/") == prefix
    )


def _key_belongs_to_prefix(key: str, prefix: str) -> bool:
    return key.startswith(f"{prefix}/") and ".." not in PurePosixPath(key).parts


def load_latest_results_from_s3(
    bucket: str,
    pointer_key: str,
    db_path: str | Path,
    *,
    expected_job_id: str | None = None,
    s3_client: Any | None = None,
    region: str = DEFAULT_AWS_REGION,
) -> CompletedS3Result:
    """Validate an immutable completed-result manifest before replacing SQLite.

    The mutable latest pointer is used only to locate a versioned manifest. No
    prefix listing is trusted: every part must be explicitly named and hashed by
    the manifest produced by the completed analysis job.
    """
    if not bucket or not pointer_key or pointer_key.startswith("/"):
        raise ResultImportError("DATA_BUCKET and RESULT_POINTER_KEY must be valid")
    if s3_client is None:
        import boto3

        s3_client = boto3.client("s3", region_name=region)

    pointer = _json_object(
        _object_bytes(s3_client, bucket, pointer_key, pointer=True),
        "latest result pointer",
    )
    if pointer.get("version") != 1:
        raise ResultImportError("latest result pointer.version must be 1")
    job_id = _required_string(pointer, "job_id", "latest result pointer")
    if JOB_ID.fullmatch(job_id) is None:
        raise ResultImportError("latest result pointer.job_id is invalid")
    if expected_job_id is not None and job_id != expected_job_id:
        raise ResultImportError(
            f"latest result belongs to job {job_id}, expected {expected_job_id}"
        )
    pointer_bucket = _required_string(pointer, "bucket", "latest result pointer")
    if pointer_bucket != bucket:
        raise ResultImportError("latest result pointer references a different bucket")
    prefix = _required_string(pointer, "prefix", "latest result pointer")
    if not _safe_prefix(prefix, job_id):
        raise ResultImportError("latest result pointer.prefix is not a safe job prefix")
    completed_at = _required_string(pointer, "completed_at", "latest result pointer")
    pointer_users = _required_integer(pointer, "users", "latest result pointer")
    manifest_key = _required_string(pointer, "manifest_key", "latest result pointer")
    stats_key = _required_string(pointer, "stats_key", "latest result pointer")
    success_key = _required_string(pointer, "success_key", "latest result pointer")
    for key in (manifest_key, stats_key, success_key):
        if not _key_belongs_to_prefix(key, prefix):
            raise ResultImportError(f"result object {key} is outside the immutable job prefix")
    if PurePosixPath(manifest_key).name != "manifest.json":
        raise ResultImportError("latest result manifest key must end in manifest.json")
    if PurePosixPath(stats_key).name != "stats.json":
        raise ResultImportError("latest result stats key must end in stats.json")
    if PurePosixPath(success_key).name != "_SUCCESS":
        raise ResultImportError("latest result success key must end in _SUCCESS")

    manifest = _json_object(
        _object_bytes(s3_client, bucket, manifest_key), "completed result manifest"
    )
    if manifest.get("version") != 1:
        raise ResultImportError("completed result manifest.version must be 1")
    if _required_string(manifest, "job_id", "completed result manifest") != job_id:
        raise ResultImportError("manifest job_id does not match latest pointer")
    if _required_string(manifest, "bucket", "completed result manifest") != bucket:
        raise ResultImportError("manifest bucket does not match configured bucket")
    if _required_string(manifest, "prefix", "completed result manifest") != prefix:
        raise ResultImportError("manifest prefix does not match latest pointer")
    if _required_string(manifest, "stats_key", "completed result manifest") != stats_key:
        raise ResultImportError("manifest stats_key does not match latest pointer")
    if _required_string(manifest, "success_key", "completed result manifest") != success_key:
        raise ResultImportError("manifest success_key does not match latest pointer")
    _required_string(manifest, "created_at", "completed result manifest")
    input_record = manifest.get("input")
    if not isinstance(input_record, dict):
        raise ResultImportError("completed result manifest.input must be an object")
    if _required_string(input_record, "bucket", "manifest input") != bucket:
        raise ResultImportError("manifest input bucket does not match configured bucket")
    _required_string(input_record, "key", "manifest input")
    _required_string(input_record, "etag", "manifest input")
    _required_integer(input_record, "size", "manifest input")
    input_sha256 = _required_string(input_record, "sha256", "manifest input")
    if SHA256.fullmatch(input_sha256) is None:
        raise ResultImportError("manifest input.sha256 is invalid")

    users = _required_integer(manifest, "users", "completed result manifest")
    edges = _required_integer(
        manifest, "distinct_non_self_edges", "completed result manifest"
    )
    followers_sum = _required_integer(
        manifest, "followers_sum", "completed result manifest"
    )
    followees_sum = _required_integer(
        manifest, "followees_sum", "completed result manifest"
    )
    if users != pointer_users:
        raise ResultImportError("pointer and manifest user totals do not match")
    if followers_sum != edges or followees_sum != edges:
        raise ResultImportError("manifest edge-count invariants failed")

    raw_stats = _object_bytes(s3_client, bucket, stats_key)
    stats_sha256 = _required_string(manifest, "stats_sha256", "completed result manifest")
    if SHA256.fullmatch(stats_sha256) is None:
        raise ResultImportError("manifest stats_sha256 is invalid")
    if hashlib.sha256(raw_stats).hexdigest() != stats_sha256:
        raise ResultImportError("stats.json SHA-256 does not match the manifest")
    stats_json = _json_object(raw_stats, "completed result statistics")
    stats: dict[str, int] = {}
    for key, value in stats_json.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ResultImportError(f"completed result statistics.{key} must be an integer")
        stats[str(key)] = value
    required_stats = {
        "users": users,
        "distinct_non_self_edges": edges,
        "followers_sum": followers_sum,
        "followees_sum": followees_sum,
    }
    for key, expected in required_stats.items():
        if stats.get(key) != expected:
            raise ResultImportError(f"statistics {key} does not match the manifest")

    success = _object_bytes(s3_client, bucket, success_key)
    if success:
        raise ResultImportError("completed-result _SUCCESS marker must be empty")

    raw_parts = manifest.get("parts")
    if not isinstance(raw_parts, list) or not raw_parts:
        raise ResultImportError("completed result manifest.parts must be a non-empty list")
    part_keys: list[str] = []
    declared_rows = 0
    validated_parts: list[tuple[str, int, str]] = []
    for index, raw_part in enumerate(raw_parts):
        if not isinstance(raw_part, dict):
            raise ResultImportError(f"manifest part {index} must be an object")
        key = _required_string(raw_part, "key", f"manifest part {index}")
        size = _required_integer(raw_part, "size", f"manifest part {index}")
        rows = _required_integer(raw_part, "rows", f"manifest part {index}")
        sha256 = _required_string(raw_part, "sha256", f"manifest part {index}")
        _required_string(raw_part, "etag", f"manifest part {index}")
        if SHA256.fullmatch(sha256) is None:
            raise ResultImportError(f"manifest part {index}.sha256 is invalid")
        if not _key_belongs_to_prefix(key, prefix) or PART_NAME.fullmatch(
            PurePosixPath(key).name
        ) is None:
            raise ResultImportError(f"manifest part key is invalid: {key}")
        part_keys.append(key)
        declared_rows += rows
        validated_parts.append((key, size, sha256))
    if part_keys != sorted(part_keys) or len(set(part_keys)) != len(part_keys):
        raise ResultImportError("manifest part keys must be unique and lexicographically ordered")
    if declared_rows != users:
        raise ResultImportError("manifest part row totals do not match users")

    with tempfile.TemporaryDirectory(prefix="twitter-completed-result-") as temporary_directory:
        result_dir = Path(temporary_directory)
        for index, (key, expected_size, expected_sha256) in enumerate(validated_parts):
            destination = result_dir / f"part-{index:05d}.csv"
            try:
                s3_client.download_file(bucket, key, str(destination))
            except Exception as exc:
                raise ResultImportError(f"could not download s3://{bucket}/{key}: {exc}") from exc
            content = destination.read_bytes()
            if len(content) != expected_size:
                raise ResultImportError(f"downloaded size does not match manifest for {key}")
            if hashlib.sha256(content).hexdigest() != expected_sha256:
                raise ResultImportError(f"downloaded SHA-256 does not match manifest for {key}")
        imported = load_results(result_dir, db_path, expected_rows=users)

    return CompletedS3Result(
        job_id=job_id,
        imported_users=imported,
        stats=stats,
        completed_at=completed_at,
    )
