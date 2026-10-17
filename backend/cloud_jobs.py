"""Durable AWS analysis jobs and on-demand ECS Fargate orchestration."""

from __future__ import annotations

import uuid
from typing import Any

from backend.job_store import JobNotFoundError, utc_now
from backend.s3_results import (
    CompletedS3Result,
    S3ResultNotFound,
    load_latest_results_from_s3,
)


DEFAULT_AWS_REGION = "ap-southeast-2"
ACTIVE_KEY = "__active__"
LATEST_KEY = "__latest__"
ACTIVE_STATUSES = {"queued", "running"}
TERMINAL_STATUSES = {"succeeded", "failed"}
ECS_ACTIVE_STATUSES = {
    "PROVISIONING",
    "PENDING",
    "ACTIVATING",
    "RUNNING",
    "DEACTIVATING",
    "STOPPING",
    "DEPROVISIONING",
}


class CloudDispatchError(RuntimeError):
    """Raised when an analysis job cannot be submitted to ECS Fargate."""


def _attribute_string(item: dict[str, Any], name: str) -> str | None:
    value = item.get(name)
    if isinstance(value, dict) and isinstance(value.get("S"), str):
        return value["S"]
    return None


def _encode_job(job: dict[str, Any]) -> dict[str, Any]:
    item: dict[str, Any] = {
        "job_id": {"S": str(job["job_id"])},
        "record_type": {"S": "job"},
        "status": {"S": str(job["status"])},
        "created_at": {"S": str(job["created_at"])},
    }
    for field in ("started_at", "finished_at", "error", "ecs_task_arn", "runner_error"):
        value = job.get(field)
        if value is not None:
            item[field] = {"S": str(value)}
    stats = job.get("stats")
    if stats is not None:
        item["stats"] = {
            "M": {str(key): {"N": str(int(value))} for key, value in stats.items()}
        }
    return item


def _decode_job(item: dict[str, Any]) -> dict[str, Any]:
    job_id = _attribute_string(item, "job_id")
    status = _attribute_string(item, "status")
    created_at = _attribute_string(item, "created_at")
    if not job_id or status not in ACTIVE_STATUSES | TERMINAL_STATUSES or not created_at:
        raise ValueError("invalid DynamoDB analysis job item")

    stats_value = item.get("stats", {}).get("M")
    stats = None
    if isinstance(stats_value, dict):
        stats = {}
        for key, value in stats_value.items():
            if not isinstance(value, dict) or "N" not in value:
                raise ValueError("invalid DynamoDB job statistics")
            stats[str(key)] = int(value["N"])

    return {
        "job_id": job_id,
        "status": status,
        "created_at": created_at,
        "started_at": _attribute_string(item, "started_at"),
        "finished_at": _attribute_string(item, "finished_at"),
        "error": _attribute_string(item, "error"),
        "stats": stats,
        "ecs_task_arn": _attribute_string(item, "ecs_task_arn"),
        "runner_error": _attribute_string(item, "runner_error"),
    }


def _error_code(exc: Exception) -> str | None:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
    return code if isinstance(code, str) else None


class DynamoDBJobStore:
    """Job store using atomic sentinel records to enforce one active analysis."""

    def __init__(
        self,
        table_name: str,
        *,
        region: str = DEFAULT_AWS_REGION,
        client: Any | None = None,
    ) -> None:
        if not table_name:
            raise ValueError("AWS_JOB_TABLE must not be empty")
        self.table_name = table_name
        if client is None:
            import boto3

            client = boto3.client("dynamodb", region_name=region)
        self.client = client

    @staticmethod
    def _key(job_id: str) -> dict[str, dict[str, str]]:
        return {"job_id": {"S": job_id}}

    def _get_item(self, job_id: str) -> dict[str, Any] | None:
        response = self.client.get_item(
            TableName=self.table_name,
            Key=self._key(job_id),
            ConsistentRead=True,
        )
        item = response.get("Item")
        return item if isinstance(item, dict) else None

    def get(self, job_id: str) -> dict[str, Any]:
        if not job_id or job_id.startswith("__"):
            raise JobNotFoundError(job_id)
        item = self._get_item(job_id)
        if item is None:
            raise JobNotFoundError(job_id)
        return _decode_job(item)

    def active(self) -> dict[str, Any] | None:
        control = self._get_item(ACTIVE_KEY)
        if control is None:
            return None
        active_job_id = _attribute_string(control, "active_job_id")
        if not active_job_id:
            raise ValueError("invalid DynamoDB active-job sentinel")
        try:
            return self.get(active_job_id)
        except JobNotFoundError:
            return None

    def _active_reference(self) -> str | None:
        control = self._get_item(ACTIVE_KEY)
        if control is None:
            return None
        active_job_id = _attribute_string(control, "active_job_id")
        if not active_job_id:
            raise ValueError("invalid DynamoDB active-job sentinel")
        return active_job_id

    def latest(self) -> dict[str, Any] | None:
        control = self._get_item(LATEST_KEY)
        if control is None:
            return None
        latest_job_id = _attribute_string(control, "latest_job_id")
        if not latest_job_id:
            raise ValueError("invalid DynamoDB latest-job pointer")
        try:
            return self.get(latest_job_id)
        except JobNotFoundError:
            return None

    def release_active(self, job_id: str) -> None:
        """Remove a stale sentinel only when it still belongs to ``job_id``."""
        try:
            self.client.delete_item(
                TableName=self.table_name,
                Key=self._key(ACTIVE_KEY),
                ConditionExpression="active_job_id = :job_id",
                ExpressionAttributeValues={":job_id": {"S": job_id}},
            )
        except Exception as exc:
            if _error_code(exc) != "ConditionalCheckFailedException":
                raise

    def create(self) -> tuple[dict[str, Any], bool]:
        """Atomically create a job unless the active sentinel already exists."""
        for attempt in range(2):
            job_id = uuid.uuid4().hex
            job: dict[str, Any] = {
                "job_id": job_id,
                "status": "queued",
                "created_at": utc_now(),
                "started_at": None,
                "finished_at": None,
                "error": None,
                "stats": None,
                "ecs_task_arn": None,
            }
            active_item = {
                "job_id": {"S": ACTIVE_KEY},
                "record_type": {"S": "control"},
                "active_job_id": {"S": job_id},
            }
            latest_item = {
                "job_id": {"S": LATEST_KEY},
                "record_type": {"S": "control"},
                "latest_job_id": {"S": job_id},
            }
            try:
                self.client.transact_write_items(
                    ClientRequestToken=job_id,
                    TransactItems=[
                        {
                            "Put": {
                                "TableName": self.table_name,
                                "Item": _encode_job(job),
                                "ConditionExpression": "attribute_not_exists(job_id)",
                            }
                        },
                        {
                            "Put": {
                                "TableName": self.table_name,
                                "Item": active_item,
                                "ConditionExpression": "attribute_not_exists(job_id)",
                            }
                        },
                        {"Put": {"TableName": self.table_name, "Item": latest_item}},
                    ],
                )
                return job, False
            except Exception as exc:
                if _error_code(exc) != "TransactionCanceledException":
                    raise
                active_job_id = self._active_reference()
                if active_job_id is None:
                    raise
                try:
                    active = self.get(active_job_id)
                except JobNotFoundError:
                    self.release_active(active_job_id)
                    if attempt == 0:
                        continue
                    raise RuntimeError("could not replace orphaned active-job sentinel") from exc
                if active["status"] in ACTIVE_STATUSES:
                    return active, True
                self.release_active(str(active["job_id"]))
                if attempt == 1:
                    raise RuntimeError("could not replace stale active-job sentinel") from exc
        raise AssertionError("unreachable")

    def mark_running(self, job_id: str) -> dict[str, Any]:
        current = self.get(job_id)
        if current["status"] != "queued":
            return current
        now = utc_now()
        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key=self._key(job_id),
                UpdateExpression=(
                    "SET #status = :running, started_at = :started "
                    "REMOVE finished_at, #error, ecs_task_arn"
                ),
                ConditionExpression="#status = :queued",
                ExpressionAttributeNames={"#status": "status", "#error": "error"},
                ExpressionAttributeValues={
                    ":running": {"S": "running"},
                    ":queued": {"S": "queued"},
                    ":started": {"S": now},
                },
                ReturnValues="ALL_NEW",
            )
            return _decode_job(response["Attributes"])
        except Exception as exc:
            if _error_code(exc) == "ConditionalCheckFailedException":
                return self.get(job_id)
            raise

    def attach_task(self, job_id: str, task_arn: str) -> dict[str, Any]:
        if not task_arn:
            raise ValueError("ECS task ARN must not be empty")
        try:
            response = self.client.update_item(
                TableName=self.table_name,
                Key=self._key(job_id),
                UpdateExpression="SET ecs_task_arn = :task",
                ConditionExpression=(
                    "#status = :running AND attribute_not_exists(ecs_task_arn)"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":running": {"S": "running"},
                    ":task": {"S": task_arn},
                },
                ReturnValues="ALL_NEW",
            )
            return _decode_job(response["Attributes"])
        except Exception as exc:
            if _error_code(exc) == "ConditionalCheckFailedException":
                current = self.get(job_id)
                if current.get("ecs_task_arn") == task_arn:
                    return current
            raise

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
        current = self.get(job_id)
        if current["status"] in TERMINAL_STATUSES:
            self.release_active(job_id)
            return current

        completed = dict(current)
        completed.update(
            status=status,
            finished_at=utc_now(),
            error=error,
            stats=stats,
        )
        error_value = {"NULL": True} if error is None else {"S": error}
        stats_value: dict[str, Any]
        if stats is None:
            stats_value = {"NULL": True}
        else:
            stats_value = {
                "M": {str(key): {"N": str(int(value))} for key, value in stats.items()}
            }
        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": self.table_name,
                            "Key": self._key(job_id),
                            "UpdateExpression": (
                                "SET #status = :terminal, finished_at = :finished, "
                                "#error = :error, stats = :stats"
                            ),
                            "ConditionExpression": "#status IN (:queued, :running)",
                            "ExpressionAttributeNames": {
                                "#status": "status",
                                "#error": "error",
                            },
                            "ExpressionAttributeValues": {
                                ":queued": {"S": "queued"},
                                ":running": {"S": "running"},
                                ":terminal": {"S": status},
                                ":finished": {"S": str(completed["finished_at"])},
                                ":error": error_value,
                                ":stats": stats_value,
                            },
                        }
                    },
                    {
                        "Delete": {
                            "TableName": self.table_name,
                            "Key": self._key(ACTIVE_KEY),
                            "ConditionExpression": "active_job_id = :job_id",
                            "ExpressionAttributeValues": {":job_id": {"S": job_id}},
                        }
                    },
                ]
            )
            return completed
        except Exception as exc:
            if _error_code(exc) == "TransactionCanceledException":
                reread = self.get(job_id)
                if reread["status"] in TERMINAL_STATUSES:
                    return reread
            raise

    def fail_result_validation(self, job_id: str, error: str) -> dict[str, Any]:
        """Convert a prematurely succeeded job to failed, then clear its sentinel."""
        current = self.get(job_id)
        if current["status"] == "failed":
            self.release_active(job_id)
            return current
        if current["status"] != "succeeded":
            return self.finish(job_id, "failed", error=error)
        failed = dict(current)
        failed.update(status="failed", finished_at=utc_now(), error=error, stats=None)
        try:
            self.client.put_item(
                TableName=self.table_name,
                Item=_encode_job(failed),
                ConditionExpression="#status = :succeeded",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":succeeded": {"S": "succeeded"}},
            )
        except Exception as exc:
            if _error_code(exc) == "ConditionalCheckFailedException":
                reread = self.get(job_id)
                if reread["status"] == "failed":
                    self.release_active(job_id)
                    return reread
            raise
        self.release_active(job_id)
        return failed


class ECSFargateOrchestrator:
    """Dispatch jobs to ECS Fargate and reconcile durable state with S3 output."""

    def __init__(
        self,
        store: DynamoDBJobStore,
        *,
        cluster: str,
        task_definition: str,
        container_name: str,
        subnet_id: str,
        security_group_id: str,
        data_bucket: str,
        result_pointer_key: str,
        db_path: str,
        region: str = DEFAULT_AWS_REGION,
        ecs_client: Any | None = None,
        s3_client: Any | None = None,
    ) -> None:
        required = {
            "AWS_ECS_CLUSTER": cluster,
            "AWS_ECS_TASK_DEFINITION": task_definition,
            "AWS_ECS_CONTAINER_NAME": container_name,
            "AWS_ECS_SUBNET_ID": subnet_id,
            "AWS_ECS_SECURITY_GROUP_ID": security_group_id,
            "DATA_BUCKET": data_bucket,
            "RESULT_POINTER_KEY": result_pointer_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ValueError(f"missing AWS analysis configuration: {', '.join(missing)}")
        if ecs_client is None or s3_client is None:
            import boto3

            if ecs_client is None:
                ecs_client = boto3.client("ecs", region_name=region)
            if s3_client is None:
                s3_client = boto3.client("s3", region_name=region)
        self.store = store
        self.cluster = cluster
        self.task_definition = task_definition
        self.container_name = container_name
        self.subnet_id = subnet_id
        self.security_group_id = security_group_id
        self.data_bucket = data_bucket
        self.result_pointer_key = result_pointer_key
        self.db_path = db_path
        self.ecs_client = ecs_client
        self.s3_client = s3_client
        self.loaded_job_id: str | None = None

    def _dispatch(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = str(job["job_id"])
        try:
            self.store.mark_running(job_id)
            response = self.ecs_client.run_task(
                cluster=self.cluster,
                taskDefinition=self.task_definition,
                launchType="FARGATE",
                count=1,
                clientToken=job_id,
                startedBy=job_id,
                networkConfiguration={
                    "awsvpcConfiguration": {
                        "subnets": [self.subnet_id],
                        "securityGroups": [self.security_group_id],
                        "assignPublicIp": "ENABLED",
                    }
                },
                overrides={
                    "containerOverrides": [
                        {
                            "name": self.container_name,
                            "environment": [{"name": "JOB_ID", "value": job_id}],
                        }
                    ]
                },
            )
            failures = response.get("failures") or []
            tasks = response.get("tasks") or []
            if failures:
                detail = failures[0].get("reason") or failures[0].get("detail")
                raise CloudDispatchError(f"ECS RunTask failed: {detail or failures[0]}")
            task_arn = tasks[0].get("taskArn") if tasks else None
            if not isinstance(task_arn, str) or not task_arn:
                raise CloudDispatchError("ECS did not return a task ARN")
            return self.store.attach_task(job_id, task_arn)
        except Exception as exc:
            message = f"Could not start ECS Fargate task: {exc}"
            self.store.finish(job_id, "failed", error=message[:1000])
            if isinstance(exc, CloudDispatchError):
                raise
            raise CloudDispatchError(message) from exc

    def submit(self) -> tuple[dict[str, Any], bool]:
        job, duplicate = self.store.create()
        if job["status"] == "queued" and not job.get("ecs_task_arn"):
            job = self._dispatch(job)
        else:
            job = self.reconcile(str(job["job_id"]))
        return job, duplicate

    def _load_result(self, job_id: str) -> CompletedS3Result:
        completed = load_latest_results_from_s3(
            self.data_bucket,
            self.result_pointer_key,
            self.db_path,
            expected_job_id=job_id,
            s3_client=self.s3_client,
        )
        self.loaded_job_id = completed.job_id
        return completed

    def restore_latest(self) -> CompletedS3Result | None:
        try:
            completed = load_latest_results_from_s3(
                self.data_bucket,
                self.result_pointer_key,
                self.db_path,
                s3_client=self.s3_client,
            )
        except S3ResultNotFound:
            return None
        self.loaded_job_id = completed.job_id
        return completed

    def reconcile(self, job_id: str) -> dict[str, Any]:
        job = self.store.get(job_id)
        if job["status"] == "succeeded":
            try:
                if self.loaded_job_id != job_id:
                    self._load_result(job_id)
            except Exception as exc:
                return self.store.fail_result_validation(
                    job_id,
                    f"Completed S3 result validation failed: {exc}"[:1000],
                )
            self.store.release_active(job_id)
            return job
        if job["status"] == "failed":
            self.store.release_active(job_id)
            return job
        if job["status"] in ACTIVE_STATUSES and not job.get("ecs_task_arn"):
            return self._dispatch(job)

        task_arn = job.get("ecs_task_arn")
        if not task_arn:
            return self.store.finish(
                job_id, "failed", error="Active job has no ECS task ARN"
            )
        response = self.ecs_client.describe_tasks(
            cluster=self.cluster,
            tasks=[task_arn],
        )
        failures = response.get("failures") or []
        tasks = response.get("tasks") or []
        if failures or not tasks:
            # ECS retains stopped-task metadata for a limited time. A backend
            # replacement may reconcile later, so a fully validated same-job
            # S3 publication remains authoritative even after ECS forgets it.
            try:
                completed = self._load_result(job_id)
            except Exception:
                detail = failures[0].get("reason") if failures else "task was not found"
                return self.store.finish(
                    job_id, "failed", error=f"ECS task lookup failed: {detail}"[:1000]
                )
            stats = dict(completed.stats)
            stats["imported_users"] = completed.imported_users
            return self.store.finish(job_id, "succeeded", stats=stats)
        task = tasks[0]
        task_status = task.get("lastStatus")
        if task_status in ECS_ACTIVE_STATUSES:
            return job
        if task_status == "STOPPED":
            containers = task.get("containers") or []
            container = next(
                (item for item in containers if item.get("name") == self.container_name),
                containers[0] if containers else {},
            )
            exit_code = container.get("exitCode")
            if exit_code != 0:
                detail = (
                    job.get("runner_error")
                    or container.get("reason")
                    or task.get("stoppedReason")
                    or f"container exit code {exit_code}"
                )
                return self.store.finish(
                    job_id, "failed", error=f"ECS analysis task failed: {detail}"[:1000]
                )
            try:
                completed = self._load_result(job_id)
            except Exception as exc:
                return self.store.finish(
                    job_id,
                    "failed",
                    error=f"Completed S3 result validation failed: {exc}"[:1000],
                )
            stats = dict(completed.stats)
            stats["imported_users"] = completed.imported_users
            return self.store.finish(job_id, "succeeded", stats=stats)
        return self.store.finish(
            job_id,
            "failed",
            error=f"Unexpected ECS task status: {task_status}"[:1000],
        )

    def restore_on_startup(self) -> None:
        active = self.store.active()
        if active is not None:
            self.reconcile(str(active["job_id"]))
        if self.loaded_job_id is None:
            self.restore_latest()
