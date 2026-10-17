import unittest
from unittest.mock import patch

from backend.cloud_jobs import (
    ACTIVE_KEY,
    LATEST_KEY,
    DynamoDBJobStore,
    ECSFargateOrchestrator,
    _decode_job,
    _encode_job,
)
from backend.load_results import ResultImportError
from backend.s3_results import CompletedS3Result


class FakeAwsError(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class FakeDynamoDB:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    @staticmethod
    def _item_key(item: dict) -> str:
        return item["job_id"]["S"]

    def get_item(self, **kwargs):
        key = kwargs["Key"]["job_id"]["S"]
        return {"Item": self.items[key]} if key in self.items else {}

    def transact_write_items(self, **kwargs):
        actions = kwargs["TransactItems"]
        if any(
            action.get("Put", {}).get("Item", {}).get("job_id", {}).get("S") == ACTIVE_KEY
            for action in actions
        ) and ACTIVE_KEY in self.items:
            raise FakeAwsError("TransactionCanceledException")

        for action in actions:
            if "Update" in action:
                key = action["Update"]["Key"]["job_id"]["S"]
                current = _decode_job(self.items[key])
                if current["status"] not in {"queued", "running"}:
                    raise FakeAwsError("TransactionCanceledException")
            if "Delete" in action:
                expected = action["Delete"]["ExpressionAttributeValues"][":job_id"]["S"]
                active = self.items.get(ACTIVE_KEY)
                if active is None or active["active_job_id"]["S"] != expected:
                    raise FakeAwsError("TransactionCanceledException")

        for action in actions:
            if "Put" in action:
                item = action["Put"]["Item"]
                self.items[self._item_key(item)] = item
            elif "Update" in action:
                update = action["Update"]
                key = update["Key"]["job_id"]["S"]
                current = _decode_job(self.items[key])
                values = update["ExpressionAttributeValues"]
                stats_value = values[":stats"]
                stats = None
                if "M" in stats_value:
                    stats = {
                        name: int(value["N"])
                        for name, value in stats_value["M"].items()
                    }
                error_value = values[":error"]
                current.update(
                    status=values[":terminal"]["S"],
                    finished_at=values[":finished"]["S"],
                    error=error_value.get("S"),
                    stats=stats,
                )
                self.items[key] = _encode_job(current)
            elif "Delete" in action:
                self.items.pop(action["Delete"]["Key"]["job_id"]["S"], None)
        return {}

    def update_item(self, **kwargs):
        key = kwargs["Key"]["job_id"]["S"]
        item = self.items[key]
        current = _decode_job(item)
        values = kwargs["ExpressionAttributeValues"]
        if ":started" in values:
            if current["status"] != "queued":
                raise FakeAwsError("ConditionalCheckFailedException")
            current.update(
                status="running",
                started_at=values[":started"]["S"],
                ecs_task_arn=None,
                finished_at=None,
                error=None,
            )
        else:
            if current["status"] != "running" or current.get(
                "ecs_task_arn"
            ):
                raise FakeAwsError("ConditionalCheckFailedException")
            current["ecs_task_arn"] = values[":task"]["S"]
        updated = _encode_job(current)
        self.items[key] = updated
        return {"Attributes": updated}

    def delete_item(self, **kwargs):
        key = kwargs["Key"]["job_id"]["S"]
        expected = kwargs["ExpressionAttributeValues"][":job_id"]["S"]
        active = self.items.get(key)
        if active is None or active["active_job_id"]["S"] != expected:
            raise FakeAwsError("ConditionalCheckFailedException")
        del self.items[key]
        return {}

    def put_item(self, **kwargs):
        item = kwargs["Item"]
        key = self._item_key(item)
        if _decode_job(self.items[key])["status"] != "succeeded":
            raise FakeAwsError("ConditionalCheckFailedException")
        self.items[key] = item
        return {}


class FakeECS:
    def __init__(self, status: str = "RUNNING", exit_code: int | None = None) -> None:
        self.status = status
        self.exit_code = exit_code
        self.missing = False
        self.started: list[dict] = []
        self.on_run = None

    def run_task(self, **kwargs):
        if self.on_run is not None:
            self.on_run()
        self.started.append(kwargs)
        return {
            "tasks": [
                {
                    "taskArn": (
                        "arn:aws:ecs:ap-southeast-2:123456789012:"
                        "task/analysis-cluster/task-123"
                    )
                }
            ],
            "failures": [],
        }

    def describe_tasks(self, **kwargs):
        if self.missing:
            return {"tasks": [], "failures": [{"reason": "MISSING"}]}
        container = {"name": "analysis"}
        task = {
            "taskArn": kwargs["tasks"][0],
            "lastStatus": self.status,
            "containers": [container],
        }
        if self.status == "STOPPED":
            container["exitCode"] = 0 if self.exit_code is None else self.exit_code
            if container["exitCode"] != 0:
                container["reason"] = "analysis container exited 1"
        return {"tasks": [task], "failures": []}


def orchestrator(store: DynamoDBJobStore, ecs: FakeECS) -> ECSFargateOrchestrator:
    return ECSFargateOrchestrator(
        store,
        cluster="analysis-cluster",
        task_definition="analysis-task:1",
        container_name="analysis",
        subnet_id="subnet-123",
        security_group_id="sg-123",
        data_bucket="results-bucket",
        result_pointer_key="results/latest.json",
        db_path="/tmp/test-users.db",
        ecs_client=ecs,
        s3_client=object(),
    )


class DynamoDBJobStoreTest(unittest.TestCase):
    def test_create_is_deduplicated_and_finish_atomically_releases_active(self) -> None:
        client = FakeDynamoDB()
        store = DynamoDBJobStore("jobs", client=client)

        first, duplicate = store.create()
        second, second_duplicate = store.create()
        running = store.mark_running(first["job_id"])
        running = store.attach_task(
            first["job_id"],
            "arn:aws:ecs:ap-southeast-2:123456789012:task/cluster/task-1",
        )
        completed = store.finish(first["job_id"], "failed", error="failure")

        self.assertFalse(duplicate)
        self.assertTrue(second_duplicate)
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(running["status"], "running")
        self.assertEqual(completed["status"], "failed")
        self.assertNotIn(ACTIVE_KEY, client.items)
        self.assertEqual(
            client.items[LATEST_KEY]["latest_job_id"]["S"], first["job_id"]
        )

    def test_create_repairs_stale_terminal_active_sentinel(self) -> None:
        client = FakeDynamoDB()
        store = DynamoDBJobStore("jobs", client=client)
        stale, _ = store.create()
        stale_item = _decode_job(client.items[stale["job_id"]])
        stale_item.update(status="failed", finished_at="2026-10-16T00:00:00Z")
        client.items[stale["job_id"]] = _encode_job(stale_item)

        replacement, duplicate = store.create()

        self.assertFalse(duplicate)
        self.assertNotEqual(replacement["job_id"], stale["job_id"])
        self.assertEqual(
            client.items[ACTIVE_KEY]["active_job_id"]["S"], replacement["job_id"]
        )

    def test_create_repairs_orphaned_active_sentinel(self) -> None:
        client = FakeDynamoDB()
        store = DynamoDBJobStore("jobs", client=client)
        orphaned, _ = store.create()
        del client.items[orphaned["job_id"]]

        replacement, duplicate = store.create()

        self.assertFalse(duplicate)
        self.assertNotEqual(replacement["job_id"], orphaned["job_id"])
        self.assertEqual(
            client.items[ACTIVE_KEY]["active_job_id"]["S"], replacement["job_id"]
        )


class ECSFargateOrchestratorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.dynamo = FakeDynamoDB()
        self.store = DynamoDBJobStore("jobs", client=self.dynamo)
        self.ecs = FakeECS()
        self.orchestrator = orchestrator(self.store, self.ecs)

    def test_submit_dispatches_expected_ecs_task_and_marks_running(self) -> None:
        def assert_running_before_ecs_starts():
            active = self.store.active()
            self.assertIsNotNone(active)
            self.assertEqual(active["status"], "running")
            self.assertIsNone(active["ecs_task_arn"])

        self.ecs.on_run = assert_running_before_ecs_starts
        job, duplicate = self.orchestrator.submit()

        self.assertFalse(duplicate)
        self.assertEqual(job["status"], "running")
        self.assertTrue(job["ecs_task_arn"].endswith("/task-123"))
        request = self.ecs.started[0]
        self.assertEqual(request["cluster"], "analysis-cluster")
        self.assertEqual(request["taskDefinition"], "analysis-task:1")
        self.assertEqual(request["launchType"], "FARGATE")
        self.assertEqual(request["clientToken"], job["job_id"])
        self.assertEqual(
            request["networkConfiguration"],
            {
                "awsvpcConfiguration": {
                    "subnets": ["subnet-123"],
                    "securityGroups": ["sg-123"],
                    "assignPublicIp": "ENABLED",
                }
            },
        )
        self.assertEqual(
            request["overrides"]["containerOverrides"][0]["environment"],
            [{"name": "JOB_ID", "value": job["job_id"]}],
        )

    def test_success_validates_import_before_terminal_transition(self) -> None:
        job, _ = self.orchestrator.submit()
        self.ecs.status = "STOPPED"
        self.ecs.exit_code = 0
        completed = CompletedS3Result(
            job_id=job["job_id"],
            imported_users=81306,
            stats={"users": 81306, "distinct_non_self_edges": 1768135},
            completed_at="2026-10-16T00:00:00Z",
        )
        def validate_before_finish(*_args, **_kwargs):
            self.assertEqual(self.store.get(job["job_id"])["status"], "running")
            self.assertIn(ACTIVE_KEY, self.dynamo.items)
            return completed

        with patch(
            "backend.cloud_jobs.load_latest_results_from_s3",
            side_effect=validate_before_finish,
        ) as loader:
            result = self.orchestrator.reconcile(job["job_id"])

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["stats"]["imported_users"], 81306)
        self.assertNotIn(ACTIVE_KEY, self.dynamo.items)
        loader.assert_called_once()

    def test_ecs_failure_releases_active_sentinel(self) -> None:
        job, _ = self.orchestrator.submit()
        self.ecs.status = "STOPPED"
        self.ecs.exit_code = 1

        result = self.orchestrator.reconcile(job["job_id"])

        self.assertEqual(result["status"], "failed")
        self.assertIn("analysis container exited 1", result["error"])
        self.assertNotIn(ACTIVE_KEY, self.dynamo.items)

    def test_expired_ecs_metadata_recovers_from_validated_s3_result(self) -> None:
        job, _ = self.orchestrator.submit()
        self.ecs.missing = True
        completed = CompletedS3Result(
            job_id=job["job_id"],
            imported_users=81306,
            stats={"users": 81306, "distinct_non_self_edges": 1768135},
            completed_at="2026-10-16T00:00:00Z",
        )

        with patch(
            "backend.cloud_jobs.load_latest_results_from_s3", return_value=completed
        ):
            result = self.orchestrator.reconcile(job["job_id"])

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["stats"]["imported_users"], 81306)
        self.assertNotIn(ACTIVE_KEY, self.dynamo.items)

    def test_premature_succeeded_state_becomes_failed_if_import_is_invalid(self) -> None:
        job, _ = self.orchestrator.submit()
        item = _decode_job(self.dynamo.items[job["job_id"]])
        item.update(status="succeeded", finished_at="2026-10-16T00:00:00Z")
        self.dynamo.items[job["job_id"]] = _encode_job(item)

        with patch(
            "backend.cloud_jobs.load_latest_results_from_s3",
            side_effect=ResultImportError("manifest mismatch"),
        ):
            result = self.orchestrator.reconcile(job["job_id"])

        self.assertEqual(result["status"], "failed")
        self.assertIn("manifest mismatch", result["error"])
        self.assertNotIn(ACTIVE_KEY, self.dynamo.items)

    def test_startup_restores_latest_completed_snapshot(self) -> None:
        completed = CompletedS3Result(
            job_id="b" * 32,
            imported_users=2,
            stats={"users": 2},
            completed_at="2026-10-16T00:00:00Z",
        )
        with patch(
            "backend.cloud_jobs.load_latest_results_from_s3", return_value=completed
        ) as loader:
            self.orchestrator.restore_on_startup()

        self.assertEqual(self.orchestrator.loaded_job_id, "b" * 32)
        loader.assert_called_once()


if __name__ == "__main__":
    unittest.main()
