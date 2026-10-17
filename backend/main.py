"""FastAPI service for precomputed Twitter follower statistics."""

from __future__ import annotations

import os
import secrets
import sqlite3
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any
from typing import AsyncIterator, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.cloud_jobs import (
    DEFAULT_AWS_REGION,
    CloudDispatchError,
    DynamoDBJobStore,
    ECSFargateOrchestrator,
)
from backend.job_store import JobNotFoundError, JobStore
from backend.load_results import DEFAULT_DB_PATH, DEFAULT_RESULT_DIR, load_results
from backend.s3_results import load_results_from_s3


SQLITE_INTEGER_MAX = 2**63 - 1


class UserCounts(BaseModel):
    user_id: int
    followers: int
    followees: int


class UserPage(BaseModel):
    items: list[UserCounts]
    total: int
    limit: int
    offset: int


class Health(BaseModel):
    status: str
    users: int


class AnalysisJob(BaseModel):
    job_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    created_at: str
    started_at: str | None
    finished_at: str | None
    error: str | None
    stats: dict[str, int] | None


class AnalysisSubmission(AnalysisJob):
    duplicate: bool


@contextmanager
def _connection(database_path: Path) -> Iterator[sqlite3.Connection]:
    if not database_path.is_file():
        raise HTTPException(status_code=503, detail="Results database is not available")
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()


def create_app(
    db_path: str | Path | None = None,
    result_dir: str | Path | None = None,
    *,
    load_on_startup: bool = False,
    result_s3_uri: str | None = None,
    origin_verify_secret: str | None = None,
    job_dir: str | Path | None = None,
    analysis_api_token: str | None = None,
    job_backend: str | None = None,
    aws_region: str | None = None,
    aws_job_table: str | None = None,
    aws_ecs_cluster: str | None = None,
    aws_ecs_task_definition: str | None = None,
    aws_ecs_container_name: str | None = None,
    aws_ecs_subnet_id: str | None = None,
    aws_ecs_security_group_id: str | None = None,
    data_bucket: str | None = None,
    result_pointer_key: str | None = None,
    dynamodb_client: Any | None = None,
    ecs_client: Any | None = None,
    s3_client: Any | None = None,
) -> FastAPI:
    database_path = Path(db_path or os.environ.get("DB_PATH", DEFAULT_DB_PATH))
    results_path = Path(result_dir or os.environ.get("RESULT_DIR", DEFAULT_RESULT_DIR))
    s3_uri = result_s3_uri or os.environ.get("RESULT_S3_URI")
    origin_secret = origin_verify_secret or os.environ.get("ORIGIN_VERIFY_SECRET")
    job_token = analysis_api_token or os.environ.get("ANALYSIS_API_TOKEN")
    selected_job_backend = (job_backend or os.environ.get("JOB_BACKEND", "local")).lower()
    orchestrator: ECSFargateOrchestrator | None = None
    if selected_job_backend == "aws":
        region = aws_region or os.environ.get("AWS_REGION", DEFAULT_AWS_REGION)
        table_name = aws_job_table or os.environ.get("AWS_JOB_TABLE", "")
        jobs: JobStore | DynamoDBJobStore = DynamoDBJobStore(
            table_name,
            region=region,
            client=dynamodb_client,
        )
        orchestrator = ECSFargateOrchestrator(
            jobs,
            cluster=aws_ecs_cluster or os.environ.get("AWS_ECS_CLUSTER", ""),
            task_definition=(
                aws_ecs_task_definition
                or os.environ.get("AWS_ECS_TASK_DEFINITION", "")
            ),
            container_name=(
                aws_ecs_container_name
                or os.environ.get("AWS_ECS_CONTAINER_NAME", "analysis")
            ),
            subnet_id=aws_ecs_subnet_id or os.environ.get("AWS_ECS_SUBNET_ID", ""),
            security_group_id=(
                aws_ecs_security_group_id
                or os.environ.get("AWS_ECS_SECURITY_GROUP_ID", "")
            ),
            data_bucket=data_bucket or os.environ.get("DATA_BUCKET", ""),
            result_pointer_key=(
                result_pointer_key or os.environ.get("RESULT_POINTER_KEY", "")
            ),
            db_path=str(database_path),
            region=region,
            ecs_client=ecs_client,
            s3_client=s3_client,
        )
    elif selected_job_backend == "local":
        jobs = JobStore(job_dir or os.environ.get("JOB_DIR", "/data/state/jobs"))
    else:
        raise ValueError("JOB_BACKEND must be 'local' or 'aws'")

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if orchestrator is not None:
            orchestrator.restore_on_startup()
        elif load_on_startup:
            if s3_uri:
                load_results_from_s3(s3_uri, database_path)
            else:
                load_results(results_path, database_path)
        yield

    application = FastAPI(title="Twitter follower counts", lifespan=lifespan)

    @application.middleware("http")
    async def verify_cloudfront_origin(request: Request, call_next):
        if (
            origin_secret
            and request.url.path.startswith("/api/")
            and request.url.path != "/api/health"
        ):
            supplied = request.headers.get("x-origin-verify", "")
            if not secrets.compare_digest(supplied, origin_secret):
                return JSONResponse(status_code=403, content={"detail": "Forbidden"})
        return await call_next(request)

    @application.get("/api/health", response_model=Health)
    def health() -> Health:
        if not database_path.is_file():
            return Health(status="ok", users=0)
        try:
            with _connection(database_path) as connection:
                total = connection.execute("SELECT COUNT(*) FROM user_counts").fetchone()[0]
        except sqlite3.Error as exc:
            if "no such table" in str(exc).lower():
                return Health(status="ok", users=0)
            raise HTTPException(status_code=503, detail="Results database is not ready") from exc
        return Health(status="ok", users=total)

    @application.post("/api/analysis/jobs", response_model=AnalysisSubmission)
    def start_analysis(request: Request, response: Response) -> AnalysisSubmission:
        if not job_token:
            raise HTTPException(status_code=503, detail="Analysis job submission is disabled")
        supplied = request.headers.get("x-analysis-token", "")
        if not secrets.compare_digest(supplied, job_token):
            raise HTTPException(status_code=403, detail="Invalid analysis token")
        try:
            if orchestrator is not None:
                job, duplicate = orchestrator.submit()
            else:
                job, duplicate = jobs.create()
        except CloudDispatchError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        response.status_code = status.HTTP_200_OK if duplicate else status.HTTP_202_ACCEPTED
        return AnalysisSubmission(**job, duplicate=duplicate)

    @application.get("/api/analysis/jobs/latest", response_model=AnalysisJob)
    def latest_analysis() -> AnalysisJob:
        job = jobs.latest()
        if job is None:
            raise HTTPException(status_code=404, detail="No analysis jobs have been submitted")
        if orchestrator is not None:
            job = orchestrator.reconcile(str(job["job_id"]))
        return AnalysisJob(**job)

    @application.get("/api/analysis/jobs/{job_id}", response_model=AnalysisJob)
    def analysis_status(job_id: str) -> AnalysisJob:
        try:
            job = (
                orchestrator.reconcile(job_id)
                if orchestrator is not None
                else jobs.get(job_id)
            )
            return AnalysisJob(**job)
        except JobNotFoundError as exc:
            raise HTTPException(status_code=404, detail=f"Analysis job {job_id} not found") from exc

    @application.get("/api/users/{user_id}", response_model=UserCounts)
    def user(user_id: int) -> UserCounts:
        if user_id < 0 or user_id > SQLITE_INTEGER_MAX:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")
        try:
            with _connection(database_path) as connection:
                row = connection.execute(
                    "SELECT user_id, followers, followees FROM user_counts WHERE user_id = ?",
                    (user_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            raise HTTPException(
                status_code=503, detail="Results database is not ready"
            ) from exc
        if row is None:
            raise HTTPException(status_code=404, detail=f"User {user_id} not found")
        return UserCounts(**dict(row))

    @application.get("/api/users", response_model=UserPage)
    def users(
        limit: int = Query(default=50, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> UserPage:
        try:
            with _connection(database_path) as connection:
                total = connection.execute("SELECT COUNT(*) FROM user_counts").fetchone()[0]
                rows = connection.execute(
                    """
                    SELECT user_id, followers, followees
                    FROM user_counts
                    ORDER BY user_id ASC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                ).fetchall()
        except sqlite3.Error as exc:
            raise HTTPException(
                status_code=503, detail="Results database is not ready"
            ) from exc
        return UserPage(
            items=[UserCounts(**dict(row)) for row in rows],
            total=total,
            limit=limit,
            offset=offset,
        )

    return application


app = create_app()
