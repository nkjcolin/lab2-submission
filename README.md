# Twitter follower analysis app

This Part 2 application keeps React, FastAPI, and PySpark in separate services.
The web application starts immediately, even with no results. PySpark starts
only when **Run analysis** is selected, publishes cleaned counts atomically,
and stops again after the job. FastAPI serves the last valid SQLite snapshot.

A dataset line `A B` means user `A` follows user `B`. Analysis rejects malformed
rows, removes self-follows, deduplicates ordered relationships, and treats the
reverse relationship as a separate edge.

## Local architecture

```text
browser -> frontend (Nginx + React) -> backend (FastAPI + SQLite)
                                        |
                                        v
                              persistent JSON job queue
                                        |
                                        v
                          analysis worker -> PySpark child process
                                        |
                              atomic CSV + SQLite publication
```

- `frontend` exposes port 8080 and proxies relative `/api` requests.
- `backend` exposes port 8000 for local API inspection. It never invokes Docker
  and never starts Spark.
- `analysis` has no published port. Its idle Python worker polls the state
  directory; no Spark JVM exists between jobs.
- `data/state/jobs/` stores job JSON, Spark logs, and statistics across restarts.
- `data/results/published/` stores the latest complete Spark CSV result.
- `data/state/users.db` stores the latest successfully imported result.

The backend and frontend stay healthy when these result files do not exist.
User lookup returns 503 until the first successful analysis, while
`GET /api/health` returns `{"status":"ok","users":0}`.

## Configuration

Copy `.env.example` to `.env` if `.env` is absent. The checked local file uses
safe development values.

| Variable | Purpose | Default |
| --- | --- | --- |
| `UPLOAD_DIR` | Directory containing `twitter_combined.txt` | `./upload` |
| `RESULTS_DIR` | Persistent Spark output root | `./data/results` |
| `STATE_DIR` | Persistent SQLite and job-state root | `./data/state` |
| `FRONTEND_PORT` | Nginx/React port | `8080` |
| `BACKEND_PORT` | Direct local FastAPI port | `8000` |
| `SPARK_MASTER` | Master for each requested Spark process | `local[*]` |
| `JOB_POLL_SECONDS` | Idle worker polling interval | `1` |
| `ANALYSIS_API_TOKEN` | Local protection for job submission | `local-development-only` |

The browser never receives `ANALYSIS_API_TOKEN`. Nginx injects it only for
`POST /api/analysis/jobs`. If the stack is reachable from another machine,
replace the default in `.env`.

## One-command local startup

Docker Desktop must be running and `upload/twitter_combined.txt` must exist.
From the repository root:

```bash
docker compose config
docker compose up --build -d
docker compose ps
```

This builds and starts all three services. It does **not** run analysis. Open
`http://localhost:8080`; the page, job control, and API are available even if
the results directory and SQLite database are empty.

Select **Run analysis** in the page. The button disables while the job is
queued or running. The page polls the persistent job record, shows `queued`,
`running`, `succeeded`, or `failed`, and refreshes reference users, lookup, and
pagination after success.

Inspect the worker and stop the stack without deleting persistent data:

```bash
docker compose logs --follow analysis backend frontend
docker compose down
```

## APIs

Existing result APIs:

- `GET /api/health`
- `GET /api/users/214328887`
- `GET /api/users/107830991`
- `GET /api/users?limit=50&offset=0`

Persistent analysis APIs:

- `POST /api/analysis/jobs` — protected; returns immediately with a job ID
- `GET /api/analysis/jobs/latest`
- `GET /api/analysis/jobs/{job_id}`

The frontend proxy adds the token automatically. For a direct backend request:

```bash
set -a
source .env
set +a

curl --fail-with-body -X POST \
  -H "X-Analysis-Token: $ANALYSIS_API_TOKEN" \
  http://localhost:8000/api/analysis/jobs
```

If a job is already queued or running, another POST returns the same job ID
with `"duplicate":true`; it does not enqueue another Spark run.

## Failure and restart behavior

- CSV output is published only after Spark succeeds.
- SQLite import validates every `part-*.csv` row inside one transaction. An
  invalid file, duplicate user ID, or row-count mismatch rolls back and keeps
  the previous `user_counts` table.
- A failed Spark child never invokes the SQLite loader.
- Queued, succeeded, and failed job records survive restarts.
- If the worker container stops during a `running` job, its next startup marks
  that interrupted job `failed`. The previous SQLite result remains available.
- Spark exits with the child process and is stopped while the worker is idle.

## Local verification commands

Pure Python job, loader, and worker tests:

```bash
python3 -m unittest -v \
  backend.tests.test_job_store \
  backend.tests.test_load_results \
  backend.tests.test_s3_results \
  backend.tests.test_cloud_jobs \
  analysis.tests.test_cloud_runner \
  analysis.tests.test_worker
```

Run the PySpark cleaning test in the existing environment (Java 17 is required):

```bash
conda run -n lab2 python -m unittest -v analysis.tests.test_main
```

Run API tests inside the pinned backend image and build/test the frontend:

```bash
docker compose build backend
docker compose run --rm --no-deps \
  -v "$PWD/backend/tests:/app/backend/tests:ro" \
  backend python -m unittest -v \
    backend.tests.test_backend \
    backend.tests.test_cloud_jobs \
    backend.tests.test_job_store \
    backend.tests.test_load_results \
    backend.tests.test_s3_results

cd frontend
npm test
npm run build
```

After a full UI-triggered run, verify the live stack:

```bash
curl --fail http://localhost:8000/api/health
curl --fail http://localhost:8000/api/analysis/jobs/latest
curl --fail http://localhost:8000/api/users/214328887
curl --fail http://localhost:8000/api/users/107830991
sqlite3 data/state/users.db \
  'SELECT user_id,followers,followees FROM user_counts WHERE user_id IN (214328887,107830991) ORDER BY user_id;'
```

Expected full-data acceptance values:

| Measure | Expected |
| --- | ---: |
| Raw rows | 2,426,143 |
| Malformed rows | 0 |
| Self-follows | 22 |
| Repeated non-self rows | 657,986 |
| Distinct non-self relationships | 1,768,135 |
| Users | 81,306 |
| Followers sum | 1,768,135 |
| Followees sum | 1,768,135 |

| User | Followers | Followees |
| --- | ---: | ---: |
| 214328887 | 180 | 214 |
| 107830991 | 180 | 157 |

## AWS deployment

The local flow above remains unchanged. Cloud mode adds a DynamoDB-backed job
store and ECS Fargate orchestrator to FastAPI plus a one-shot analysis entry
point. The Terraform module under [`infra/terraform`](infra/terraform/README.md)
deploys:

- the existing frontend/Nginx and backend images as Docker Compose on one
  single-instance Elastic Beanstalk environment, with no load balancer;
- CloudFront as the authenticated, valid-HTTPS endpoint;
- a private versioned S3 bucket for the input and completed results;
- one no-ingress ECS Fargate task started only by an API request; and
- DynamoDB, private ECR, IAM, and the required networking.

The Fargate task runs one local Spark worker with 1024 CPU units (1 vCPU) and
4096 MiB memory. It reads the input through Hadoop S3A using its task role;
this is not a distributed Spark cluster. There is no persistent analysis EC2
instance or SSM Automation workflow. Analysis logs are retained in CloudWatch
for one day for diagnosis and lab evidence.
Results are published to an immutable job prefix. FastAPI requires the exact
manifest, hashes, statistics, and zero-byte `_SUCCESS` marker before replacing
SQLite transactionally. A replacement Beanstalk instance restores that cache
from `results/latest.json`.

All regional resources use AWS profile `colin` and Region
`ap-southeast-2`. The browser receives neither the analysis token nor AWS
credentials. CloudFront provides server-side Basic Auth, Nginx injects the
analysis token only for the protected POST, and the origin security group
accepts traffic only from CloudFront's managed origin-facing prefix list.

### Required manual cost confirmation

Install Terraform 1.8+ and ensure Docker Buildx is available, then:

```bash
cd infra/terraform
./scripts/preflight.sh
```

Review credits and current spend in the AWS Billing console first. The script
only confirms profile `colin`, Region `ap-southeast-2`, and caller identity; it
does not call Free Tier or Cost Explorer APIs and therefore needs no billing API
permissions. It does not create an AWS Organization or upgrade the account.

Copy [`terraform.tfvars.example`](infra/terraform/terraform.tfvars.example) to
`terraform.tfvars`. There are no credit-balance, email-alert, or monthly-budget
variables to configure. Then:

```bash
./scripts/deploy.sh
```

The script validates Terraform, displays a three-repository bootstrap plan,
requires `BOOTSTRAP`, builds and pushes immutable `linux/amd64` images, displays
the complete plan, and requires `APPLY`. Full commands, expected resources,
verification, failure/replacement tests, evidence, cost risks, and cleanup are in
the Terraform README.

### Current deployment status (2026-10-17)

The application is live at `https://d1zr9tydtpx0sp.cloudfront.net`. Retrieve
the Basic Auth username and password locally without printing the password into
documentation:

```bash
cd infra/terraform
terraform output -raw viewer_username
terraform output -raw viewer_password
```

Successful cloud job `92f14a98dad34637b9536e23dc4ef877` ran with task
definition revision 3 at 1 vCPU / 4 GiB. CloudWatch recorded Spark's exact
acceptance totals, ECS stopped with exit code 0, S3 received the immutable
result and `results/latest.json`, and the backend imported 81,306 rows into
SQLite. Duplicate submission returned the same job ID. Anonymous requests to
both the page and protected job endpoint returned HTTP 401.

Verified deployed results:

| User | Followers | Followees |
| --- | ---: | ---: |
| `214328887` | 180 | 214 |
| `107830991` | 180 | 157 |

Use the log and verification commands in
[`infra/terraform/README.md`](infra/terraform/README.md). A controlled
post-success failure test and Beanstalk replacement/restore test remain to be
run if required as separate destructive verification exercises.

After the review window, remove all lab resources:

```bash
cd infra/terraform
CONFIRM_DESTROY=cs5224-lab2 ./scripts/cleanup.sh
```
