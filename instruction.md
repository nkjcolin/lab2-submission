# Part 2 local and AWS deployment instructions

## Goal and scope

Maintain the existing PySpark, FastAPI, and React application. Preserve its
data-cleaning rules, API responses, verified follower counts, and working local
Docker flow. Deploy it to AWS only after the manual cost confirmation below.
Do not use local Docker output as cloud-execution evidence.

## Data contract and analysis invariants

Each input line contains `follower_id followee_id`. The first user follows the
second. `analysis/main.py` must continue to:

1. accept exactly two non-negative integer IDs and report rejected rows;
2. remove self-follows and deduplicate ordered pairs while retaining reverse
   relationships;
3. count followees by follower ID and followers by followee ID;
4. include every valid ID and fill missing counts with zero;
5. publish headered `part-*.csv` files atomically; and
6. verify that both count sums equal the distinct non-self edge total.

Acceptance values for `upload/twitter_combined.txt` remain 2,426,143 raw rows,
0 malformed rows, 22 self-follows, 657,986 repeated non-self rows, 1,768,135
distinct non-self relationships, and 81,306 users. User `214328887` must have
180 followers and 214 followees. User `107830991` must have 180 followers and
157 followees. These values are verification checks, never hard-coded output.

## Local service workflow

Keep three separate Docker services:

- `frontend`: React production build served by Nginx. It uses relative `/api`
  requests and injects the local analysis token only in the reverse proxy.
- `backend`: FastAPI and SQLite. It starts successfully with zero users, keeps
  the lookup and pagination APIs, accepts protected job submissions, and reads
  persistent job records.
- `analysis`: a private worker with no published port. It polls a persistent
  filesystem queue, starts `analysis/main.py` in a child process only for a
  queued job, loads successful CSV output transactionally into SQLite, then
  returns to an idle state with no Spark JVM.

Do not mount the Docker socket. Share only the read-only input, result directory,
and state directory required by the workflow.

## Job API and persistence

The API must retain `GET /api/health`, `GET /api/users/{user_id}`, and paginated
`GET /api/users`. Add:

- protected `POST /api/analysis/jobs` returning a job ID immediately;
- `GET /api/analysis/jobs/latest`; and
- `GET /api/analysis/jobs/{job_id}`.

Statuses are `queued`, `running`, `succeeded`, and `failed`. Submission must be
atomic: if a queued or running job exists, a second submission returns that job
instead of creating another. Job JSON and logs live below `STATE_DIR/jobs` so
status survives container restarts. A worker restart marks an interrupted
`running` job failed; a queued job remains eligible to run.

Successful analysis publishes results first, then uses the existing
transactional SQLite loader. A process or import failure must leave the prior
valid SQLite table available. The backend and frontend must remain available
when neither results nor jobs exist.

## Frontend behavior

Keep first-load reference users, numeric lookup, and pagination. Add a Run
analysis control that:

- submits one job and disables itself while submission is queued or running;
- polls and displays all four job states;
- displays a useful failure message; and
- reloads featured, lookup, and directory data after success.

The browser must not contain the analysis token. Nginx adds it to the protected
POST request; Vite's development proxy adds the safe local development value.

## Docker and configuration

`docker compose up --build` starts all three services without executing Spark.
Use bind mounts for `UPLOAD_DIR`, `RESULTS_DIR`, and `STATE_DIR`. Configure
`SPARK_MASTER`, `JOB_POLL_SECONDS`, the frontend/backend ports, and a local-only
`ANALYSIS_API_TOKEN` through `.env`; document placeholders in `.env.example`.

The analysis service is never public. The backend may remain exposed locally
for API inspection, but direct job POST requests require the configured token.

## Required local verification

Before completion:

1. run unit tests for cleaning, transactional loading, job persistence,
   protected/deduplicated submission, worker success/failure, and frontend API
   calls;
2. build all images and start the complete stack with one Compose command;
3. verify the frontend and backend start with no results;
4. submit duplicate requests and confirm one job ID;
5. interrupt a running job and confirm restart recovery marks it failed while
   preserving old results;
6. run the supplied full dataset to success and verify all acceptance values;
7. restart containers and verify the completed job and SQLite results remain;
8. document exact commands and observed results in `README.md`.

## AWS deployment constraints

Use AWS CLI profile `colin`; pin all regional resources and commands to
`ap-southeast-2`. Before provisioning, record `sts get-caller-identity`, review
credits and current-month spend in the AWS Billing console, and review the
Terraform plan and estimate. The deployment scripts must not require Free Tier
or Cost Explorer API permissions. Stop if credit eligibility cannot be
confirmed manually or if gross lab cost could exceed USD 3. Do not create or join an AWS
Organization, upgrade the plan, or add a load balancer or NAT gateway.

The deployed architecture is:

1. a single-instance Elastic Beanstalk Docker Compose environment containing
   the built React/Nginx frontend and FastAPI backend, with no load balancer;
2. CloudFront as the valid-HTTPS viewer endpoint and server-side Basic Auth
   gate, with the Beanstalk origin restricted to CloudFront traffic;
3. one private, versioned S3 bucket for the input, immutable completed result
   sets, and latest-success pointer;
4. a DynamoDB job table with an atomic active-job lock; and
5. one on-demand ECS Fargate task with no inbound ports, launched directly by
   FastAPI for each job and automatically released when its container exits.

The Fargate task runs the existing PySpark job as one local worker. It is not a
distributed multi-node Spark deployment. Use the tested task size of 1024 CPU
units (1 vCPU) and 4096 MiB RAM. AWS credentials are provided only by narrowly
scoped task roles, never by files or images. Do not create an analysis EC2
instance or SSM Automation workflow. Retain analysis CloudWatch logs for one
day to support cloud-run diagnosis and evidence capture.

## Cloud result safety and recovery

FastAPI creates the durable job and active lock atomically, calls ECS `RunTask`,
stores the returned task ARN, and returns the job ID immediately. The Fargate
task exists only for the job and stops automatically when its container exits.
Repeated submissions return the active job.

PySpark reads `input/twitter_combined.txt` directly through Hadoop S3A using the
task role; the runner also downloads the pinned object version to a fresh
working directory only for size/SHA-256 provenance. After validation succeeds, it
publishes immutable files below `results/jobs/<job_id>/`: exact part files,
statistics, a manifest with row counts and SHA-256 digests, and `_SUCCESS`.
Only then may it replace `results/latest.json`. Failure must never change the
latest pointer.

The backend imports only a completed same-job manifest after validating its
bucket, prefix, object sizes and digests, statistics, row total, and success
marker. The existing SQLite staging-table transaction remains the final swap,
so a failed run/import keeps the previous results. A replacement Beanstalk
instance restores its SQLite cache from the latest successful S3 result.

## AWS verification checkpoints

1. Verify HTTPS authentication, direct-origin rejection, Beanstalk health, and
   the analysis security group's empty ingress list.
2. Submit two simultaneous authenticated requests. Require one job ID, one
   ECS task ARN, and visible queued/running/succeeded states.
3. Validate genuine ECS task status/exit code, CloudWatch analysis logs,
   DynamoDB job state, S3 manifest, and the full acceptance totals through the
   deployed APIs.
4. Run a controlled invalid-input job. Require failure, unchanged latest S3
   pointer and API results, and a stopped ECS task with nonzero exit code.
5. Replace the Beanstalk instance and verify its SQLite cache is restored from
   S3 without another analysis run.

Store evidence in a new timestamped `evidence/aws-<UTC timestamp>/` directory.
Correlate Region, UTC time, job ID, ECS task ARN, S3 prefix, and results.
Screenshots and command output must be genuine AWS evidence.

Leave the web environment available only for the requested review window. The
analysis service has no persistent compute between jobs. Promptly destroy
Beanstalk/EC2/EBS, CloudFront, S3, ECR, DynamoDB, ECS, IAM, SSM parameters, and
networking after review.
