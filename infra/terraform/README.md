# AWS deployment infrastructure

This module deploys the existing Part 2 application in `ap-southeast-2` using
AWS CLI profile `colin`. It leaves the local `compose.yaml` workflow unchanged.

## Architecture

```text
browser -- HTTPS --> CloudFront Basic Auth
                         |
                  X-Origin-Verify
                         |
                         v
              Elastic Beanstalk single instance
                 frontend Nginx + React :80
                         |
                         v
                    FastAPI :8000
                         |
               DynamoDB job lock/state
                         |
                    ecs:RunTask
                         |
                         v
          one on-demand ECS Fargate PySpark task
                         |
             private S3 input and results
```

- Elastic Beanstalk is `SingleInstance`; there is no load balancer or NAT
  gateway.
- CloudFront supplies the valid `cloudfront.net` HTTPS certificate and checks
  viewer Basic Auth. The Beanstalk security group accepts HTTP only from the
  AWS-managed CloudFront origin-facing prefix list.
- FastAPI creates the DynamoDB job and active lock, then calls ECS `RunTask`.
  A duplicate click returns the existing job instead of starting another task.
- The analysis worker is ECS Fargate, not an EC2 analysis instance. There is
  nothing to start, stop, patch, or retain between runs. Fargate provisions the
  requested capacity when the API calls `RunTask` and releases it when the
  container exits.
- The task uses 1024 CPU units (1 vCPU) and 4096 MiB RAM by default. This was
  tested against the full dataset and keeps the one-shot Spark run reasonably
  short; it is still one local Spark worker and not a distributed cluster.
- The task has no inbound rules and receives a temporary public IP only to
  reach ECR, S3, DynamoDB, and Maven without a paid NAT gateway.
- No SSM Automation document is created. The analysis container writes to the
  `/ecs/cs5224-lab2-analysis` CloudWatch log group with one-day retention so a
  failed cloud run can be diagnosed without retaining logs indefinitely.
- SSM Parameter Store is still used only for two encrypted web secrets. This
  is unrelated to SSM Automation.
- Validated results are written to an immutable job prefix. The latest pointer
  is published last, and FastAPI verifies the manifest and hashes before
  replacing SQLite transactionally.

## Requirements and manual cost review

Requirements: Terraform 1.8+, AWS CLI v2, Docker Buildx, and
`upload/twitter_combined.txt`.

Review current credits and spending in the AWS Billing console before creating
resources. Terraform no longer creates an AWS Budget and has no credit-balance,
email-alert, or monthly-budget variables. The main continuous cost remains the
single `t3.micro` Beanstalk instance, its EBS volume, and public IPv4 address.
Fargate is charged only while an analysis task is provisioning or running.

Run the identity and Region check:

```bash
cd infra/terraform
./scripts/preflight.sh
```

It confirms profile `colin`, Region `ap-southeast-2`, and caller identity. It
does not call Free Tier or Cost Explorer APIs.

## Deployer permissions

Use AWS-managed `PowerUserAccess` plus the customer-managed policy in
`deployer-iam-policy.json`. `PowerUserAccess` includes private ECR and image
push access. The custom policy permits Terraform to manage and pass only IAM
roles and instance profiles named `cs5224-lab2-*`, including the two ECS task
roles.

`AmazonElasticContainerRegistryPublicFullAccess` is not needed because all
repositories are private.

## Configure

```bash
cp terraform.tfvars.example terraform.tfvars
```

The low-cost Fargate defaults are:

```hcl
analysis_task_cpu    = 1024
analysis_task_memory = 4096
```

The full cloud run completed in about four minutes at this size. Terraform
state contains generated authentication
secrets, so do not commit or share `terraform.tfstate` or `terraform.tfvars`.

## Deploy

```bash
./scripts/deploy.sh
```

The script:

1. checks the AWS identity and Region;
2. initializes, formats, and validates Terraform;
3. displays a plan for only the three private ECR repositories and requires
   the literal confirmation `BOOTSTRAP`;
4. builds and pushes `linux/amd64` frontend, backend, and analysis images;
5. displays the complete Terraform plan; and
6. requires the literal confirmation `APPLY` before creating the application.

The two stages are required because the repositories must exist before Docker
can push the images referenced by Elastic Beanstalk and the ECS task definition.

## Verify the web application

```bash
APP_URL=$(terraform output -raw live_url)
APP_USER=$(terraform output -raw viewer_username)
APP_PASSWORD=$(terraform output -raw viewer_password)

curl --fail --user "$APP_USER:$APP_PASSWORD" "$APP_URL/api/health"
curl --fail --user "$APP_USER:$APP_PASSWORD" "$APP_URL/api/users/214328887"
curl --fail --user "$APP_USER:$APP_PASSWORD" "$APP_URL/api/users/107830991"
```

Open `$APP_URL`, authenticate, and select **Run analysis**. A repeated click
while the task is active must return the same job ID.

## Inspect the Fargate run

Capture the job ID shown by the frontend, then run:

```bash
CLUSTER=$(terraform output -raw analysis_ecs_cluster)
TABLE=$(terraform output -raw job_table)
BUCKET=$(terraform output -raw data_bucket)

TASK_ARN=$(aws --profile colin --region ap-southeast-2 dynamodb get-item \
  --table-name "$TABLE" \
  --key "{\"job_id\":{\"S\":\"$JOB_ID\"}}" \
  --query 'Item.ecs_task_arn.S' --output text)

aws --profile colin --region ap-southeast-2 ecs describe-tasks \
  --cluster "$CLUSTER" --tasks "$TASK_ARN"

aws --profile colin --region ap-southeast-2 logs tail \
  /ecs/cs5224-lab2-analysis --since 30m

aws --profile colin --region ap-southeast-2 s3 ls \
  "s3://$BUCKET/results/" --recursive
aws --profile colin --region ap-southeast-2 s3 cp \
  "s3://$BUCKET/results/latest.json" -
```

Stopped ECS tasks are retained for a limited time, so capture the task details
promptly. Use the one-day CloudWatch logs, ECS `lastStatus`, container
`exitCode`, the DynamoDB job record, and S3 manifest/statistics as cloud-run
evidence.

## Verified deployment (2026-10-17)

- HTTPS URL: `https://d1zr9tydtpx0sp.cloudfront.net`
- Successful job: `92f14a98dad34637b9536e23dc4ef877`
- ECS task definition: `cs5224-lab2-analysis:3` (1 vCPU, 4 GiB)
- ECS result: `STOPPED`, exit code `0`; no running task remained
- Spark result: 1,768,135 distinct non-self relationships and 81,306 users
- API result: user `214328887` = 180/214; user `107830991` = 180/157
- Duplicate submission returned the same job ID with `duplicate: true`
- Anonymous page and job requests both returned HTTP `401`

The first diagnostic run exposed a missing `s3:ListBucket` prefix permission.
The task role now permits only `input`, `input/*`, `results/jobs/*`, and
`results/latest.json` bucket listing; object permissions remain scoped to the
single input object and result prefixes.

After success, the task must be `STOPPED` with exit code `0`. Verify:

| User | Followers | Followees |
| --- | ---: | ---: |
| `214328887` | 180 | 214 |
| `107830991` | 180 | 157 |

The published statistics must show 1,768,135 distinct non-self relationships
and 81,306 users.

## Refresh results

Replace `input/twitter_combined.txt` in the private bucket, then select **Run
analysis** again. The new immutable result prefix becomes current only after
Spark validation, S3 publication, backend validation, and the transactional
SQLite import. A failed task leaves the previous result pointer and SQLite
data unchanged.

## Failure test

After one successful run, temporarily move the input object, start another job,
and then restore the input:

```bash
BACKUP_KEY="failure-test/twitter_combined.txt"
aws --profile colin --region ap-southeast-2 s3 cp \
  "s3://$BUCKET/input/twitter_combined.txt" "s3://$BUCKET/$BACKUP_KEY"
aws --profile colin --region ap-southeast-2 s3 rm \
  "s3://$BUCKET/input/twitter_combined.txt"

restore_input() {
  aws --profile colin --region ap-southeast-2 s3 cp \
    "s3://$BUCKET/$BACKUP_KEY" "s3://$BUCKET/input/twitter_combined.txt"
  aws --profile colin --region ap-southeast-2 s3 rm \
    "s3://$BUCKET/$BACKUP_KEY"
}
trap restore_input EXIT
```

Start the job from the UI. It must become `failed`, the ECS task must stop with
a nonzero exit code, and the previous API counts and `results/latest.json` must
remain unchanged.

## Cleanup

There is no Fargate worker to stop when idle. The Beanstalk instance continues
to incur charges until the environment is destroyed:

```bash
CONFIRM_DESTROY=cs5224-lab2 ./scripts/cleanup.sh
```

The cleanup script enables final deletion of versioned S3/ECR content and then
runs `terraform destroy`. Review the destroy plan before confirming it.
