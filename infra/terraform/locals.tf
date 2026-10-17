locals {
  input_file_path = var.input_file_path == null ? abspath("${path.module}/../../upload/twitter_combined.txt") : abspath(var.input_file_path)

  input_key          = "input/twitter_combined.txt"
  result_prefix_root = "results/jobs"
  result_pointer_key = "results/latest.json"

  frontend_image = "${aws_ecr_repository.frontend.repository_url}:${var.image_tag}"
  backend_image  = "${aws_ecr_repository.backend.repository_url}:${var.image_tag}"
  analysis_image = "${aws_ecr_repository.analysis.repository_url}:${var.image_tag}"

  viewer_basic_token = base64encode("${var.viewer_username}:${random_password.viewer.result}")

  eb_imds_hook = <<-BASH
    #!/bin/bash
    set -euo pipefail
    token=$(curl --fail --silent --show-error --request PUT \
      --header 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
      http://169.254.169.254/latest/api/token)
    instance_id=$(curl --fail --silent --show-error \
      --header "X-aws-ec2-metadata-token: $token" \
      http://169.254.169.254/latest/meta-data/instance-id)
    aws ec2 modify-instance-metadata-options \
      --region '${var.aws_region}' \
      --instance-id "$instance_id" \
      --http-endpoint enabled \
      --http-tokens required \
      --http-put-response-hop-limit 2
  BASH

  # Elastic Beanstalk creates .env from environment properties. The local
  # compose.yaml is deliberately not modified by this cloud bundle.
  eb_compose = <<-YAML
    services:
      backend:
        image: ${local.backend_image}
        environment:
          ANALYSIS_API_TOKEN: "$${ANALYSIS_API_TOKEN}"
          AWS_ECS_CLUSTER: "$${AWS_ECS_CLUSTER}"
          AWS_ECS_CONTAINER_NAME: "$${AWS_ECS_CONTAINER_NAME}"
          AWS_ECS_SECURITY_GROUP_ID: "$${AWS_ECS_SECURITY_GROUP_ID}"
          AWS_ECS_SUBNET_ID: "$${AWS_ECS_SUBNET_ID}"
          AWS_ECS_TASK_DEFINITION: "$${AWS_ECS_TASK_DEFINITION}"
          AWS_JOB_TABLE: "$${AWS_JOB_TABLE}"
          AWS_REGION: "$${AWS_REGION}"
          DATA_BUCKET: "$${DATA_BUCKET}"
          DB_PATH: /data/state/users.db
          INPUT_KEY: "$${INPUT_KEY}"
          JOB_BACKEND: aws
          JOB_DIR: /data/state/jobs
          ORIGIN_VERIFY_SECRET: "$${ORIGIN_VERIFY_SECRET}"
          RESULT_POINTER_KEY: "$${RESULT_POINTER_KEY}"
          RESULT_PREFIX_ROOT: "$${RESULT_PREFIX_ROOT}"
        expose:
          - "8000"
        volumes:
          - app-state:/data/state
        healthcheck:
          test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=2).read()"]
          interval: 10s
          timeout: 3s
          retries: 20
          start_period: 15s
        restart: unless-stopped

      frontend:
        image: ${local.frontend_image}
        environment:
          ANALYSIS_API_TOKEN: "$${ANALYSIS_API_TOKEN}"
        depends_on:
          backend:
            condition: service_healthy
        ports:
          - "80:80"
        restart: unless-stopped

    volumes:
      app-state:
  YAML
}
