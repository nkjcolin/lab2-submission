#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
terraform_dir=$(cd "$script_dir/.." && pwd)
repo_dir=$(cd "$terraform_dir/../.." && pwd)

profile="${AWS_PROFILE:-colin}"
region="${AWS_REGION:-ap-southeast-2}"

cd "$terraform_dir"
"$script_dir/preflight.sh"
terraform init
terraform fmt -recursive -check
terraform validate

# ECR must exist before Buildx can push the immutable application tag. The
# final un-targeted plan remains the authoritative review of all resources.
terraform plan -out=bootstrap.tfplan \
  -target=aws_ecr_repository.frontend \
  -target=aws_ecr_repository.backend \
  -target=aws_ecr_repository.analysis
terraform show bootstrap.tfplan
read -r -p "Create only these three private ECR repositories? Type BOOTSTRAP: " bootstrap_answer
[[ "$bootstrap_answer" == "BOOTSTRAP" ]] || { echo "Cancelled; no resources created."; exit 1; }
terraform apply bootstrap.tfplan

account_id=$(aws --profile "$profile" --region "$region" sts get-caller-identity --query Account --output text)
registry="$account_id.dkr.ecr.$region.amazonaws.com"
image_tag=$(terraform console <<< 'var.image_tag' | tr -d '"')

aws --profile "$profile" --region "$region" ecr get-login-password \
  | docker login --username AWS --password-stdin "$registry"

frontend_repo=$(terraform output -raw frontend_ecr_repository)
backend_repo=$(terraform output -raw backend_ecr_repository)
analysis_repo=$(terraform output -raw analysis_ecr_repository)

docker buildx build --platform linux/amd64 --file "$repo_dir/frontend/Dockerfile" --tag "$frontend_repo:$image_tag" --push "$repo_dir"
docker buildx build --platform linux/amd64 --file "$repo_dir/backend/Dockerfile" --tag "$backend_repo:$image_tag" --push "$repo_dir"
docker buildx build --platform linux/amd64 --file "$repo_dir/analysis/Dockerfile" --tag "$analysis_repo:$image_tag" --push "$repo_dir"

terraform plan -out=tfplan
terraform show tfplan
read -r -p "Apply this reviewed plan in $region? Type APPLY: " answer
[[ "$answer" == "APPLY" ]] || { echo "Cancelled; no additional resources created."; exit 1; }
terraform apply tfplan

terraform output -raw live_url
printf '\nViewer username: %s\n' "$(terraform output -raw viewer_username)"
printf 'Viewer password: %s\n' "$(terraform output -raw viewer_password)"
