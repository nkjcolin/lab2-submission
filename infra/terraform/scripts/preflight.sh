#!/usr/bin/env bash
set -euo pipefail

profile="${AWS_PROFILE:-colin}"
region="${AWS_REGION:-ap-southeast-2}"

if [[ "$profile" != "colin" ]]; then
  echo "Refusing to continue: AWS_PROFILE must be colin." >&2
  exit 1
fi
if [[ "$region" != "ap-southeast-2" ]]; then
  echo "Refusing to continue: AWS_REGION must be ap-southeast-2." >&2
  exit 1
fi

identity_json=$(aws --profile "$profile" --region "$region" sts get-caller-identity --output json)
printf '%s\n' "$identity_json"

echo "AWS identity and deployment region confirmed."
echo "No Free Tier or Cost Explorer APIs are queried by this script."
echo "Review credits/spend in the Billing console, then record the manual confirmation in terraform.tfvars."
