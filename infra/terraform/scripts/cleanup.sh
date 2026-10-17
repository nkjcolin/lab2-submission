#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
terraform_dir=$(cd "$script_dir/.." && pwd)
cd "$terraform_dir"

[[ "${CONFIRM_DESTROY:-}" == "cs5224-lab2" ]] || {
  echo "Set CONFIRM_DESTROY=cs5224-lab2 to confirm final, irreversible cloud cleanup." >&2
  exit 1
}

terraform apply -var='force_destroy=true' -auto-approve
terraform destroy -var='force_destroy=true'
