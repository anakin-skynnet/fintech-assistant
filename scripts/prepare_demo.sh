#!/usr/bin/env bash
# Prepare the Getnet Financial Closure solution for demo: validate, deploy, run UC setup,
# workflow extension, demo bootstrap (seed sample Excel), then run the full pipeline.
# Usage: ./scripts/prepare_demo.sh [profile] [target]
# Example: ./scripts/prepare_demo.sh              # use default profile, target dev
#          ./scripts/prepare_demo.sh myprofile dev
#          ./scripts/prepare_demo.sh azure_getnet azure_dev
set -e

PROFILE="${1:-}"
TARGET="${2:-dev}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export DATABRICKS_BUNDLE_TARGET="$TARGET"
ARGS=(-t "$TARGET")
[[ -n "$PROFILE" ]] && ARGS+=(--profile "$PROFILE")

echo "=== Target: $TARGET ${PROFILE:+ (profile: $PROFILE)} ==="

# Helper: get job_id by name substring (bundle dev prefix is [dev user] so we match by substring)
get_job_id() {
  local name="$1"
  local out
  if [[ -n "$PROFILE" ]]; then
    out=$(databricks jobs list -o json --profile "$PROFILE" --limit 200)
  else
    out=$(databricks jobs list -o json --limit 200)
  fi
  # API returns top-level array; match job name containing the given substring
  echo "$out" | jq -r --arg n "$name" '.[] | select(.settings.name != null and (.settings.name | test($n; "i"))) | .job_id' | head -1
}

run_job_and_wait() {
  local name="$1"
  local id
  id=$(get_job_id "$name")
  if [[ -z "$id" ]]; then
    echo "Job not found: $name (deploy first?). Skipping."
    return 1
  fi
  echo "Running job: $name (id: $id)"
  if [[ -n "$PROFILE" ]]; then
    databricks jobs run-now "$id" --profile "$PROFILE" --timeout 25m
  else
    databricks jobs run-now "$id" --timeout 25m
  fi
  echo "Done: $name"
}

echo "=== 1. Validate bundle ==="
databricks bundle validate "${ARGS[@]}"

echo "=== 2. Deploy bundle ==="
databricks bundle deploy "${ARGS[@]}"

echo "=== 3. Run UC Setup (once) ==="
run_job_and_wait "Getnet Closure - UC Setup" || true

echo "=== 4. Run Workflow Extension (once) ==="
run_job_and_wait "Getnet Closure - Workflow Extension (reviewer, recipients, notified_bu_at)" || true

echo "=== 5. Run Demo Bootstrap (seed sample Excel in volume) ==="
run_job_and_wait "Getnet Closure - Demo Bootstrap (seed sample Excel in volume)" || true

echo "=== 6. Run Full Pipeline (Ingest → Validate → Agent → Reject → Notify BU → Global Send) ==="
run_job_and_wait "Getnet Closure - Full Pipeline (Ingest → Validate → Reject → Global Send)" || true

echo "=== Demo ready ==="
echo "Next: open the Databricks App (getnet-financial-closure), or Genie, or check Workflows → Runs for pipeline results."
echo "If global_closure_send skipped (e.g. missing BUs or already sent), run Validate and load then Global closure send separately, or re-run the pipeline after ensuring all 3 BUs have valid files."
