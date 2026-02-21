#!/usr/bin/env bash
# Deploy Getnet Financial Closure bundle to Azure Databricks.
# Usage: ./scripts/deploy_azure.sh <profile> [target]
# Example: ./scripts/deploy_azure.sh azure_getnet azure_dev
#          ./scripts/deploy_azure.sh azure_getnet azure_prod

set -e
PROFILE="${1:?Usage: $0 <databricks-profile-name> [target]. Example: $0 azure_getnet azure_dev}"
TARGET="${2:-azure_dev}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "Validating bundle for target: $TARGET (profile: $PROFILE)"
databricks bundle validate -t "$TARGET" --profile "$PROFILE"

echo "Deploying bundle to Azure Databricks (target: $TARGET)"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE"

echo "Done. Next: run the 'Getnet Closure - UC Setup' job once in the workspace, and configure secret scopes (see docs/DEPLOY_AZURE.md)."
