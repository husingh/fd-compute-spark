#!/usr/bin/env bash
# =============================================================================
# FDS Compute — Deploy / Run Script
#
# USAGE:
#   1. Copy this file to deploy/run.local.sh  (gitignored — never committed)
#   2. Fill in the values in the "── Credentials ──" section below
#   3. chmod +x deploy/run.local.sh && ./deploy/run.local.sh
#
# This script:
#   a) Substitutes all placeholders in fds-compute-sparkapplication.yaml
#   b) Writes the filled YAML to /tmp/fds-compute-filled.yaml (not committed)
#   c) Deletes any previous SparkApplication with the same name
#   d) Applies the new SparkApplication
# =============================================================================
set -euo pipefail

KUBECONFIG_PATH="${KUBECONFIG:-/Users/husingh/Desktop/fd-compute-spark-June/kubeconfig.yaml}"
YAML_TEMPLATE="$(dirname "$0")/fds-compute-sparkapplication.yaml"
FILLED_YAML="/tmp/fds-compute-filled.yaml"

# ── Credentials (fill these in your local copy — NEVER commit real values) ───

INPUT_AK="<INPUT_S3_ACCESS_KEY>"
INPUT_SK="<INPUT_S3_SECRET_KEY>"

OUTPUT_AK="<OUTPUT_S3_ACCESS_KEY>"
OUTPUT_SK="<OUTPUT_S3_SECRET_KEY>"

# Full JSON strings for SSE-C key files.
# Example: '{"1":"base64encodedkey1==","2":"base64encodedkey2=="}'
INPUT_SSE_C_KEYS_JSON='{"1":"<INPUT_SSE_C_KEY_1_BASE64>"}'
OUTPUT_SSE_C_KEYS_JSON='{"1":"<OUTPUT_SSE_C_KEY_BASE64>"}'

# ── Run ID — bump this for every new pipeline run ────────────────────────────
RUN_ID="run-20260603-003"

# ─────────────────────────────────────────────────────────────────────────────

echo "==> Substituting placeholders → $FILLED_YAML"
sed \
  -e "s|<INPUT_S3_ACCESS_KEY>|${INPUT_AK}|g" \
  -e "s|<INPUT_S3_SECRET_KEY>|${INPUT_SK}|g" \
  -e "s|<OUTPUT_S3_ACCESS_KEY>|${OUTPUT_AK}|g" \
  -e "s|<OUTPUT_S3_SECRET_KEY>|${OUTPUT_SK}|g" \
  -e "s|\$INPUT_SSE_C_KEYS_JSON|${INPUT_SSE_C_KEYS_JSON}|g" \
  -e "s|\$OUTPUT_SSE_C_KEYS_JSON|${OUTPUT_SSE_C_KEYS_JSON}|g" \
  -e "s|run-20260603-002|${RUN_ID}|g" \
  "$YAML_TEMPLATE" > "$FILLED_YAML"

echo "==> Deleting previous SparkApplication (if any)"
kubectl --kubeconfig "$KUBECONFIG_PATH" delete sparkapplication fds-compute-pipeline \
  -n spark-operator --ignore-not-found

echo "==> Applying $FILLED_YAML"
kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f "$FILLED_YAML"

echo ""
echo "==> Submitted. Watch with:"
echo "    kubectl --kubeconfig $KUBECONFIG_PATH get sparkapplication fds-compute-pipeline -n spark-operator -w"
echo "    kubectl --kubeconfig $KUBECONFIG_PATH logs -n spark-operator -l spark-role=driver -f"
