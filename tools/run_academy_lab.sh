#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Databricks Academy — reusable runner for Labs 05-12
#
# One script:
#   - checks GP1 / GP2
#   - prefers an already RUNNING all-purpose cluster
#   - starts GP1 if neither is running
#   - injects the selected cluster ID into a Bundle variable
#   - validates the Bundle
#   - deploys only the selected lab resources
#   - runs the selected lab Job
#
# Known mappings are currently configured for Lab 05 and Lab 06.
# Add Labs 07-12 to configure_lab() when their resource keys are finalized.
#
# Examples:
#
#   Lab 05 dev:
#     bash tools/run_academy_lab.sh \
#       --lab 05 \
#       --target azure_dev \
#       --profile adb-7405604503619901
#
#   Lab 05 prod:
#     bash tools/run_academy_lab.sh \
#       --lab 05 \
#       --target prod \
#       --profile adb-7405604503619901
#
#   Lab 06 dev:
#     bash tools/run_academy_lab.sh \
#       --lab 06 \
#       --target azure_dev \
#       --profile adb-7405604503619901
#
#   Generic/future lab:
#     bash tools/run_academy_lab.sh \
#       --job-key my_job \
#       --cluster-var my_cluster_id \
#       --select "jobs.my_job" \
#       --target azure_dev \
#       --profile adb-7405604503619901
# ============================================================================

GP1_DEFAULT="0702-132442-toro5spu"
GP2_DEFAULT="0702-171207-xo9bbc0y"

LAB=""
TARGET="azure_dev"
PROFILE=""

JOB_KEY=""
CLUSTER_VAR=""
RESOURCE_SELECT=""

GP1="$GP1_DEFAULT"
GP2="$GP2_DEFAULT"

DO_VALIDATE="true"
DO_DEPLOY="true"
DO_RUN="true"

usage() {
  cat <<'EOF'
Usage:
  run_academy_lab.sh --lab <05|06|...> [options]

or:

  run_academy_lab.sh \
    --job-key KEY \
    --cluster-var VARIABLE \
    --select RESOURCE_LIST \
    [options]

Options:
  --lab LAB
      Academy lab number. Built-in mappings currently: 05, 06.

  --target TARGET
      Bundle target. Default: azure_dev

  --profile PROFILE
      Databricks CLI profile. Optional.

  --job-key KEY
      Override / manually supply Bundle job resource key.

  --cluster-var VARIABLE
      Bundle variable that receives the selected cluster ID.

  --select RESOURCE_LIST
      Comma-separated Bundle resources to deploy.

  --gp1 CLUSTER_ID
      Primary all-purpose cluster.

  --gp2 CLUSTER_ID
      Secondary all-purpose cluster.

  --skip-validate
      Skip bundle validation.

  --skip-deploy
      Skip deployment.

  --skip-run
      Skip Job execution.

  -h, --help
      Show this help.

Cluster selection:
  GP1 RUNNING -> use GP1
  else GP2 RUNNING -> use GP2
  else -> start GP1 and wait until RUNNING
EOF
}

configure_lab() {
  case "$LAB" in
    5|05)
      LAB="05"
      JOB_KEY="${JOB_KEY:-lab05_lakeflow_job}"
      CLUSTER_VAR="${CLUSTER_VAR:-lab05_cluster_id}"
      RESOURCE_SELECT="${RESOURCE_SELECT:-jobs.lab05_lakeflow_job,pipelines.lab05_lakeflow_pipeline}"
      ;;
    6|06)
      LAB="06"
      JOB_KEY="${JOB_KEY:-lab06_external_gold_job}"
      CLUSTER_VAR="${CLUSTER_VAR:-lab06_cluster_id}"
      RESOURCE_SELECT="${RESOURCE_SELECT:-jobs.lab06_external_gold_job,dashboards.lab06_external_healthcare_dashboard,genie_spaces.lab06_external_healthcare_genie,alerts.lab06_external_healthcare_volume_drop}"
      ;;
    7|07|8|08|9|09|10|11|12)
      echo "ERROR: Lab $LAB mapping has not been configured yet." >&2
      echo "Add its JOB_KEY / CLUSTER_VAR / RESOURCE_SELECT in configure_lab()," >&2
      echo "or call this script with --job-key, --cluster-var and --select." >&2
      exit 2
      ;;
    "")
      ;;
    *)
      echo "ERROR: Unsupported lab number: $LAB" >&2
      exit 2
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lab)
      LAB="$2"; shift 2 ;;
    --target)
      TARGET="$2"; shift 2 ;;
    --profile)
      PROFILE="$2"; shift 2 ;;
    --job-key)
      JOB_KEY="$2"; shift 2 ;;
    --cluster-var)
      CLUSTER_VAR="$2"; shift 2 ;;
    --select)
      RESOURCE_SELECT="$2"; shift 2 ;;
    --gp1)
      GP1="$2"; shift 2 ;;
    --gp2)
      GP2="$2"; shift 2 ;;
    --skip-validate)
      DO_VALIDATE="false"; shift ;;
    --skip-deploy)
      DO_DEPLOY="false"; shift ;;
    --skip-run)
      DO_RUN="false"; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "ERROR: Unknown argument: $1" >&2
      usage
      exit 2 ;;
  esac
done

configure_lab

if [[ -z "$JOB_KEY" ]]; then
  echo "ERROR: A Job key is required." >&2
  echo "Use --lab for a configured lab or --job-key manually." >&2
  exit 2
fi

if [[ -z "$CLUSTER_VAR" ]]; then
  echo "ERROR: A cluster Bundle variable is required." >&2
  echo "Use --lab for a configured lab or --cluster-var manually." >&2
  exit 2
fi

if [[ "$DO_DEPLOY" == "true" && -z "$RESOURCE_SELECT" ]]; then
  echo "ERROR: --select is required when deployment is enabled." >&2
  exit 2
fi

command -v databricks >/dev/null 2>&1 || {
  echo "ERROR: Databricks CLI is not available in PATH." >&2
  exit 1
}

command -v python >/dev/null 2>&1 || {
  echo "ERROR: Python is required to parse CLI JSON output." >&2
  exit 1
}

DBX_AUTH_ARGS=()
if [[ -n "$PROFILE" ]]; then
  DBX_AUTH_ARGS=(--profile "$PROFILE")
fi

cluster_state() {
  local cluster_id="$1"

  databricks clusters get \
    "$cluster_id" \
    "${DBX_AUTH_ARGS[@]}" \
    -o json |
    python -c 'import json,sys; print(json.load(sys.stdin)["state"])'
}

echo "============================================================"
echo "Databricks Academy Runner"
echo "Lab         : ${LAB:-custom}"
echo "Target      : $TARGET"
echo "Job         : $JOB_KEY"
echo "Cluster var : $CLUSTER_VAR"
echo "GP1         : $GP1"
echo "GP2         : $GP2"
echo "============================================================"
echo

echo "Checking all-purpose clusters..."

GP1_STATE="$(cluster_state "$GP1")"
GP2_STATE="$(cluster_state "$GP2")"

echo "GP1: $GP1_STATE"
echo "GP2: $GP2_STATE"
echo

if [[ "$GP1_STATE" == "RUNNING" ]]; then
  CLUSTER_ID="$GP1"
  CLUSTER_NAME="GP1"

elif [[ "$GP2_STATE" == "RUNNING" ]]; then
  CLUSTER_ID="$GP2"
  CLUSTER_NAME="GP2"

else
  CLUSTER_ID="$GP1"
  CLUSTER_NAME="GP1"

  echo "No preferred all-purpose cluster is RUNNING."
  echo "Starting GP1..."

  databricks clusters start \
    "$GP1" \
    "${DBX_AUTH_ARGS[@]}" >/dev/null

  echo "Waiting for GP1 to become RUNNING..."

  for attempt in $(seq 1 60); do
    STATE="$(cluster_state "$GP1")"

    if [[ "$STATE" == "RUNNING" ]]; then
      echo "GP1 is RUNNING."
      break
    fi

    if [[ "$STATE" == "ERROR" || "$STATE" == "UNKNOWN" ]]; then
      echo "ERROR: GP1 entered state: $STATE" >&2
      exit 1
    fi

    if [[ "$attempt" -eq 60 ]]; then
      echo "ERROR: Timed out waiting for GP1." >&2
      exit 1
    fi

    sleep 10
  done
fi

echo
echo "Selected compute: $CLUSTER_NAME ($CLUSTER_ID)"
echo

BUNDLE_VAR_ARG="${CLUSTER_VAR}=${CLUSTER_ID}"

if [[ "$DO_VALIDATE" == "true" ]]; then
  echo "Validating Bundle..."
  databricks bundle validate \
    -t "$TARGET" \
    "${DBX_AUTH_ARGS[@]}" \
    --var "$BUNDLE_VAR_ARG"

  echo
fi

if [[ "$DO_DEPLOY" == "true" ]]; then
  echo "Deploying:"
  echo "  $RESOURCE_SELECT"

  databricks bundle deploy \
    -t "$TARGET" \
    "${DBX_AUTH_ARGS[@]}" \
    --var "$BUNDLE_VAR_ARG" \
    --select "$RESOURCE_SELECT"

  echo
fi

if [[ "$DO_RUN" == "true" ]]; then
  echo "Running Job: $JOB_KEY"

  databricks bundle run \
    -t "$TARGET" \
    "${DBX_AUTH_ARGS[@]}" \
    --var "$BUNDLE_VAR_ARG" \
    "$JOB_KEY"

  echo
fi

echo "============================================================"
echo "Completed"
echo "Lab     : ${LAB:-custom}"
echo "Target  : $TARGET"
echo "Job     : $JOB_KEY"
echo "Compute : $CLUSTER_NAME ($CLUSTER_ID)"
echo "============================================================"
