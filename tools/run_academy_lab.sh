#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Databricks Academy reusable runner
#
# Supported labs:
#   05 - Citi Bike Lakeflow
#   06 - External V2 End-to-End Gold Analytics
#
# Azure:
#   --cluster gp1  (default)
#   --cluster gp2
#   --cluster auto
#
# Personal:
#   serverless compute; --cluster is ignored.
#
# Lab 05 production deployment additionally creates its Prod schema/Volumes.
# Lab 06 uses ONE combined end-to-end Job:
#   01_dimensions
#     -> 02_fact_encounters
#     -> 03_fact_conditions
#     -> 04_aggregations
#          -> 05_register_shared_tables
#          -> 06_alert_metrics
#     -> 07_validation
# ============================================================================

AZURE_GP1_DEFAULT="0702-132442-toro5spu"
AZURE_GP2_DEFAULT="0702-171207-xo9bbc0y"

LAB=""
TARGET="azure_dev"
PROFILE=""
CLUSTER_CHOICE="gp1"

JOB_KEY=""
CLUSTER_VAR=""
RESOURCE_SELECT=""

GP1="$AZURE_GP1_DEFAULT"
GP2="$AZURE_GP2_DEFAULT"

DO_VALIDATE="true"
DO_DEPLOY="true"
DO_RUN="true"

usage() {
  cat <<'EOF'
Usage:
  run_academy_lab.sh --lab 05|06 [options]

Options:
  --lab LAB
      Supported: 05, 06.

  --target TARGET
      azure_dev, azure_prod, personal_dev, personal_prod.
      Default: azure_dev

  --profile PROFILE
      Databricks CLI profile.

  --cluster gp1|gp2|auto
      Azure compute selection. Default: gp1.
      Personal targets ignore this option and use serverless.

  --gp1 CLUSTER_ID
      Override Azure GP1 cluster ID.

  --gp2 CLUSTER_ID
      Override Azure GP2 cluster ID.

  --skip-validate
  --skip-deploy
  --skip-run

  -h, --help
EOF
}

configure_lab() {
  case "$LAB" in
    5|05)
      LAB="05"
      JOB_KEY="lab05_lakeflow_job"
      CLUSTER_VAR="lab05_cluster_id"

      case "$TARGET" in
        azure_prod|personal_prod)
          RESOURCE_SELECT="schemas.lab05_prod_schema,volumes.lab05_reference_volume,volumes.lab05_streaming_volume,jobs.lab05_lakeflow_job,pipelines.lab05_lakeflow_pipeline"
          ;;
        *)
          RESOURCE_SELECT="jobs.lab05_lakeflow_job,pipelines.lab05_lakeflow_pipeline"
          ;;
      esac
      ;;

    6|06)
      LAB="06"

      # Final Lab 06 design: ONE combined end-to-end Job.
      JOB_KEY="lab06_external_gold_job"
      CLUSTER_VAR="lab06_cluster_id"
      RESOURCE_SELECT="jobs.lab06_external_gold_job"
      ;;

    7|07|8|08|9|09|10|11|12)
      echo "ERROR: Lab $LAB is not wired into this runner yet." >&2
      echo "Add its resource mapping to configure_lab()." >&2
      exit 2
      ;;

    *)
      echo "ERROR: Supported labs are 05 and 06." >&2
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
    --cluster)
      CLUSTER_CHOICE="$(echo "$2" | tr '[:upper:]' '[:lower:]')"; shift 2 ;;
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

if [[ -z "$LAB" ]]; then
  echo "ERROR: --lab is required." >&2
  usage
  exit 2
fi

case "$CLUSTER_CHOICE" in
  gp1|gp2|auto) ;;
  *)
    echo "ERROR: --cluster must be gp1, gp2, or auto." >&2
    exit 2 ;;
esac

configure_lab

command -v databricks >/dev/null 2>&1 || {
  echo "ERROR: Databricks CLI is not available in PATH." >&2
  exit 1
}

DBX_AUTH_ARGS=()
if [[ -n "$PROFILE" ]]; then
  DBX_AUTH_ARGS=(--profile "$PROFILE")
fi

BUNDLE_VAR_ARGS=()

case "$TARGET" in
  azure_dev|azure_prod)
    command -v python >/dev/null 2>&1 || {
      echo "ERROR: Python is required to parse cluster state." >&2
      exit 1
    }

    cluster_state() {
      local cluster_id="$1"
      databricks clusters get \
        "$cluster_id" \
        "${DBX_AUTH_ARGS[@]}" \
        -o json |
        python -c 'import json,sys; print(json.load(sys.stdin)["state"])'
    }

    wait_until_running() {
      local cluster_id="$1"
      local cluster_name="$2"

      echo "Waiting for $cluster_name to become RUNNING..."

      for attempt in $(seq 1 60); do
        local state
        state="$(cluster_state "$cluster_id")"

        if [[ "$state" == "RUNNING" ]]; then
          echo "$cluster_name is RUNNING."
          return 0
        fi

        if [[ "$state" == "ERROR" || "$state" == "UNKNOWN" ]]; then
          echo "ERROR: $cluster_name entered state $state." >&2
          exit 1
        fi

        if [[ "$attempt" -eq 60 ]]; then
          echo "ERROR: Timed out waiting for $cluster_name." >&2
          exit 1
        fi

        sleep 10
      done
    }

    start_if_needed() {
      local cluster_id="$1"
      local cluster_name="$2"
      local state

      state="$(cluster_state "$cluster_id")"
      echo "$cluster_name state: $state"

      if [[ "$state" != "RUNNING" ]]; then
        echo "Starting $cluster_name ($cluster_id)..."
        databricks clusters start \
          "$cluster_id" \
          "${DBX_AUTH_ARGS[@]}" >/dev/null
        wait_until_running "$cluster_id" "$cluster_name"
      fi
    }

    echo "============================================================"
    echo "Databricks Academy Runner"
    echo "Lab             : $LAB"
    echo "Target          : $TARGET"
    echo "Cluster choice  : $CLUSTER_CHOICE"
    echo "============================================================"
    echo

    case "$CLUSTER_CHOICE" in
      gp1)
        CLUSTER_ID="$GP1"
        CLUSTER_NAME="GP1"
        start_if_needed "$CLUSTER_ID" "$CLUSTER_NAME"
        ;;

      gp2)
        CLUSTER_ID="$GP2"
        CLUSTER_NAME="GP2"
        start_if_needed "$CLUSTER_ID" "$CLUSTER_NAME"
        ;;

      auto)
        GP1_STATE="$(cluster_state "$GP1")"
        GP2_STATE="$(cluster_state "$GP2")"

        echo "GP1 state: $GP1_STATE"
        echo "GP2 state: $GP2_STATE"

        if [[ "$GP1_STATE" == "RUNNING" ]]; then
          CLUSTER_ID="$GP1"
          CLUSTER_NAME="GP1"
        elif [[ "$GP2_STATE" == "RUNNING" ]]; then
          CLUSTER_ID="$GP2"
          CLUSTER_NAME="GP2"
        else
          CLUSTER_ID="$GP1"
          CLUSTER_NAME="GP1"
          start_if_needed "$CLUSTER_ID" "$CLUSTER_NAME"
        fi
        ;;
    esac

    echo
    echo "Selected compute: $CLUSTER_NAME ($CLUSTER_ID)"
    echo

    BUNDLE_VAR_ARGS=(--var "${CLUSTER_VAR}=${CLUSTER_ID}")
    ;;

  personal_dev|personal_prod)
    echo "============================================================"
    echo "Databricks Academy Runner"
    echo "Lab     : $LAB"
    echo "Target  : $TARGET"
    echo "Compute : serverless"
    echo "============================================================"
    echo
    echo "Personal workspace uses serverless compute."
    echo "--cluster $CLUSTER_CHOICE is ignored for this target."
    echo
    ;;

  *)
    echo "ERROR: Unsupported target '$TARGET'." >&2
    exit 2
    ;;
esac

if [[ "$DO_VALIDATE" == "true" ]]; then
  echo "Validating bundle..."
  databricks bundle validate \
    -t "$TARGET" \
    "${DBX_AUTH_ARGS[@]}" \
    "${BUNDLE_VAR_ARGS[@]}"
  echo
fi

if [[ "$DO_DEPLOY" == "true" ]]; then
  echo "Deploying selected Lab $LAB resources:"
  echo "  $RESOURCE_SELECT"

  databricks bundle deploy \
    -t "$TARGET" \
    "${DBX_AUTH_ARGS[@]}" \
    "${BUNDLE_VAR_ARGS[@]}" \
    --select "$RESOURCE_SELECT"
  echo
fi

if [[ "$DO_RUN" == "true" ]]; then
  echo "Running Job: $JOB_KEY"

  databricks bundle run \
    -t "$TARGET" \
    "${DBX_AUTH_ARGS[@]}" \
    "${BUNDLE_VAR_ARGS[@]}" \
    "$JOB_KEY"
  echo
fi

echo "============================================================"
echo "Completed Lab $LAB"
echo "Target: $TARGET"

if [[ "$TARGET" == azure_* ]]; then
  echo "Compute: $CLUSTER_NAME ($CLUSTER_ID)"
else
  echo "Compute: serverless"
fi

echo "============================================================"
