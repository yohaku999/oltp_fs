#!/usr/bin/env bash

# Run BenchBase TPC-C via docker compose:
#   - PostgreSQL as one compose service
#   - BenchBase as another compose service

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCHMARKING_ROOT="${REPO_ROOT}/benchmarking"
RESULTS_ROOT="${BENCHMARKING_ROOT}/results"
COMPOSE_FILE="${BENCHMARKING_ROOT}/docker-compose.yaml"
CONFIG_ROOT="${BENCHMARKING_ROOT}/config"
BENCHBASE_CONFIG_PATH=""

usage() {
  echo "Usage: $0 --config <path>" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--config)
      if [[ $# -lt 2 ]]; then
        echo "Missing argument for $1" >&2
        usage
        exit 1
      fi
      BENCHBASE_CONFIG_PATH="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 --config <path>"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "${BENCHBASE_CONFIG_PATH}" ]]; then
  echo "Config path is required." >&2
  usage
  exit 1
fi

if [[ "${BENCHBASE_CONFIG_PATH}" != /* ]]; then
  BENCHBASE_CONFIG_PATH="$(cd "$(dirname "${BENCHBASE_CONFIG_PATH}")" && pwd)/$(basename "${BENCHBASE_CONFIG_PATH}")"
fi

if [[ "${BENCHBASE_CONFIG_PATH}" != "${CONFIG_ROOT}"/* ]]; then
  echo "Config path must be under ${CONFIG_ROOT}" >&2
  exit 1
fi

CONTAINER_CONFIG="/benchbase/user-config/${BENCHBASE_CONFIG_PATH#"${CONFIG_ROOT}/"}"

WORKLOAD="tpcc"
ENGINE="postgres"
RUN_LABEL=${RUN_LABEL:-"$(date +%Y%m%d-%H%M%S)"}
RESULTS_SUBDIR="${WORKLOAD}/${ENGINE}/${RUN_LABEL}"

mkdir -p "${RESULTS_ROOT}"

compose_cmd=(docker compose -f "${COMPOSE_FILE}")

echo "Starting PostgreSQL compose service..."
"${compose_cmd[@]}" up -d postgres

echo "Waiting for PostgreSQL compose service to become ready..."
for i in {1..60}; do
  if "${compose_cmd[@]}" exec -T postgres pg_isready -U admin -d benchbase >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ERROR: PostgreSQL did not become ready in time." >&2
    exit 1
  fi
done

RESULTS_DIR_CONTAINER="/benchbase/results/${RESULTS_SUBDIR}"

echo "Running BenchBase compose service..."
echo "  Workload:   ${WORKLOAD}"
echo "  Config:     ${CONTAINER_CONFIG}"
echo "  Results:    ${RESULTS_DIR_CONTAINER} (mounted to ${RESULTS_ROOT})"

"${compose_cmd[@]}" run --rm benchbase \
  -b "${WORKLOAD}" \
  -c "${CONTAINER_CONFIG}" \
  --create=true \
  --load=true \
  --execute=true \
  -d "${RESULTS_DIR_CONTAINER}"

HOST_RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_SUBDIR}"

echo "BenchBase run completed. Results should be under:"
echo "  ${HOST_RESULTS_DIR}"

echo "NOTE: PostgreSQL remains running as a compose service."
echo "      You can stop it manually with: docker compose -f ${COMPOSE_FILE} down"
