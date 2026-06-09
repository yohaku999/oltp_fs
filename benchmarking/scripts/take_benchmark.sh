#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKING_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${SCRIPT_DIR}/run_postgres_tpcc_docker.sh"
COMPOSE_FILE="${BENCHMARKING_DIR}/docker-compose.yaml"
DBFS_CONFIG="${BENCHMARKING_DIR}/benchbase-config/dbfs_tpcc_docker.xml"
POSTGRES_CONFIG="${BENCHMARKING_DIR}/benchbase-config/postgres_tpcc_docker.xml"

usage() {
  echo "Usage: $0 [exp-name]" >&2
}

cleanup() {
  docker compose -f "${COMPOSE_FILE}" down >/dev/null 2>&1 || true
}

log() {
  echo "[take_benchmark] $*"
}

if [[ $# -gt 1 ]]; then
  usage
  exit 1
fi

EXP_NAME="${1:-}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_LABEL="${TIMESTAMP}"

if [[ -n "${EXP_NAME}" ]]; then
  RUN_LABEL+="_${EXP_NAME}"
fi

trap cleanup EXIT

log "run_label=${RUN_LABEL}"
log "resetting compose services"
cleanup

log "starting dbfs benchmark"
DBFS_LOG_LEVEL="${DBFS_LOG_LEVEL:-warn}" \
DBFS_SERVER_LOG_LEVEL="${DBFS_SERVER_LOG_LEVEL:-warn}" \
DBFS_INDEX_LOG_LEVEL="${DBFS_INDEX_LOG_LEVEL:-warn}" \
DBFS_STORAGE_LOG_LEVEL="${DBFS_STORAGE_LOG_LEVEL:-info}" \
DBFS_BUFFER_POOL_LOG_STATS_EVERY="${DBFS_BUFFER_POOL_LOG_STATS_EVERY:-1000}" \
DBFS_OPERATOR_LOG_ROWS="${DBFS_OPERATOR_LOG_ROWS:-0}" \
RUN_LABEL="${RUN_LABEL}" \
  "${RUNNER}" \
  -c "${DBFS_CONFIG}" \
  --profile-perf

log "starting postgres benchmark"
RUN_LABEL="${RUN_LABEL}" \
  "${RUNNER}" \
  -c "${POSTGRES_CONFIG}"

log "completed run_label=${RUN_LABEL}"
