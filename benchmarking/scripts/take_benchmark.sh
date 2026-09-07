#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARKING_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${SCRIPT_DIR}/run_benchbase_docker.sh"
COMPOSE_FILE="${BENCHMARKING_DIR}/docker-compose.yaml"
TPCC_DBFS_STATS_CONFIG="${BENCHMARKING_DIR}/benchbase-config/dbfs_tpcc_controlled_stats.xml"
TPCC_DBFS_THROUGHPUT_CONFIG="${BENCHMARKING_DIR}/benchbase-config/dbfs_tpcc_throughput.xml"
TPCC_POSTGRES_STATS_CONFIG="${BENCHMARKING_DIR}/benchbase-config/postgres_tpcc_controlled_stats.xml"
TPCC_POSTGRES_THROUGHPUT_CONFIG="${BENCHMARKING_DIR}/benchbase-config/postgres_tpcc_throughput.xml"
YCSB_DBFS_STATS_CONFIG="${BENCHMARKING_DIR}/benchbase-config/dbfs_ycsb_zipf_controlled_stats.xml"
YCSB_DBFS_THROUGHPUT_CONFIG="${BENCHMARKING_DIR}/benchbase-config/dbfs_ycsb_zipf_throughput.xml"
YCSB_POSTGRES_STATS_CONFIG="${BENCHMARKING_DIR}/benchbase-config/postgres_ycsb_zipf_controlled_stats.xml"
YCSB_POSTGRES_THROUGHPUT_CONFIG="${BENCHMARKING_DIR}/benchbase-config/postgres_ycsb_zipf_throughput.xml"

usage() {
  echo "Usage: $0 [--throughput|--stats] [--workload tpcc|ycsb] [--target dbfs|postgres|both] [exp-name]" >&2
  echo "  --throughput  Disable buffer-pool logging and generate a dbfs flamegraph." >&2
  echo "  --stats       Enable buffer-pool statistics and events (default)." >&2
  echo "  --workload    Select tpcc (default) or Zipfian YCSB." >&2
  echo "  --target      Select the database to benchmark (default: both)." >&2
}

cleanup() {
  if [[ -z "$(docker ps -aq --filter label=com.docker.compose.project=benchmarking 2>/dev/null)" ]]; then
    return
  fi
  docker compose -f "${COMPOSE_FILE}" down >/dev/null 2>&1 || true
}

log() {
  echo "[take_benchmark] $*"
}

MODE="stats"
WORKLOAD="tpcc"
TARGET="both"
EXP_NAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --throughput)
      MODE="throughput"
      ;;
    --stats)
      MODE="stats"
      ;;
    -b|--workload)
      if [[ $# -lt 2 ]]; then
        echo "--workload requires tpcc or ycsb." >&2
        usage
        exit 1
      fi
      WORKLOAD="$2"
      shift
      ;;
    --target)
      if [[ $# -lt 2 ]]; then
        echo "--target requires dbfs, postgres, or both." >&2
        usage
        exit 1
      fi
      TARGET="$2"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
    *)
      if [[ -n "${EXP_NAME}" ]]; then
        echo "Only one experiment name may be specified." >&2
        usage
        exit 1
      fi
      EXP_NAME="$1"
      ;;
  esac
  shift
done

case "${TARGET}" in
  dbfs|postgres|both)
    ;;
  *)
    echo "Invalid target: ${TARGET}" >&2
    usage
    exit 1
    ;;
esac

case "${WORKLOAD}" in
  tpcc)
    DBFS_STATS_CONFIG="${TPCC_DBFS_STATS_CONFIG}"
    DBFS_THROUGHPUT_CONFIG="${TPCC_DBFS_THROUGHPUT_CONFIG}"
    POSTGRES_STATS_CONFIG="${TPCC_POSTGRES_STATS_CONFIG}"
    POSTGRES_THROUGHPUT_CONFIG="${TPCC_POSTGRES_THROUGHPUT_CONFIG}"
    ;;
  ycsb)
    DBFS_STATS_CONFIG="${YCSB_DBFS_STATS_CONFIG}"
    DBFS_THROUGHPUT_CONFIG="${YCSB_DBFS_THROUGHPUT_CONFIG}"
    POSTGRES_STATS_CONFIG="${YCSB_POSTGRES_STATS_CONFIG}"
    POSTGRES_THROUGHPUT_CONFIG="${YCSB_POSTGRES_THROUGHPUT_CONFIG}"
    ;;
  *)
    echo "Invalid workload: ${WORKLOAD}" >&2
    usage
    exit 1
    ;;
esac

if [[ "${MODE}" == "throughput" ]]; then
  DBFS_CONFIG="${DBFS_THROUGHPUT_CONFIG}"
  POSTGRES_CONFIG="${POSTGRES_THROUGHPUT_CONFIG}"
  DEFAULT_STORAGE_LOG_LEVEL="warn"
  DEFAULT_BUFFER_POOL_STATS_INTERVAL_MS="0"
  DEFAULT_BUFFER_POOL_EVENT_LOG="0"
else
  DBFS_CONFIG="${DBFS_STATS_CONFIG}"
  POSTGRES_CONFIG="${POSTGRES_STATS_CONFIG}"
  DEFAULT_STORAGE_LOG_LEVEL="info"
  DEFAULT_BUFFER_POOL_STATS_INTERVAL_MS="1000"
  DEFAULT_BUFFER_POOL_EVENT_LOG="1"
fi

DBFS_RUNNER_COMMAND=("${RUNNER}" --workload "${WORKLOAD}" -c "${DBFS_CONFIG}")
if [[ "${MODE}" == "throughput" ]]; then
  DBFS_RUNNER_COMMAND+=(--profile-perf)
fi

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
RUN_LABEL="${TIMESTAMP}"

if [[ -n "${EXP_NAME}" ]]; then
  RUN_LABEL+="_${EXP_NAME}"
fi

trap cleanup EXIT

log "run_label=${RUN_LABEL}"
log "mode=${MODE}"
log "workload=${WORKLOAD}"
log "target=${TARGET}"
if [[ "${TARGET}" == "dbfs" || "${TARGET}" == "both" ]]; then
  log "dbfs_config=${DBFS_CONFIG}"
fi
if [[ "${TARGET}" == "postgres" || "${TARGET}" == "both" ]]; then
  log "postgres_config=${POSTGRES_CONFIG}"
fi
log "resetting compose services"
cleanup

if [[ "${TARGET}" == "dbfs" || "${TARGET}" == "both" ]]; then
  log "starting dbfs benchmark"
  DBFS_LOG_LEVEL="${DBFS_LOG_LEVEL:-warn}" \
  DBFS_SERVER_LOG_LEVEL="${DBFS_SERVER_LOG_LEVEL:-warn}" \
  DBFS_INDEX_LOG_LEVEL="${DBFS_INDEX_LOG_LEVEL:-warn}" \
  DBFS_STORAGE_LOG_LEVEL="${DBFS_STORAGE_LOG_LEVEL:-${DEFAULT_STORAGE_LOG_LEVEL}}" \
  DBFS_BUFFER_POOL_LOG_STATS_EVERY_MS="${DBFS_BUFFER_POOL_LOG_STATS_EVERY_MS:-${DEFAULT_BUFFER_POOL_STATS_INTERVAL_MS}}" \
  DBFS_BUFFER_POOL_EVENT_LOG="${DBFS_BUFFER_POOL_EVENT_LOG:-${DEFAULT_BUFFER_POOL_EVENT_LOG}}" \
  DBFS_OPERATOR_LOG_ROWS="${DBFS_OPERATOR_LOG_ROWS:-0}" \
  RUN_LABEL="${RUN_LABEL}" \
    "${DBFS_RUNNER_COMMAND[@]}"
fi

if [[ "${TARGET}" == "postgres" || "${TARGET}" == "both" ]]; then
  log "starting postgres benchmark"
  RUN_LABEL="${RUN_LABEL}" \
    "${RUNNER}" \
    --workload "${WORKLOAD}" \
    -c "${POSTGRES_CONFIG}"
fi

log "completed run_label=${RUN_LABEL}"
