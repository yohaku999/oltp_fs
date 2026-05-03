#!/usr/bin/env bash

# Run BenchBase TPC-C via docker compose:
#   - PostgreSQL as one compose service
#   - BenchBase as another compose service

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BENCHMARKING_ROOT="${REPO_ROOT}/benchmarking"
RESULTS_ROOT="${BENCHMARKING_ROOT}/results"
COMPOSE_FILE="${BENCHMARKING_ROOT}/docker-compose.yaml"
CONFIG_ROOT="${BENCHMARKING_ROOT}/benchbase-config"
DBFS_JDBC_ROOT="${REPO_ROOT}/dbfs-jdbc"
BENCHBASE_CONFIG_PATH=""
POSTGRES_DB_NAME="${POSTGRES_DB:-benchbase}"
POSTGRES_USER_NAME="${POSTGRES_USER:-admin}"

usage() {
  echo "Usage: $0 --config <path>" >&2
}

ensure_postgres_query_stats_ready() {
  "${compose_cmd[@]}" exec -T postgres psql -U "${POSTGRES_USER_NAME}" -d "${POSTGRES_DB_NAME}" \
    -v ON_ERROR_STOP=1 \
    -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements;' \
    -c 'SELECT pg_stat_statements_reset();' >/dev/null
}

export_postgres_query_stats() {
  local host_results_dir="$1"
  local output_path="${host_results_dir}/postgres_query_trace.csv"

  mkdir -p "${host_results_dir}"
  "${compose_cmd[@]}" exec -T postgres psql -U "${POSTGRES_USER_NAME}" -d "${POSTGRES_DB_NAME}" \
    -v ON_ERROR_STOP=1 \
    -c "\\copy (
      SELECT
        query,
        calls,
        total_exec_time,
        mean_exec_time,
        min_exec_time,
        max_exec_time,
        rows,
        shared_blks_hit,
        shared_blks_read,
        temp_blks_read,
        temp_blks_written
      FROM pg_stat_statements
      WHERE dbid = (SELECT oid FROM pg_database WHERE datname = '${POSTGRES_DB_NAME}')
        AND query NOT LIKE 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements%'
        AND query NOT LIKE 'SELECT pg_stat_statements_reset()%'
      ORDER BY total_exec_time DESC
    ) TO STDOUT WITH CSV HEADER" >"${output_path}"
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

if grep -q '<driver>dev.yohaku.dbfs.jdbc.DbfsDriver</driver>' "${BENCHBASE_CONFIG_PATH}"; then
  echo "Building dbfs-jdbc jar for BenchBase classpath..."
  (cd "${DBFS_JDBC_ROOT}" && mvn -q -DskipTests package)
fi

CONTAINER_CONFIG="/benchbase/benchbase-config/${BENCHBASE_CONFIG_PATH#"${CONFIG_ROOT}/"}"

WORKLOAD="tpcc"
if grep -q '<driver>dev.yohaku.dbfs.jdbc.DbfsDriver</driver>' "${BENCHBASE_CONFIG_PATH}"; then
  ENGINE="dbfs"
elif grep -q '<driver>org.postgresql.Driver</driver>' "${BENCHBASE_CONFIG_PATH}"; then
  ENGINE="postgres"
else
  echo "Unable to determine benchmark engine from ${BENCHBASE_CONFIG_PATH}" >&2
  exit 1
fi
RUN_LABEL=${RUN_LABEL:-"$(date +%Y%m%d-%H%M%S)"}
RESULTS_SUBDIR="${RUN_LABEL}/${WORKLOAD}/${ENGINE}"
HOST_RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_SUBDIR}"
RESULTS_DIR_CONTAINER="/benchbase/results/${RESULTS_SUBDIR}"

mkdir -p "${RESULTS_ROOT}"

compose_cmd=(docker compose -f "${COMPOSE_FILE}")
benchbase_env_args=()

if [[ "${ENGINE}" == "dbfs" ]]; then
  trace_timing_file=${DBFS_SQL_TRACE_TIMING_FILE:-"${RESULTS_DIR_CONTAINER}/dbfs_query_trace.csv"}
  benchbase_env_args+=( -e "DBFS_SQL_TRACE_TIMING_FILE=${trace_timing_file}" )
fi

if [[ -n "${DBFS_SQL_TRACE_SAMPLE_FILE:-}" ]]; then
  benchbase_env_args+=( -e "DBFS_SQL_TRACE_SAMPLE_FILE=${DBFS_SQL_TRACE_SAMPLE_FILE}" )
fi

echo "Starting PostgreSQL compose service..."
"${compose_cmd[@]}" up -d postgres

echo "Waiting for PostgreSQL compose service to become ready..."
for i in {1..60}; do
  if "${compose_cmd[@]}" exec -T postgres pg_isready -U "${POSTGRES_USER_NAME}" -d "${POSTGRES_DB_NAME}" >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ERROR: PostgreSQL did not become ready in time." >&2
    exit 1
  fi
done

if [[ "${ENGINE}" == "postgres" ]]; then
  echo "Resetting PostgreSQL query stats..."
  ensure_postgres_query_stats_ready
fi

echo "Running BenchBase compose service..."
echo "  Compare:    ${RUN_LABEL}"
echo "  Workload:   ${WORKLOAD}"
echo "  Engine:     ${ENGINE}"
echo "  Config:     ${CONTAINER_CONFIG}"
echo "  Results:    ${RESULTS_DIR_CONTAINER} (mounted to ${RESULTS_ROOT})"

if [[ ${#benchbase_env_args[@]} -gt 0 ]]; then
  "${compose_cmd[@]}" run --rm "${benchbase_env_args[@]}" benchbase \
    -b "${WORKLOAD}" \
    -c "${CONTAINER_CONFIG}" \
    --create=true \
    --load=true \
    --execute=true \
    -d "${RESULTS_DIR_CONTAINER}"
else
  "${compose_cmd[@]}" run --rm benchbase \
    -b "${WORKLOAD}" \
    -c "${CONTAINER_CONFIG}" \
    --create=true \
    --load=true \
    --execute=true \
    -d "${RESULTS_DIR_CONTAINER}"
fi

if [[ "${ENGINE}" == "postgres" ]]; then
  echo "Exporting PostgreSQL query stats..."
  export_postgres_query_stats "${HOST_RESULTS_DIR}"
fi

echo "BenchBase run completed. Results should be under:"
echo "  ${HOST_RESULTS_DIR}"

echo "NOTE: RBMS remains running as a compose service."
echo "      You can stop it manually with: docker compose -f ${COMPOSE_FILE} down"
