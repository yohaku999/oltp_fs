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
DBFS_DRIVER_CLASS='dev.yohaku.dbfs.jdbc.DbfsDriver'
TRACING_POSTGRES_DRIVER_CLASS='dev.yohaku.dbfs.jdbc.TracingPostgresDriver'
ENABLE_PERF_PROFILE=0
PERF_FREQUENCY="${PERF_FREQUENCY:-199}"
PERF_CALL_GRAPH="${PERF_CALL_GRAPH:-fp}"
PERF_MMAP_PAGES="${PERF_MMAP_PAGES:-64}"
FLAMEGRAPH_WIDTH="${FLAMEGRAPH_WIDTH:-1800}"
FLAMEGRAPH_FONT_SIZE="${FLAMEGRAPH_FONT_SIZE:-14}"
FLAMEGRAPH_MIN_WIDTH="${FLAMEGRAPH_MIN_WIDTH:-0.5}"

usage() {
  echo "Usage: $0 --config <path> [--profile-perf] [--perf-frequency <hz>] [--perf-mmap-pages <pages>]" >&2
}

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
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
    --profile-perf)
      ENABLE_PERF_PROFILE=1
      shift
      ;;
    --perf-frequency)
      if [[ $# -lt 2 ]]; then
        echo "Missing argument for $1" >&2
        usage
        exit 1
      fi
      PERF_FREQUENCY="$2"
      shift 2
      ;;
    --perf-mmap-pages)
      if [[ $# -lt 2 ]]; then
        echo "Missing argument for $1" >&2
        usage
        exit 1
      fi
      PERF_MMAP_PAGES="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 --config <path> [--profile-perf] [--perf-frequency <hz>] [--perf-mmap-pages <pages>]"
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

if ! is_positive_integer "${PERF_FREQUENCY}"; then
  echo "perf frequency must be a positive integer: ${PERF_FREQUENCY}" >&2
  exit 1
fi

if ! is_positive_integer "${PERF_MMAP_PAGES}"; then
  echo "perf mmap pages must be a positive integer: ${PERF_MMAP_PAGES}" >&2
  exit 1
fi

if [[ "${BENCHBASE_CONFIG_PATH}" != /* ]]; then
  BENCHBASE_CONFIG_PATH="$(cd "$(dirname "${BENCHBASE_CONFIG_PATH}")" && pwd)/$(basename "${BENCHBASE_CONFIG_PATH}")"
fi

if [[ "${BENCHBASE_CONFIG_PATH}" != "${CONFIG_ROOT}"/* ]]; then
  echo "Config path must be under ${CONFIG_ROOT}" >&2
  exit 1
fi

if grep -Eq "<driver>(${DBFS_DRIVER_CLASS}|${TRACING_POSTGRES_DRIVER_CLASS})</driver>" "${BENCHBASE_CONFIG_PATH}"; then
  echo "Building dbfs-jdbc jar for BenchBase classpath..."
  (cd "${DBFS_JDBC_ROOT}" && mvn -q -DskipTests package dependency:copy-dependencies -DincludeScope=runtime)
fi

CONTAINER_CONFIG="/benchbase/benchbase-config/${BENCHBASE_CONFIG_PATH#"${CONFIG_ROOT}/"}"

WORKLOAD="tpcc"
if grep -q "<driver>${DBFS_DRIVER_CLASS}</driver>" "${BENCHBASE_CONFIG_PATH}"; then
  ENGINE="dbfs"
elif grep -Eq "<driver>(${TRACING_POSTGRES_DRIVER_CLASS}|org.postgresql.Driver)</driver>" "${BENCHBASE_CONFIG_PATH}"; then
  ENGINE="postgres"
else
  echo "Unable to determine benchmark engine from ${BENCHBASE_CONFIG_PATH}" >&2
  exit 1
fi
RUN_LABEL=${RUN_LABEL:-"$(date +%Y%m%d-%H%M%S)"}
RESULTS_SUBDIR="${RUN_LABEL}/${WORKLOAD}/${ENGINE}"
HOST_RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_SUBDIR}"
RESULTS_DIR_CONTAINER="/benchbase/results/${RESULTS_SUBDIR}"
SETUP_RESULTS_DIR_CONTAINER="${RESULTS_DIR_CONTAINER}/setup"
ARTIFACTS_DIR_CONTAINER="/artifacts/${RESULTS_SUBDIR}"
PERF_DATA_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs.perf.data"
PERF_RECORD_LOG_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs.perf.record.log"
PERF_SCRIPT_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs.perf.script"
PERF_FOLDED_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs.perf.folded"
PERF_SIMPLIFIED_FOLDED_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs.perf.simplified.folded"
FLAMEGRAPH_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs_flamegraph.svg"
READ_FLAMEGRAPH_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs_flamegraph_read.svg"
WRITE_FLAMEGRAPH_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs_flamegraph_write.svg"
HOST_PERF_DATA_FILE="${HOST_RESULTS_DIR}/dbfs.perf.data"
HOST_PERF_SCRIPT_FILE="${HOST_RESULTS_DIR}/dbfs.perf.script"
HOST_PERF_FOLDED_FILE="${HOST_RESULTS_DIR}/dbfs.perf.folded"
HOST_PERF_SIMPLIFIED_FOLDED_FILE="${HOST_RESULTS_DIR}/dbfs.perf.simplified.folded"
HOST_FLAMEGRAPH_FILE="${HOST_RESULTS_DIR}/dbfs_flamegraph.svg"
HOST_READ_FLAMEGRAPH_FILE="${HOST_RESULTS_DIR}/dbfs_flamegraph_read.svg"
HOST_WRITE_FLAMEGRAPH_FILE="${HOST_RESULTS_DIR}/dbfs_flamegraph_write.svg"
HOST_DBFS_BINARY_FILE="${HOST_RESULTS_DIR}/dbfs_server"
HOST_BENCHBASE_LOG_FILE="${HOST_RESULTS_DIR}/benchbase.log"
HOST_BENCHBASE_SETUP_LOG_FILE="${HOST_RESULTS_DIR}/benchbase_setup.log"
HOST_BENCHBASE_EXECUTE_LOG_FILE="${HOST_RESULTS_DIR}/benchbase_execute.log"
HOST_DBFS_COMPOSE_LOG_FILE="${HOST_RESULTS_DIR}/dbfs.compose.log"
HOST_DBFS_OPERATOR_LOG_FILE="${HOST_RESULTS_DIR}/dbfs.operator_rows.log"
HOST_POSTGRES_COMPOSE_LOG_FILE="${HOST_RESULTS_DIR}/postgres.compose.log"
RUN_LOG_SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p "${HOST_RESULTS_DIR}"

DBFS_OPERATOR_LOG_FILE_CONTAINER="${ARTIFACTS_DIR_CONTAINER}/dbfs.operator_rows.log"

compose_cmd=(docker compose -f "${COMPOSE_FILE}")
benchbase_env_args=()
perf_started=0
runtime_logs_collected=0

if [[ "${ENGINE}" == "dbfs" ]]; then
  export DBFS_OPERATOR_LOG_FILE="${DBFS_OPERATOR_LOG_FILE:-${DBFS_OPERATOR_LOG_FILE_CONTAINER}}"
  rm -f "${HOST_DBFS_OPERATOR_LOG_FILE}"
fi

collect_compose_logs() {
  local service_name="$1"
  local host_log_file="$2"
  local container_id

  container_id="$("${compose_cmd[@]}" ps -q "${service_name}" 2>/dev/null)"
  if [[ -z "${container_id}" ]]; then
    return
  fi

  "${compose_cmd[@]}" logs --no-color --timestamps --since "${RUN_LOG_SINCE}" "${service_name}" >"${host_log_file}" 2>&1 || true
}

collect_runtime_logs() {
  if [[ "${runtime_logs_collected}" -eq 1 ]]; then
    return
  fi

  if [[ "${ENGINE}" == "dbfs" ]]; then
    collect_compose_logs dbfs "${HOST_DBFS_COMPOSE_LOG_FILE}"
  else
    collect_compose_logs postgres "${HOST_POSTGRES_COMPOSE_LOG_FILE}"
  fi

  runtime_logs_collected=1
}

cleanup() {
  local exit_code=$?

  if [[ "${perf_started}" -eq 1 ]]; then
    "${compose_cmd[@]}" exec -T dbfs sh -lc "if test -s /tmp/dbfs-perf.pid; then kill -INT \$(cat /tmp/dbfs-perf.pid) 2>/dev/null || true; else pkill -INT -f '^perf record .*dbfs\.perf\.data' 2>/dev/null || true; fi" >/dev/null 2>&1 || true
  fi

  collect_runtime_logs

  trap - EXIT
  exit "${exit_code}"
}

start_perf_profile() {
  echo "Starting perf recording in dbfs container..."
  "${compose_cmd[@]}" exec -T dbfs sh -lc "mkdir -p '${ARTIFACTS_DIR_CONTAINER}' && rm -f '${PERF_DATA_CONTAINER}' '${PERF_RECORD_LOG_CONTAINER}' /tmp/dbfs-perf.pid && { nohup perf record -F '${PERF_FREQUENCY}' -m '${PERF_MMAP_PAGES}' -g --call-graph '${PERF_CALL_GRAPH}' -p \"\$(pgrep -xo dbfs_server)\" -o '${PERF_DATA_CONTAINER}' >'${PERF_RECORD_LOG_CONTAINER}' 2>&1 & pid=\$!; echo \$pid >/tmp/dbfs-perf.pid; }"
  "${compose_cmd[@]}" exec -T dbfs sh -lc "test -s /tmp/dbfs-perf.pid && kill -0 \$(cat /tmp/dbfs-perf.pid)"
  perf_started=1
}

stop_perf_profile() {
  if [[ "${perf_started}" -eq 0 ]]; then
    return
  fi

  echo "Stopping perf recording..."
  "${compose_cmd[@]}" exec -T dbfs sh -lc "if test -s /tmp/dbfs-perf.pid; then kill -INT \$(cat /tmp/dbfs-perf.pid) 2>/dev/null || true; else pkill -INT -f '^perf record .*dbfs\.perf\.data' 2>/dev/null || true; fi"

  for _ in {1..30}; do
    if "${compose_cmd[@]}" exec -T dbfs sh -lc "if test -s /tmp/dbfs-perf.pid; then ! kill -0 \$(cat /tmp/dbfs-perf.pid) 2>/dev/null; else true; fi" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  "${compose_cmd[@]}" exec -T dbfs sh -lc "rm -f /tmp/dbfs-perf.pid && test -s '${PERF_DATA_CONTAINER}'"
  perf_started=0
}

generate_flamegraph() {
  local dbfs_container_id

  if [[ ! -s "${HOST_PERF_DATA_FILE}" ]]; then
    echo "ERROR: perf data was not written to ${HOST_PERF_DATA_FILE}" >&2
    return 1
  fi

  dbfs_container_id="$("${compose_cmd[@]}" ps -q dbfs)"
  if [[ -z "${dbfs_container_id}" ]]; then
    echo "ERROR: dbfs container is not running; cannot copy symbols for perf." >&2
    return 1
  fi

  echo "Copying dbfs symbols for perf script..."
  docker cp "${dbfs_container_id}:/app/dbfs_server" "${HOST_DBFS_BINARY_FILE}"

  echo "Generating flamegraph..."
  "${compose_cmd[@]}" run --rm flamegraph sh -lc "mkdir -p /app && cp '${ARTIFACTS_DIR_CONTAINER}/dbfs_server' /app/dbfs_server && perf script -i '${PERF_DATA_CONTAINER}' > '${PERF_SCRIPT_CONTAINER}' && /opt/FlameGraph/stackcollapse-perf.pl '${PERF_SCRIPT_CONTAINER}' > '${PERF_FOLDED_CONTAINER}' && perl -pe \"s/nlohmann::json_abi_v[0-9_]+::basic_json<[^;]+>/nlohmann::json/g; s/nlohmann::json_abi_v[0-9_]+::detail::lexer<[^;]+>/nlohmann::json::lexer/g; s/std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >/std::string/g; s/std::__do_uninit_copy<[^;]+>/std::vector copy/g; s/std::allocator_traits<[^;]+>::construct/std::allocator construct/g; s/std::__new_allocator<[^;]+>::allocate/std::allocator allocate/g; s/std::__new_allocator<[^;]+>::deallocate/std::allocator deallocate/g; s/std::__detail::_Map_base<[^;]+>::operator\\[\\]/std::map::operator[]/g;\" '${PERF_FOLDED_CONTAINER}' > '${PERF_SIMPLIFIED_FOLDED_CONTAINER}' && /opt/FlameGraph/flamegraph.pl --title 'dbfs ${RUN_LABEL} ${WORKLOAD}' --subtitle 'simplified symbols, wider view' --width '${FLAMEGRAPH_WIDTH}' --fontsize '${FLAMEGRAPH_FONT_SIZE}' --minwidth '${FLAMEGRAPH_MIN_WIDTH}' --countname samples --hash '${PERF_SIMPLIFIED_FOLDED_CONTAINER}' > '${FLAMEGRAPH_CONTAINER}' && grep -E 'executor::read|LoopJoinOperator|AggregateOperator|FilterOperator|IndexScanOperator|SeqScanOperator|HeapFetchOperator' '${PERF_SIMPLIFIED_FOLDED_CONTAINER}' > /tmp/dbfs-read.folded || true && if test -s /tmp/dbfs-read.folded; then /opt/FlameGraph/flamegraph.pl --title 'dbfs ${RUN_LABEL} read path' --subtitle 'read/query-focused stacks' --width '${FLAMEGRAPH_WIDTH}' --fontsize '${FLAMEGRAPH_FONT_SIZE}' --minwidth '${FLAMEGRAPH_MIN_WIDTH}' --countname samples --hash /tmp/dbfs-read.folded > '${READ_FLAMEGRAPH_CONTAINER}'; fi && grep -E 'executor::insert|updateMatchingRows|removeMatchingRows|BTreeCursor|RecordSerializer|heap_file_access::appendCell' '${PERF_SIMPLIFIED_FOLDED_CONTAINER}' > /tmp/dbfs-write.folded || true && if test -s /tmp/dbfs-write.folded; then /opt/FlameGraph/flamegraph.pl --title 'dbfs ${RUN_LABEL} write path' --subtitle 'write/index-focused stacks' --width '${FLAMEGRAPH_WIDTH}' --fontsize '${FLAMEGRAPH_FONT_SIZE}' --minwidth '${FLAMEGRAPH_MIN_WIDTH}' --countname samples --hash /tmp/dbfs-write.folded > '${WRITE_FLAMEGRAPH_CONTAINER}'; fi"
}

run_benchbase() {
  local create_flag="$1"
  local load_flag="$2"
  local execute_flag="$3"
  local results_dir="$4"
  local include_trace_env="$5"
  local log_file="$6"
  local -a cmd=("${compose_cmd[@]}" run --rm)

  if [[ "${include_trace_env}" -eq 1 && ${#benchbase_env_args[@]} -gt 0 ]]; then
    cmd+=("${benchbase_env_args[@]}")
  fi

  cmd+=(benchbase \
    -b "${WORKLOAD}" \
    -c "${CONTAINER_CONFIG}" \
    --create="${create_flag}" \
    --load="${load_flag}" \
    --execute="${execute_flag}" \
    -d "${results_dir}")

  set +e
  "${cmd[@]}" 2>&1 | tee "${log_file}"
  local cmd_status=${PIPESTATUS[0]}
  set -e
  return "${cmd_status}"
}

trap cleanup EXIT

if [[ "${ENGINE}" == "dbfs" ]]; then
  trace_timing_file=${SQL_TRACE_TIMING_FILE:-${DBFS_SQL_TRACE_TIMING_FILE:-"${RESULTS_DIR_CONTAINER}/dbfs_query_trace.csv"}}
  benchbase_env_args+=( -e "SQL_TRACE_TIMING_FILE=${trace_timing_file}" )
elif [[ "${ENGINE}" == "postgres" ]] && grep -q "<driver>${TRACING_POSTGRES_DRIVER_CLASS}</driver>" "${BENCHBASE_CONFIG_PATH}"; then
  trace_timing_file=${SQL_TRACE_TIMING_FILE:-"${RESULTS_DIR_CONTAINER}/postgres_query_trace.csv"}
  benchbase_env_args+=( -e "SQL_TRACE_TIMING_FILE=${trace_timing_file}" )
fi

trace_sample_file=${SQL_TRACE_SAMPLE_FILE:-${DBFS_SQL_TRACE_SAMPLE_FILE:-}}
if [[ -n "${trace_sample_file}" ]]; then
  benchbase_env_args+=( -e "SQL_TRACE_SAMPLE_FILE=${trace_sample_file}" )
fi

if [[ "${ENABLE_PERF_PROFILE}" -eq 1 && "${ENGINE}" != "dbfs" ]]; then
  echo "perf profiling is only supported for the dbfs compose service." >&2
  exit 1
fi

if [[ "${ENABLE_PERF_PROFILE}" -eq 1 && "$(uname -s)" != "Linux" ]]; then
  echo "WARNING: perf inside Docker Desktop may expose fewer counters on $(uname -s)."
fi

if [[ "${ENGINE}" == "postgres" ]]; then
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
else
  echo "Starting dbfs compose service..."
  "${compose_cmd[@]}" up -d --build --force-recreate dbfs

  echo "Waiting for dbfs compose service to become ready..."
  for i in {1..60}; do
    if "${compose_cmd[@]}" exec -T dbfs nc -z localhost 25432 >/dev/null 2>&1; then
      echo "dbfs is ready."
      break
    fi
    sleep 1
    if [[ $i -eq 60 ]]; then
      echo "ERROR: dbfs did not become ready in time." >&2
      exit 1
    fi
  done
fi

echo "Running BenchBase compose service..."
echo "  Compare:    ${RUN_LABEL}"
echo "  Workload:   ${WORKLOAD}"
echo "  Engine:     ${ENGINE}"
echo "  Config:     ${CONTAINER_CONFIG}"
echo "  Results:    ${RESULTS_DIR_CONTAINER} (mounted to ${RESULTS_ROOT})"

if [[ "${ENABLE_PERF_PROFILE}" -eq 1 ]]; then
  echo "  perf data:  ${PERF_DATA_CONTAINER}"
  echo "  setup dir:  ${SETUP_RESULTS_DIR_CONTAINER}"
fi

benchbase_exit_code=0
if [[ "${ENABLE_PERF_PROFILE}" -eq 1 ]]; then
  echo "Preparing BenchBase dataset without perf..."
  if run_benchbase true true false "${SETUP_RESULTS_DIR_CONTAINER}" 0 "${HOST_BENCHBASE_SETUP_LOG_FILE}"; then
    start_perf_profile
    echo "Running BenchBase execute phase with perf..."
    if run_benchbase false false true "${RESULTS_DIR_CONTAINER}" 1 "${HOST_BENCHBASE_EXECUTE_LOG_FILE}"; then
      benchbase_exit_code=0
    else
      benchbase_exit_code=$?
    fi
  else
    benchbase_exit_code=$?
  fi
else
  if run_benchbase true true true "${RESULTS_DIR_CONTAINER}" 1 "${HOST_BENCHBASE_LOG_FILE}"; then
    benchbase_exit_code=0
  else
    benchbase_exit_code=$?
  fi
fi

if [[ "${ENABLE_PERF_PROFILE}" -eq 1 ]]; then
  stop_perf_profile
  generate_flamegraph
fi

collect_runtime_logs

echo "BenchBase run completed. Results should be under:"
echo "  ${HOST_RESULTS_DIR}"

if [[ "${ENABLE_PERF_PROFILE}" -eq 1 ]]; then
  echo "  ${HOST_BENCHBASE_SETUP_LOG_FILE}"
  echo "  ${HOST_BENCHBASE_EXECUTE_LOG_FILE}"
else
  echo "  ${HOST_BENCHBASE_LOG_FILE}"
fi

if [[ "${ENGINE}" == "dbfs" ]]; then
  echo "  ${HOST_DBFS_COMPOSE_LOG_FILE}"
  echo "  ${HOST_DBFS_OPERATOR_LOG_FILE}"
else
  echo "  ${HOST_POSTGRES_COMPOSE_LOG_FILE}"
fi

if [[ "${ENABLE_PERF_PROFILE}" -eq 1 ]]; then
  echo "  ${HOST_PERF_DATA_FILE}"
  echo "  ${HOST_PERF_SCRIPT_FILE}"
  echo "  ${HOST_PERF_FOLDED_FILE}"
  echo "  ${HOST_PERF_SIMPLIFIED_FOLDED_FILE}"
  echo "  ${HOST_FLAMEGRAPH_FILE}"
  echo "  ${HOST_READ_FLAMEGRAPH_FILE}"
  echo "  ${HOST_WRITE_FLAMEGRAPH_FILE}"
fi

echo "NOTE: RBMS remains running as a compose service."
echo "      You can stop it manually with: docker compose -f ${COMPOSE_FILE} down"

if [[ "${benchbase_exit_code}" -ne 0 ]]; then
  exit "${benchbase_exit_code}"
fi
