#!/usr/bin/env bash

# Run BenchBase TPC-C against a PostgreSQL Docker container.
#
# This script is intentionally minimal and focuses on wiring pieces together.
# It assumes that:
#   - Docker is installed and usable.
#   - A BenchBase distribution with the `postgres` profile is available and
#     exposed via $BENCHBASE_HOME (directory containing benchbase.jar).
#   - A BenchBase config file for PostgreSQL TPC-C exists at
#       benchmarking/config/postgres_tpcc_local.xml
#     (you are expected to derive it from BenchBase's official
#      config/postgres/sample_tpcc_config.xml).
#
# Usage (environment variables):
#   BENCHBASE_HOME=...</path/to/benchbase-postgres>
#   WAREHOUSES=<number of warehouses>    # required
#   THREADS=<number of client threads>    # required
#   DURATION=<benchmark duration seconds> # required
#   RESULTS_ROOT=benchmarking/results     # optional; default shown below
#
# Then run:
#   ./benchmarking/scripts/run_postgres_tpcc_docker.sh
#
# NOTE: This is a starting point; you can customize container name,
# ports, database name, and how BenchBase is invoked.

set -euo pipefail

# ---- Configuration knobs -------------------------------------------------

CONTAINER_NAME=${CONTAINER_NAME:-"dbfs-benchbase-postgres"}
POSTGRES_IMAGE=${POSTGRES_IMAGE:-"postgres:latest"}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_DB=${POSTGRES_DB:-"tpcc"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-"postgres"}

BENCHBASE_HOME=${BENCHBASE_HOME:-}
CONFIG_PATH=${CONFIG_PATH:-"benchmarking/config/postgres_tpcc_local.xml"}

# Require core benchmark parameters to be explicit (no hard-coded defaults).
: "${WAREHOUSES:?Set WAREHOUSES (number of TPC-C warehouses)}"
: "${THREADS:?Set THREADS (number of client threads)}"
: "${DURATION:?Set DURATION (benchmark duration in seconds)}"

RESULTS_ROOT=${RESULTS_ROOT:-"benchmarking/results"}
WORKLOAD_NAME="tpcc"
ENGINE_NAME="postgres"
RUN_LABEL=${RUN_LABEL:-"$(date +%Y%m%d-%H%M%S)"}
RESULTS_DIR="${RESULTS_ROOT}/${WORKLOAD_NAME}/${ENGINE_NAME}/${RUN_LABEL}"

# ---- Sanity checks --------------------------------------------------------

if [[ -z "${BENCHBASE_HOME}" ]]; then
  echo "ERROR: BENCHBASE_HOME is not set. It must point to a directory containing benchbase.jar." >&2
  exit 1
fi

if [[ ! -f "${BENCHBASE_HOME}/benchbase.jar" ]]; then
  echo "ERROR: benchbase.jar not found under BENCHBASE_HOME=${BENCHBASE_HOME}" >&2
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "ERROR: BenchBase config file not found at ${CONFIG_PATH}." >&2
  echo "       Please create it based on BenchBase's sample_tpcc_config.xml." >&2
  exit 1
fi

mkdir -p "${RESULTS_DIR}"

# ---- Start PostgreSQL container (if not already running) ------------------

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "Starting PostgreSQL container '${CONTAINER_NAME}' using image ${POSTGRES_IMAGE}..."
  docker run -d --rm \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -e POSTGRES_USER="${POSTGRES_USER}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    -p "${POSTGRES_PORT}:5432" \
    "${POSTGRES_IMAGE}" >/dev/null
else
  echo "PostgreSQL container '${CONTAINER_NAME}' already running; reusing it."
fi

# ---- Wait for PostgreSQL to become ready ---------------------------------

echo "Waiting for PostgreSQL to accept connections on port ${POSTGRES_PORT}..."
for i in {1..60}; do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ERROR: PostgreSQL did not become ready in time." >&2
    exit 1
  fi
done

# ---- Run BenchBase TPC-C --------------------------------------------------

JDBC_URL="jdbc:postgresql://localhost:${POSTGRES_PORT}/${POSTGRES_DB}"

echo "Running BenchBase TPC-C against ${JDBC_URL}..."

echo "  WAREHOUSES=${WAREHOUSES} THREADS=${THREADS} DURATION=${DURATION}s"

eval_java_cmd=(
  java
  -jar "${BENCHBASE_HOME}/benchbase.jar"
  -b "${WORKLOAD_NAME}"
  -c "${CONFIG_PATH}"
  --create=true
  --load=true
  --execute=true
  -d "${RESULTS_DIR}"
)

# The actual scale factor, threads, and duration are normally specified
# inside the BenchBase config file. If you want to override them via
# command-line options, you can extend this script to add the appropriate
# flags here.

"${eval_java_cmd[@]}"

echo "BenchBase run completed. Results should be under: ${RESULTS_DIR}"

echo "NOTE: This script does not stop the PostgreSQL container automatically."
echo "      You can stop it manually with: docker stop ${CONTAINER_NAME}"
