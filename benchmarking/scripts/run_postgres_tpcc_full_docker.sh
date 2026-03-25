#!/usr/bin/env bash

# Run BenchBase TPC-C entirely in Docker:
#   - PostgreSQL in one container
#   - BenchBase in another container
#
# This script assumes:
#   - Docker is installed and usable.
#   - The BenchBase Docker image `benchbase.azurecr.io/benchbase` is
#     pullable from your environment.
#
# It does *not* rely on a local Java runtime or a local BenchBase jar.
#
# Usage (env vars are optional unless marked otherwise):
#   WORKLOAD=tpcc (fixed in this script)
#   ENGINE=postgres (fixed in this script)
#   RUN_LABEL=...   (optional; default: current timestamp)
#
# Example:
#   ./benchmarking/scripts/run_postgres_tpcc_full_docker.sh
#
# Notes:
#   - Scale factor, terminals, and duration are controlled via the
#     BenchBase config XML, not via this script.
#   - The script creates (or reuses) a user-defined Docker network so
#     that BenchBase can reach PostgreSQL by container name.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RESULTS_ROOT="${REPO_ROOT}/benchmarking/results"

NETWORK_NAME=${NETWORK_NAME:-"dbfs-bench-net"}
PG_CONTAINER=${PG_CONTAINER:-"dbfs-bench-pg"}
POSTGRES_IMAGE=${POSTGRES_IMAGE:-"postgres:latest"}
POSTGRES_DB=${POSTGRES_DB:-"benchbase"}
POSTGRES_USER=${POSTGRES_USER:-"postgres"}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-"postgres"}

BENCHBASE_IMAGE=${BENCHBASE_IMAGE:-"benchbase.azurecr.io/benchbase"}
BENCHBASE_PROFILE=${BENCHBASE_PROFILE:-"postgres"}

WORKLOAD="tpcc"
ENGINE="postgres"
RUN_LABEL=${RUN_LABEL:-"$(date +%Y%m%d-%H%M%S)"}
RESULTS_SUBDIR="${WORKLOAD}/${ENGINE}/${RUN_LABEL}"

# ---- Sanity checks --------------------------------------------------------

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker command not found. Please install Docker." >&2
  exit 1
fi

mkdir -p "${RESULTS_ROOT}"

# ---- Ensure Docker network exists ----------------------------------------

if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
  echo "Creating Docker network '${NETWORK_NAME}'..."
  docker network create "${NETWORK_NAME}" >/dev/null
else
  echo "Docker network '${NETWORK_NAME}' already exists; reusing it."
fi

# ---- Start PostgreSQL container (if not already running) ------------------

if ! docker ps --format '{{.Names}}' | grep -q "^${PG_CONTAINER}$"; then
  echo "Starting PostgreSQL container '${PG_CONTAINER}' on network '${NETWORK_NAME}'..."
  docker run -d --rm \
    --name "${PG_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -e POSTGRES_DB="${POSTGRES_DB}" \
    -e POSTGRES_USER="${POSTGRES_USER}" \
    -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
    "${POSTGRES_IMAGE}" >/dev/null
else
  echo "PostgreSQL container '${PG_CONTAINER}' already running; reusing it."
fi

# ---- Wait for PostgreSQL to become ready ---------------------------------

echo "Waiting for PostgreSQL in container '${PG_CONTAINER}' to become ready..."
for i in {1..60}; do
  if docker exec "${PG_CONTAINER}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  sleep 1
  if [[ $i -eq 60 ]]; then
    echo "ERROR: PostgreSQL did not become ready in time." >&2
    exit 1
  fi
done

# ---- Run BenchBase TPC-C container ---------------------------------------

RESULTS_DIR_CONTAINER="/benchbase/results/${RESULTS_SUBDIR}"

echo "Running BenchBase container in the PostgreSQL container's network namespace..."
echo "  Image:      ${BENCHBASE_IMAGE} (profile=${BENCHBASE_PROFILE})"
echo "  Workload:   ${WORKLOAD}"

echo "  Config:     /benchbase/config/postgres/sample_tpcc_config.xml (built-in)"
echo "  Results:    ${RESULTS_DIR_CONTAINER} (mounted to ${RESULTS_ROOT})"

# The built-in sample TPCC config inside the BenchBase image is used here.
# It is preconfigured for PostgreSQL at jdbc:postgresql://localhost:5432/benchbase
# with a default schema. By running the BenchBase container in the same
# network namespace as the PostgreSQL container (--network container:...),
# "localhost" inside BenchBase resolves to the Postgres container itself.

docker run --rm \
  --network "container:${PG_CONTAINER}" \
  -e "BENCHBASE_PROFILE=${BENCHBASE_PROFILE}" \
  -v "${RESULTS_ROOT}":/benchbase/results \
  "${BENCHBASE_IMAGE}" \
  -b "${WORKLOAD}" \
  -c "/benchbase/config/postgres/sample_tpcc_config.xml" \
  --create=true \
  --load=true \
  --execute=true \
  -d "${RESULTS_DIR_CONTAINER}"

HOST_RESULTS_DIR="${RESULTS_ROOT}/${RESULTS_SUBDIR}"

echo "BenchBase run completed. Results should be under:"
echo "  ${HOST_RESULTS_DIR}"

echo "NOTE: This script does not stop the PostgreSQL container automatically."
echo "      You can stop it manually with: docker stop ${PG_CONTAINER}"
