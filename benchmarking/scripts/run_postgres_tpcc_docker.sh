#!/usr/bin/env bash

# Backward-compatible entry point. New callers should use
# run_benchbase_docker.sh, which supports both TPC-C and YCSB.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/run_benchbase_docker.sh" "$@"
