# Benchmarking dbfs with BenchBase

This directory contains helper configuration, scripts, results, and notebooks
for benchmarking **dbfs** and comparing it against **PostgreSQL** using
[BenchBase](https://github.com/cmu-db/benchbase), focusing on the **TPC-C** workload.

BenchBase itself (jar and configs) is assumed to be available in your
environment (or documented separately as a "build environment"). This
README does **not** describe how to build BenchBase; it only explains how
to wire an existing BenchBase distribution to PostgreSQL (and later dbfs).

## 1. Requirements (high level)

- Docker
  - Used to run a `postgres:latest` container for baseline measurements.
- A pre-built BenchBase distribution with the `postgres` profile
  - Exposed via an environment variable like `BENCHBASE_HOME`, or any
    other convention you prefer.
- Python and Jupyter
  - Used to analyze and visualize results under `notebooks/`.

Details of Docker commands and BenchBase invocations will be added as the
scripts and configs in this directory are implemented.

## 2. Directory layout

- `config/`
  - BenchBase configuration files for TPC-C.
  - Example (planned):
    - `postgres_tpcc_local.xml` – TPC-C config for PostgreSQL (Docker).
    - `dbfs_tpcc_local.xml` – TPC-C config for dbfs (future work).
- `scripts/`
  - Shell scripts that orchestrate benchmark runs.
  - Example (planned):
    - `run_postgres_tpcc_docker.sh` – start a PostgreSQL container,
      run BenchBase TPC-C with a config from `config/`, and store
      results under `results/`.
    - `run_dbfs_tpcc.sh` – run TPC-C against dbfs once a dbfs
      server/JDBC driver is available.
- `results/`
  - Raw outputs produced by BenchBase runs.
  - Recommended structure (example):
    - `results/tpcc/postgres/<run-label>/...`
    - `results/tpcc/dbfs/<run-label>/...`
- `notebooks/`
  - Jupyter notebooks for analysis and visualization.
  - Example (planned):
    - `tpcc_baseline_analysis.ipynb` – analyze PostgreSQL baseline runs.
    - `tpcc_dbfs_comparison.ipynb` – compare PostgreSQL vs dbfs for TPC-C.

## 3. Typical workflow (conceptual)

1. Ensure the following are available:
   - Docker and the `postgres:latest` image (or another supported tag).
   - A ready-to-run BenchBase distribution (e.g., via `BENCHBASE_HOME`).
2. Use a script from `scripts/` (to be added) to:
   - start a PostgreSQL container for benchmarking,
   - run BenchBase TPC-C with a config from `config/`, and
   - write results into a new subdirectory under `results/`.
3. Open a notebook from `notebooks/` in Jupyter and point it at the
   corresponding `results/` subdirectory to visualize TPS, latency
   distributions, and other metrics.
4. Once dbfs is wired into BenchBase (via a server/JDBC driver), run
   equivalent TPC-C configurations against dbfs and analyze the results
   side-by-side with the PostgreSQL baseline.

Sections describing concrete Docker commands, BenchBase CLI invocations,
and notebook usage will be filled in as the implementation proceeds.
