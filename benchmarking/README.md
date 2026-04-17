# Benchmarking dbfs with BenchBase

This directory contains helper configuration, scripts, results, and notebooks
for benchmarking **dbfs** and comparing it against **PostgreSQL** using
[BenchBase](https://github.com/cmu-db/benchbase), focusing on the **TPC-C** workload.

The PostgreSQL baseline flow is docker compose based. PostgreSQL and BenchBase
both run in containers, and BenchBase reaches PostgreSQL through the compose
service name `postgres`.

Repository-managed benchmark configs are mounted into the BenchBase container
under `/benchbase/user-config` so they do not overwrite BenchBase's built-in
`/benchbase/config` directory.

## 1. Requirements

- Docker with `docker compose`
- Python and Jupyter if you want to analyze output files under `results/`

## 2. Directory layout

- `docker-compose.yaml`
  - Docker compose definition for the PostgreSQL baseline flow.
- `config/`
  - BenchBase configuration files.
  - Example:
    - `postgres_tpcc_docker.xml` – repo-managed TPCC config for the
      compose-based PostgreSQL baseline.
    - `dbfs_tpcc_docker.xml` – future config for the compose-based dbfs flow.
- `scripts/`
  - Shell wrappers around the benchmark flow.
  - Example:
    - `run_postgres_tpcc_docker.sh` – start PostgreSQL with docker compose,
      wait for readiness, and run BenchBase TPCC. By default it uses
      `config/postgres_tpcc_docker.xml`, and you can switch only the
      benchmark target config with `-c <path>`.
    - `run_dbfs_tpcc.sh` – run TPC-C against dbfs once a dbfs
      server/JDBC driver is available.
- `results/`
  - Raw outputs produced by BenchBase runs.
  - Recommended structure:
    - `results/tpcc/postgres/<run-label>/...`
    - `results/tpcc/dbfs/<run-label>/...`
- `notebooks/`
  - Jupyter notebooks for analysis and visualization.

## 3. Typical workflow

1. Ensure Docker is available.
2. Run `scripts/run_postgres_tpcc_docker.sh`.
3. Inspect results under `results/tpcc/postgres/<run-label>/`.
4. Open notebooks as needed to visualize throughput and latency.

If you want to keep the same compose environment and change only the
BenchBase config file, place another XML file under `config/` and run:

`./benchmarking/scripts/run_postgres_tpcc_docker.sh -c benchmarking/config/your_config.xml`

The PostgreSQL TPCC config in this repository uses:

- host: `postgres`
- port: `5432`
- database: `benchbase`
- user: `admin`
- password: `password`

Those values match the compose services defined in `docker-compose.yaml`.
