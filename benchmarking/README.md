# Benchmarking dbfs with BenchBase

This directory contains helper configuration, scripts, results, and notebooks
for benchmarking **dbfs** and comparing it against **PostgreSQL** using
[BenchBase](https://github.com/cmu-db/benchbase), focusing on the **TPC-C** workload.

The PostgreSQL baseline flow is docker compose based. PostgreSQL and BenchBase
both run in containers, and BenchBase reaches PostgreSQL through the compose
service name `postgres`.

Repository-managed benchmark configs are mounted into the BenchBase container
under `/benchbase/benchbase-config` so they do not overwrite BenchBase's built-in
`/benchbase/config` directory.

## Embedded Execution Model

This DBMS currently runs as an embedded library invoked directly from a JDBC
driver. Therefore, the buffer pool is process-local and not shared across
independent client processes.

This design is sufficient for studying storage-engine internals such as page
caching, WAL ordering, B-tree page access, and flush/eviction policies.

However, it does not model a client-server DBMS with a shared global buffer
pool, network protocol overhead, or centralized connection/process management.

```text
Java benchmark / JDBC process
  └─ JNI or native library
       └─ your C++ RDBMS
```

## 1. Requirements

- Docker with `docker compose`
- Python and Jupyter if you want to analyze output files under `results/`

## 2. Directory layout

- `docker-compose.yaml`
  - Docker compose definition for the PostgreSQL baseline flow.
- `benchbase-config/`
  - BenchBase configuration files.
  - Example:
    - `postgres_tpcc_docker.xml` – repo-managed TPCC config for the
      compose-based PostgreSQL baseline.
    - `dbfs_tpcc_docker.xml` – future config for the compose-based dbfs flow.
- `scripts/`
  - Shell wrappers around the benchmark flow.
  - Example:
    - `run_postgres_tpcc_docker.sh` – start PostgreSQL with docker compose,
      wait for readiness, and run BenchBase TPCC. You must pass the
      benchmark target config with `-c <path>`.
    - `run_dbfs_tpcc.sh` – run TPC-C against dbfs once a dbfs
      server/JDBC driver is available.
- `results/`
  - Raw outputs produced by BenchBase runs.
  - Recommended structure:
    - `results/<compare-label>/tpcc/postgres/...`
    - `results/<compare-label>/tpcc/dbfs/...`
  - The compose wrapper also saves BenchBase stdout/stderr in the run result
    directory as `benchbase.log`, or `benchbase_setup.log` and
    `benchbase_execute.log` when `--profile-perf` splits the phases.
  - The wrapper also snapshots compose service logs into the same directory as
    `dbfs.compose.log` or `postgres.compose.log` so SQLExceptions can be traced
    back to server-side messages.
  - dbfs runs also emit `dbfs_query_trace.csv` in the dbfs result directory,
    containing per-query-shape timing aggregates for bottleneck analysis.
  - PostgreSQL runs emit `postgres_query_trace.csv` in the postgres result
    directory from the tracing JDBC wrapper, using the same client-side SQL
    shape style as dbfs.
- `notebooks/`
  - Jupyter notebooks for analysis and visualization.

## 3. Typical workflow

1. Ensure Docker is available.
2. Choose a shared compare label and run both targets with the same `RUN_LABEL`.
3. Inspect results under `results/<compare-label>/tpcc/<engine>/`.
4. Open notebooks as needed to visualize throughput and latency.
5. Inspect `dbfs_query_trace.csv` and `postgres_query_trace.csv`, or use the
   comparison notebook to rank query shapes by total time, mean latency, and
   max latency. Both files use client-side JDBC shapes; if you need
   PostgreSQL server-side rewritten SQL, inspect `pg_stat_statements`
   separately inside the container.
6. When a statement shape shows `SQLException`, inspect the matching
  `benchbase*.log` and `*.compose.log` files in the same result directory to
  recover the concrete error message.

If you want to keep the same compose environment and change only the BenchBase
config file, place another XML file under `benchbase-config/` and run:

```bash
RUN_LABEL=20260503-compare \
  ./benchmarking/scripts/run_postgres_tpcc_docker.sh \
  -c benchmarking/benchbase-config/your_config.xml
```

The PostgreSQL TPCC config in this repository uses:

- host: `postgres`
- port: `5432`
- database: `benchbase`
- user: `admin`
- password: `password`

Those values match the compose services defined in `docker-compose.yaml`.

## 4. perf + FlameGraph

The compose-based dbfs flow can capture `perf record` data inside the `dbfs`
container and post-process it into a flamegraph in a separate `flamegraph`
container.

Example:

```bash
RUN_LABEL=20260515-perf \
  ./benchmarking/scripts/run_postgres_tpcc_docker.sh \
  -c benchmarking/benchbase-config/dbfs_tpcc_docker.xml \
  --profile-perf \
  --perf-frequency 199
```

When `--profile-perf` is enabled, the wrapper runs BenchBase in two steps:

- `create + load` first, without `perf` or JDBC query tracing
- `execute` second, with `perf` and query tracing enabled

This keeps the generated flamegraph and `dbfs_query_trace.csv` focused on the
steady-state benchmark phase instead of the bulk load phase.

Profiling outputs are written alongside the normal BenchBase results:

- `results/<compare-label>/tpcc/dbfs/dbfs.perf.data`
- `results/<compare-label>/tpcc/dbfs/dbfs.perf.script`
- `results/<compare-label>/tpcc/dbfs/dbfs.perf.folded`
- `results/<compare-label>/tpcc/dbfs/dbfs_flamegraph.svg`
- `results/<compare-label>/tpcc/dbfs/benchbase_setup.log`
- `results/<compare-label>/tpcc/dbfs/benchbase_execute.log`
- `results/<compare-label>/tpcc/dbfs/dbfs.compose.log`

The setup-only BenchBase outputs, if any, are written under:

- `results/<compare-label>/tpcc/dbfs/setup/`

Notes:

- This flow is intended for the compose-based `dbfs` service only.
- The `dbfs` image is built with `RelWithDebInfo` and frame pointers to make
  stack sampling more usable.
- On macOS with Docker Desktop, `perf` data is still useful for learning and
  hotspot discovery, but low-level hardware counters may be less reliable than
  on a native Linux host.
