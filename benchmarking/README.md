# Benchmarking dbfs with BenchBase

This directory contains helper configuration, scripts, results, and notebooks
for benchmarking **dbfs** and comparing it against **PostgreSQL** using
[BenchBase](https://github.com/cmu-db/benchbase). The repository includes
**TPC-C** and Zipfian **YCSB** workloads.

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
    - `dbfs_tpcc_docker.xml` – TPCC config for the compose-based dbfs flow.
    - `dbfs_ycsb_zipf_*.xml` and `postgres_ycsb_zipf_*.xml` – matching
      Zipfian YCSB configs for statistics and throughput runs.
- `scripts/`
  - Shell wrappers around the benchmark flow.
  - Example:
    - `run_benchbase_docker.sh` – start the selected database with docker
      compose, wait for readiness, and run BenchBase. You must pass the
      benchmark target config with `-c <path>`; use `--workload ycsb` for
      YCSB because the backward-compatible default is `tpcc`.
    - `run_postgres_tpcc_docker.sh` – backward-compatible alias for
      `run_benchbase_docker.sh`.
- `results/`
  - Raw outputs produced by BenchBase runs.
  - Recommended structure:
    - `results/<compare-label>/<workload>/postgres/...`
    - `results/<compare-label>/<workload>/dbfs/...`
  - The compose wrapper saves BenchBase stdout/stderr as
    `benchbase_setup.log` and `benchbase_execute.log`. Setup and execution are
    separate so dbfs can reset its buffer pool before workload warmup.
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
3. Inspect results under
   `results/<compare-label>/<workload>/<engine>/`.
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
  ./benchmarking/scripts/run_benchbase_docker.sh \
  -c benchmarking/benchbase-config/your_config.xml
```

Pass `--workload ycsb` as well when the config contains YCSB transaction
types.

The PostgreSQL TPCC config in this repository uses:

- host: `postgres`
- port: `5432`
- database: `benchbase`
- user: `admin`
- password: `password`

Those values match the compose services defined in `docker-compose.yaml`.

### Throughput and statistics runs

Use `take_benchmark.sh` to run dbfs and PostgreSQL with one shared result
label. Throughput runs disable buffer-pool logging and enable perf profiling
for dbfs so the same run produces a flamegraph:

```bash
./benchmarking/scripts/take_benchmark.sh --throughput arc-throughput
```

Select one database with `--target`; the default is `both`:

```bash
./benchmarking/scripts/take_benchmark.sh --throughput --target dbfs arc-dbfs
./benchmarking/scripts/take_benchmark.sh --throughput --target postgres arc-postgres
```

Statistics runs enable one-second aggregate buffer-pool statistics and
per-operation buffer-pool events while leaving perf profiling disabled:

```bash
./benchmarking/scripts/take_benchmark.sh --stats arc-stats
```

### Zipfian YCSB

Run the supplied read/scan YCSB workload against dbfs with:

```bash
./benchmarking/scripts/take_benchmark.sh \
  --throughput \
  --workload ycsb \
  --target dbfs \
  ycsb-zipf-099
```

Use `--stats` instead of `--throughput` for the controlled run with a
180-second warmup followed by a 600-second measurement interval at 100
requests per second. Omit `--target` to compare dbfs and PostgreSQL using
matching YCSB settings. Both modes use 90% point reads and 10% scans so a
repeatedly accessed hot set competes with scan pages without introducing page
creation or dirty-page write-back. Throughput mode removes the controlled
rate cap while keeping the workload mix identical.

The supplied YCSB configs load 50,000 rows (`scalefactor` 50), use ten
100-character fields, and fix the random seed at 42. Record keys are selected
by BenchBase's Zipfian generator. The default `skewFactor` is 0.99:

```xml
<skewFactor>0.99</skewFactor>
```

The valid range is greater than zero and less than one. Values nearer one put
more traffic on a small hot set. To compare skew levels, copy both engine
configs for the selected mode, change only `skewFactor`, and keep the scale,
seed, phase timing, rate, and transaction weights identical.

BenchBase's YCSB generator clusters the hottest records at the low end of the
integer key range; it does not scramble hot records across the key space.

For TPC-C, the dbfs statistics mode uses a controlled workload from
`dbfs_tpcc_controlled_stats.xml`: one terminal, random seed 42, a 60-second
warmup, a 600-second measurement interval, and a regular target rate of two
requests per second. Throughput mode uses `dbfs_tpcc_throughput.xml`: the same
seed and warmup, followed by a 300-second saturated measurement interval.

The default mode is `--stats` for compatibility with existing invocations.
Use throughput runs for performance numbers and flamegraphs, and statistics
runs for explaining their behavior. The throughput result includes the small
but non-zero overhead of perf sampling. Environment variables such as
`DBFS_BUFFER_POOL_EVENT_LOG` and `DBFS_BUFFER_POOL_LOG_STATS_EVERY_MS` can still
override a mode's defaults. Flamegraphs are generated only for dbfs; PostgreSQL
throughput runs do not use perf profiling.

### Buffer-pool state at the benchmark boundary

The wrapper runs `create + load` separately from `execute`. For dbfs, it
watches `benchbase_execute.log` for BenchBase's `PHASE START` marker and then
sends the server's `resetBufferPool` management operation. Waiting until this
marker also excludes execute-initialization queries such as YCSB's
`SELECT MAX(...)` from the initial ARC state.

The reset flushes WAL and dirty resident pages, clears every buffer-pool frame,
and resets ARC and the aggregate buffer-pool counters. BenchBase then uses the
remaining warmup interval to train ARC from the workload itself. ARC is not
reset at `MEASURE`, because doing so would discard the warmup state; notebooks
instead subtract the last pre-measure counter sample.

The reset response and timestamp are saved as `dbfs.buffer_pool_reset.log`.
Aggregate stats also include `buffer_pool_generation`, which becomes `1` after
the reset, so a result can be rejected if the phase hook did not run.

This reset intentionally leaves the operating-system page cache intact. It
isolates dbfs buffer-replacement behavior without turning every run into a
cold-storage benchmark.

## 4. perf + FlameGraph

The compose-based dbfs flow can capture `perf record` data inside the `dbfs`
container and post-process it into a flamegraph in a separate `flamegraph`
container.

Example:

```bash
RUN_LABEL=20260515-perf \
  ./benchmarking/scripts/run_benchbase_docker.sh \
  -c benchmarking/benchbase-config/dbfs_tpcc_docker.xml \
  --profile-perf \
  --perf-frequency 199
```

The wrapper runs BenchBase in two steps:

- `create + load` first, without `perf` or JDBC query tracing
- `execute` second, with `perf` and query tracing enabled

For dbfs, `perf` starts only after the warmup-start buffer-pool reset. This
keeps the generated flamegraph and `dbfs_query_trace.csv` focused on execution
instead of bulk load, and keeps reset flushing out of the profile.

Profiling outputs are written alongside the normal BenchBase results:

- `results/<compare-label>/<workload>/dbfs/dbfs.perf.data`
- `results/<compare-label>/<workload>/dbfs/dbfs.perf.script`
- `results/<compare-label>/<workload>/dbfs/dbfs.perf.folded`
- `results/<compare-label>/<workload>/dbfs/dbfs_flamegraph.svg`
- `results/<compare-label>/<workload>/dbfs/benchbase_setup.log`
- `results/<compare-label>/<workload>/dbfs/benchbase_execute.log`
- `results/<compare-label>/<workload>/dbfs/dbfs.compose.log`

The setup-only BenchBase outputs, if any, are written under:

- `results/<compare-label>/<workload>/dbfs/setup/`

Notes:

- This flow is intended for the compose-based `dbfs` service only.
- The `dbfs` image is built with `RelWithDebInfo` and frame pointers to make
  stack sampling more usable.
- On macOS with Docker Desktop, `perf` data is still useful for learning and
  hotspot discovery, but low-level hardware counters may be less reliable than
  on a native Linux host.
