# etcd backend performance lab

This lab compares configured etcd storage backends under the same three-member
Raft topology, Docker resource limits, workload parameters, and fresh volumes.
It reuses the official `tools/benchmark` and `etcdctl` binaries.

The lab measures performance and storage behavior. Correctness and fault
recovery remain owned by the official `tests/e2e`, `tests/integration`, and
`tests/robustness` suites.

## Layout

```text
tools/perf/
├── README.md
├── Dockerfile
├── compose.yaml
├── scripts/
│   ├── benchmark.sh
│   ├── cluster.sh
│   ├── storage.sh
│   └── reset.sh
└── report/
    ├── main.go
    ├── html.go
    ├── report_test.go
    ├── system.go
    └── scenarios.go
```

The report package is intentionally a single local command rather than a
public library. Shell scripts own Docker and scenario execution; Go owns TSV
parsing, aggregation, and report rendering.

## Basic comparison

From the repository root:

```sh
tools/perf/scripts/benchmark.sh s
```

The default modes are `etcd:bbolt pebble:pebble`. Select backends without
changing the script:

```sh
BENCH_MODES='etcd:bbolt pebble:pebble another:another' \
  tools/perf/scripts/benchmark.sh l
```

The runner builds the current etcd server, `etcdctl`, and the
official `tools/benchmark` binary. Each mode starts a fresh three-member
cluster with the same resource envelope.

Useful basic settings:

```sh
BENCH_ROUNDS=5 \
BENCH_OPERATIONS=20000 \
BENCH_READ_OPERATIONS=20000 \
BENCH_CONCURRENCY=16 \
BENCH_VALUE_SIZE=512 \
tools/perf/scripts/benchmark.sh l
```

Use `BENCH_ROUNDS=1` for a smoke run. Published comparisons should use at
least three rounds and report raw rounds as well as arithmetic means.

For a larger comparison that keeps compaction measurements paired, use:

```sh
BENCH_ROUNDS=3 \
BENCH_OPERATIONS=20000 \
BENCH_READ_OPERATIONS=1000 \
BENCH_COMPACTION_OPERATIONS=20000 \
tools/perf/scripts/benchmark.sh l
```

The script reuses the local performance image by default. Set
`PERF_BUILD_IMAGE=true` when the image must be rebuilt from the current
checkout; this requires the configured Docker base images to be available.

## Default measurements

The normal workload matrix records, per backend, workload, and round:

- requests per second;
- p50, p95, and p99 latency;
- workload status and raw output from the official benchmark.

For the storage comparison, the runner captures two small per-member artifacts
after every round:

- backend in-use bytes, from the single Prometheus metric relevant to storage
  efficiency;
- WAL and backend snapshot-directory sizes.

The report includes:

1. the official workload comparison table;
2. storage efficiency, including physical size, backend-in-use size, and their
   ratio;
3. MVCC compaction impact on foreground writes and reads.

`backend-in-use` is etcd's logical backend allocation metric. It is a useful
storage-efficiency proxy, but it is not the same as raw Kubernetes application
payload bytes. For a scale curve, run the same command with increasing
`BENCH_OPERATIONS` and keep the resulting `system-*.tsv` files together.

## MVCC compaction impact

Compaction scenarios run by default alongside the clean workload comparison.
Set `BENCH_COMPACTION=false` to disable them:

```sh
BENCH_COMPACTION_INTERVAL=1s \
BENCH_ROUNDS=3 \
tools/perf/scripts/benchmark.sh l
```

This adds two paired scenarios using the official benchmark binary:

- `put-compaction`: runs the same foreground `put` workload while a separate
  official `put` driver triggers MVCC compaction;
- `range-compaction`: pre-fills the same range dataset, then runs a foreground
  serializable range workload while a separate official `put` driver triggers
  MVCC compaction;

For every scenario and round, baseline and treatment run on fresh clusters
with the same workload parameters. The report stores these pairs separately
from the official workload matrix and compares ratios per round. The important
values are throughput ratio and p99 ratio:

```text
throughput impact = 1 - compaction throughput / normal throughput
p99 increase      = compaction p99 / normal p99 - 1
```

This measures foreground interference. It does not replace the official
robustness correctness tests.

## Results

Each run writes under `tools/perf/results`:

```text
matrix-<load>.tsv       official workload-level raw matrix
compaction-<load>.tsv   paired compaction baseline/treatment matrix
system-<load>.tsv       per-member storage metrics
report-<load>.md        Markdown report
report-<load>.html      self-contained HTML report
manifest.env            test conditions and source metadata
```

The report command can be rerun directly:

```sh
go run ./tools/perf/report \
  -input tools/perf/results/matrix-l.tsv \
  -system-output tools/perf/results/system-l.tsv \
  -output tools/perf/results/report-l.md \
  -html-output tools/perf/results/report-l.html
```

The performance gate is opt-in:

```sh
go run ./tools/perf/report \
  -input tools/perf/results/matrix-l.tsv \
  -baseline-mode bbolt \
  -candidate-mode pebble \
  -workload put \
  -min-write-rps-ratio 0.95 \
  -max-write-p99-ratio 1.10
```

## Scope boundary

`tools/perf` does not contain pprof, runtime-trace, defrag, snapshot
save/restore, catch-up, or fault-injection scenarios. Those are either separate
follow-up investigations or official correctness/recovery tests.

The repeatable comparison is:

```text
official benchmark
    ↓
throughput and latency
    ↓
storage efficiency evidence
    ↓
Markdown/TSV report
```
