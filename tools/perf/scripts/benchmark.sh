#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$script_dir/cluster.sh"
. "$script_dir/storage.sh"

load=${1:-s}
result_dir=${RESULT_DIR:-$lab_root/results}
compaction_matrix="$result_dir/compaction-${load}.tsv"
strict_perf=${STRICT_PERF:-false}
concurrency=${BENCH_CONCURRENCY:-8}
clients=${BENCH_CLIENTS:-$concurrency}
conns=${BENCH_CONNS:-$concurrency}
value_size=${BENCH_VALUE_SIZE:-256}
rounds=${BENCH_ROUNDS:-3}
compaction=${BENCH_COMPACTION:-true}
compaction_interval=${BENCH_COMPACTION_INTERVAL:-1s}
compaction_index_delta=${BENCH_COMPACTION_INDEX_DELTA:-1000}
compaction_operations=${BENCH_COMPACTION_OPERATIONS:-}
compaction_key_space=${BENCH_COMPACTION_KEY_SPACE_SIZE:-1000}
compaction_warmup=${BENCH_COMPACTION_WARMUP:-1s}
# Entries are display-name:storage-backend pairs. Keep the runner generic so
# adding a backend does not require another branch in this script.
bench_modes=${BENCH_MODES:-"etcd:bbolt pebble:pebble"}
bench_workloads=${BENCH_WORKLOADS:-"put range-value-linearizable range-value-serializable range-count-only range-keys-only txn-put txn-mixed watch watch-get watch-latency lease-keepalive stm"}
txn_key_space_size=${BENCH_TXN_KEY_SPACE_SIZE:-1000}
txn_ops_per_txn=${BENCH_TXN_OPS_PER_TXN:-1}
txn_range_limit=${BENCH_TXN_RANGE_LIMIT:-1000}
txn_rw_ratio=${BENCH_TXN_RW_RATIO:-1}
watch_streams=${BENCH_WATCH_STREAMS:-10}
watch_per_stream=${BENCH_WATCHERS_PER_STREAM:-100}
watch_watched_keys=${BENCH_WATCHED_KEY_TOTAL:-1}
watch_put_total=${BENCH_WATCH_PUT_TOTAL:-1000}
watch_put_rate=${BENCH_WATCH_PUT_RATE:-0}
watch_key_size=${BENCH_WATCH_KEY_SIZE:-32}
watch_key_space_size=${BENCH_WATCH_KEY_SPACE_SIZE:-1}
watch_get_watchers=${BENCH_WATCH_GET_WATCHERS:-10000}
watch_get_streams=${BENCH_WATCH_GET_STREAMS:-1}
watch_get_events=${BENCH_WATCH_GET_EVENTS:-8}
watch_latency_put_total=${BENCH_WATCH_LATENCY_PUT_TOTAL:-1000}
watch_latency_put_rate=${BENCH_WATCH_LATENCY_PUT_RATE:-100}
watch_latency_streams=${BENCH_WATCH_LATENCY_STREAMS:-10}
watch_latency_watchers=${BENCH_WATCH_LATENCY_WATCHERS_PER_STREAM:-10}
stm_keys=${BENCH_STM_KEYS:-1000}
stm_keys_per_txn=${BENCH_STM_KEYS_PER_TXN:-1}
stm_write_percent=${BENCH_STM_WRITE_PERCENT:-50}
stm_isolation=${BENCH_STM_ISOLATION:-r}
cpu_limit=${UNIFIED_RAFT_LSM_CPUS:-2.0}
memory_limit=${UNIFIED_RAFT_LSM_MEMORY:-1g}

case "$load" in
  s) default_operations=1000 ;;
  m) default_operations=5000 ;;
  l) default_operations=20000 ;;
  xl) default_operations=50000 ;;
esac
operations=${BENCH_OPERATIONS:-$default_operations}
read_operations=${BENCH_READ_OPERATIONS:-$operations}
[ -n "$compaction_operations" ] || compaction_operations=$operations

case "$rounds" in
  ''|*[!0-9]*|0)
    echo "BENCH_ROUNDS must be a positive integer" >&2
    exit 2
    ;;
esac

trap stop_cluster EXIT INT TERM

case "$load" in
  s|m|l|xl) ;;
  *)
    echo "usage: $0 {s|m|l|xl}" >&2
    exit 2
    ;;
esac

mkdir -p "$result_dir"
matrix="$result_dir/matrix-${load}.tsv"
printf 'round\tmode\tbackend\tworkload\trequests_per_second\tp50_ms\tp95_ms\tp99_ms\tstatus\n' >"$matrix"
printf 'round\tmode\tbackend\tscenario\trole\tbaseline\trequests_per_second\tp50_ms\tp95_ms\tp99_ms\tstatus\n' >"$compaction_matrix"

git_sha=unknown
if git_sha=$(git rev-parse HEAD 2>/dev/null); then
  :
fi
{
  echo "git_sha=$git_sha"
  echo "load=$load"
  echo "rounds=$rounds"
  echo "modes=$bench_modes"
  echo "workloads=$bench_workloads"
  echo "operations=$operations"
  echo "concurrency=$concurrency"
  echo "clients=$clients"
  echo "connections=$conns"
  echo "value_size=$value_size"
  echo "read_operations=$read_operations"
  echo "collection_nodes=etcd1,etcd2,etcd3"
  echo "compaction=$compaction"
  echo "compaction_interval=$compaction_interval"
  echo "compaction_index_delta=$compaction_index_delta"
  echo "compaction_warmup=$compaction_warmup"
  echo "compaction_scenarios=put-compaction,range-compaction"
  echo "cpu_limit=$cpu_limit"
  echo "memory_limit=$memory_limit"
  echo "benchmark_tool=etcd/tools/benchmark"
} >"$result_dir/manifest.env"

if [ "${PERF_BUILD_IMAGE:-false}" = true ]; then
  # Build explicitly when the image must reflect the current checkout. Keeping
  # this opt-in lets a local run reuse an already-built lab image offline.
  compose build
fi

official_result_value() {
  file=$1
  key=$2
  section=${3:-}
  if [ ! -f "$file" ]; then
    printf 'skipped\n'
    return 0
  fi
  awk -v section="$section" -v key="$key" '
    function is_section_header(line) {
      return index(line, "Watch creation summary:") > 0 || index(line, "Watch events received summary:") > 0 || index(line, "Get during watch summary:") > 0 || index(line, "Total Read Ops:") > 0 || index(line, "Total Write Ops:") > 0 || index(line, "Put summary:") > 0 || index(line, "Watch events summary:") > 0
    }
    {
      if (section == "") {
        active = 1
      } else if (index($0, section) > 0) {
        active = 1
        next
      } else if (active && is_section_header($0)) {
        active = 0
      }
      if (active && index($0, key) > 0) {
        if (key == "Requests/sec:") {
          printf "%.3f\n", $2
        } else {
          printf "%.3f\n", $3 * 1000
        }
        found = 1
        exit
      }
    }
    END {
      if (!found) print "skipped"
    }' "$file"
}

run_benchmark_command() {
	run_benchmark_command_on \
	  'http://etcd1:2379,http://etcd2:2379,http://etcd3:2379' "$@"
}

run_benchmark_command_on() {
	endpoints=$1
	shift
  compose exec -T runner benchmark \
    --endpoints="$endpoints" \
    --clients="$clients" --conns="$conns" --precise "$@"
}

compaction_prefix_for() {
  printf '/etcd-perf-compaction%s' "${1#/etcd-perf-benchmark}"
}

run_range_compaction_workload() {
  bench_output=$1
  bench_prefix=$2
  driver_output="${bench_output}.driver"
  driver_prefix=$(compaction_prefix_for "$bench_prefix")

  # Keep the compaction driver separate from the foreground range workload.
  # Both commands are still the official benchmark binary; the driver only
  # supplies writes and MVCC compactions while range measures foreground reads.
  run_benchmark_command \
    put --total="$compaction_operations" --key-space-size="$compaction_key_space" \
    --sequential-keys --prefix="$driver_prefix" --val-size="$value_size" \
    --compact-interval="$compaction_interval" \
    --compact-index-delta="$compaction_index_delta" \
    >"$driver_output" 2>&1 &
  driver_pid=$!
  sleep "$compaction_warmup"

  range_status=0
  if run_benchmark_command \
    range "$bench_prefix" --prefix --consistency=s --total="$read_operations" \
    >"$bench_output" 2>&1; then
    :
  else
    range_status=$?
  fi

  driver_status=0
  if wait "$driver_pid"; then
    :
  else
    driver_status=$?
  fi
  [ "$range_status" -eq 0 ] && [ "$driver_status" -eq 0 ]
}

run_put_compaction_workload() {
  bench_output=$1
  bench_prefix=$2
  driver_output="${bench_output}.driver"
  driver_prefix=$(compaction_prefix_for "$bench_prefix")

  # Keep the compaction driver separate from the foreground put workload. The
  # foreground result is the only value recorded for the treatment sample.
  run_benchmark_command \
    put --total="$compaction_operations" --key-space-size="$compaction_key_space" \
    --sequential-keys --prefix="$driver_prefix" --val-size="$value_size" \
    --compact-interval="$compaction_interval" \
    --compact-index-delta="$compaction_index_delta" \
    >"$driver_output" 2>&1 &
  driver_pid=$!
  sleep "$compaction_warmup"

  foreground_status=0
  if run_benchmark_command \
    put --total="$operations" --key-space-size="$operations" \
    --sequential-keys --prefix="$bench_prefix" --val-size="$value_size" \
    >"$bench_output" 2>&1; then
    :
  else
    foreground_status=$?
  fi

  driver_status=0
  if wait "$driver_pid"; then
    :
  else
    driver_status=$?
  fi
  [ "$foreground_status" -eq 0 ] && [ "$driver_status" -eq 0 ]
}

run_official_workload() {
  workload=$1
  bench_output=$2
  bench_prefix=$3

  if [ "$workload" = range-compaction ]; then
    run_range_compaction_workload "$bench_output" "$bench_prefix"
    return $?
  fi

  case "$workload" in
    put)
      run_benchmark_command \
        put --total="$operations" --key-space-size="$operations" \
        --sequential-keys --prefix="$bench_prefix" --val-size="$value_size"
      ;;
    put-compaction)
      run_benchmark_command \
        put --total="$compaction_operations" --key-space-size="$compaction_key_space" \
        --sequential-keys --prefix="$(compaction_prefix_for "$bench_prefix")" --val-size="$value_size" \
        --compact-interval="$compaction_interval" \
        --compact-index-delta="$compaction_index_delta"
      ;;
    range-value-linearizable)
      run_benchmark_command \
        range "$bench_prefix" --prefix --consistency=l --total="$read_operations"
      ;;
    range-value-serializable)
      run_benchmark_command \
        range "$bench_prefix" --prefix --consistency=s --total="$read_operations"
      ;;
    range-count-only)
      run_benchmark_command \
        range "$bench_prefix" --prefix --count-only --consistency=l --total="$read_operations"
      ;;
    range-keys-only)
      run_benchmark_command \
        range "$bench_prefix" --prefix --keys-only --consistency=l --total="$read_operations"
      ;;
    txn-put)
      run_benchmark_command \
        txn-put --total="$operations" --key-space-size="$txn_key_space_size" \
        --txn-ops="$txn_ops_per_txn" --key-size=8 --val-size="$value_size"
      ;;
    txn-mixed)
      run_benchmark_command \
        txn-mixed "$bench_prefix" --total="$operations" \
        --key-space-size="$txn_key_space_size" --limit="$txn_range_limit" \
        --consistency=l --rw-ratio="$txn_rw_ratio"
      ;;
    watch)
      run_benchmark_command \
        watch --streams="$watch_streams" --watch-per-stream="$watch_per_stream" \
        --watched-key-total="$watch_watched_keys" --put-total="$watch_put_total" \
        --put-rate="$watch_put_rate" --key-size="$watch_key_size" \
        --key-space-size="$watch_key_space_size" --sequential-keys
      ;;
    watch-get)
      run_benchmark_command \
        watch-get --watchers="$watch_get_watchers" --streams="$watch_get_streams" \
        --events="$watch_get_events"
      ;;
    watch-latency)
      run_benchmark_command \
        watch-latency --streams="$watch_latency_streams" \
        --watchers-per-stream="$watch_latency_watchers" \
        --put-total="$watch_latency_put_total" \
        --put-rate="$watch_latency_put_rate"
      ;;
    lease-keepalive)
      run_benchmark_command lease-keepalive --total="$operations"
      ;;
    stm)
      run_benchmark_command \
        stm --total="$operations" --keys="$stm_keys" \
        --keys-per-txn="$stm_keys_per_txn" --txn-wr-percent="$stm_write_percent" \
        --val-size="$value_size" --isolation="$stm_isolation" --stm-locker=stm
      ;;
    *)
      echo "unknown benchmark workload: $workload" >&2
      return 2
      ;;
  esac >"$bench_output" 2>&1
}

workload_section() {
  case "$1" in
    txn-mixed-read) printf 'Total Read Ops:' ;;
    txn-mixed-write) printf 'Total Write Ops:' ;;
    watch-create) printf 'Watch creation summary:' ;;
    watch-events) printf 'Watch events received summary:' ;;
    watch-get) printf 'Get during watch summary:' ;;
    watch-latency-put) printf 'Put summary:' ;;
    watch-latency-event) printf 'Watch events summary:' ;;
    *) printf '' ;;
  esac
}

workload_command() {
  case "$1" in
    txn-mixed-read|txn-mixed-write) printf 'txn-mixed' ;;
    watch-create|watch-events) printf 'watch' ;;
    watch-latency-put|watch-latency-event) printf 'watch-latency' ;;
    *) printf '%s' "$1" ;;
  esac
}

append_matrix_row() {
  mode=$1
  storage_backend=$2
  round=$3
  workload=$4
  raw_output=$5
  section=$6
  status=$7
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$round" \
    "$mode" \
    "$storage_backend" \
    "$workload" \
    "$(official_result_value "$raw_output" 'Requests/sec:' "$section")" \
    "$(official_result_value "$raw_output" '50% in' "$section")" \
    "$(official_result_value "$raw_output" '95% in' "$section")" \
    "$(official_result_value "$raw_output" '99% in' "$section")" \
    "$status" >>"$matrix"
}

append_compaction_matrix_row() {
  mode=$1
  storage_backend=$2
  round=$3
  scenario=$4
  role=$5
  baseline=$6
  raw_output=$7
  status=$8
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$round" \
    "$mode" \
    "$storage_backend" \
    "$scenario" \
    "$role" \
    "$baseline" \
    "$(official_result_value "$raw_output" 'Requests/sec:')" \
    "$(official_result_value "$raw_output" '50% in')" \
    "$(official_result_value "$raw_output" '95% in')" \
    "$(official_result_value "$raw_output" '99% in')" \
    "$status" >>"$compaction_matrix"
}

record_workload() {
  mode=$1
  storage_backend=$2
  round=$3
  workload=$4
  raw_output=$5
  status=ok

  case "$workload" in
    txn-mixed)
      if ! run_official_workload txn-mixed "$raw_output" "$prefix"; then
        status=failed
        perf_status=1
      fi
      append_matrix_row "$mode" "$storage_backend" "$round" txn-mixed-read "$raw_output" "Total Read Ops:" "$status"
      append_matrix_row "$mode" "$storage_backend" "$round" txn-mixed-write "$raw_output" "Total Write Ops:" "$status"
      ;;
    watch)
      if ! run_official_workload watch "$raw_output" "$prefix"; then
        status=failed
        perf_status=1
      fi
      append_matrix_row "$mode" "$storage_backend" "$round" watch-create "$raw_output" "Watch creation summary:" "$status"
      append_matrix_row "$mode" "$storage_backend" "$round" watch-events "$raw_output" "Watch events received summary:" "$status"
      ;;
    watch-latency)
      if ! run_official_workload watch-latency "$raw_output" "$prefix"; then
        status=failed
        perf_status=1
      fi
      append_matrix_row "$mode" "$storage_backend" "$round" watch-latency-put "$raw_output" "Put summary:" "$status"
      append_matrix_row "$mode" "$storage_backend" "$round" watch-latency-event "$raw_output" "Watch events summary:" "$status"
      ;;
    *)
      section=$(workload_section "$workload")
      command_name=$(workload_command "$workload")
      if ! run_official_workload "$command_name" "$raw_output" "$prefix"; then
        status=failed
        perf_status=1
      fi
      append_matrix_row "$mode" "$storage_backend" "$round" "$workload" "$raw_output" "$section" "$status"
      ;;
  esac
}

start_compaction_cluster() {
  stop_cluster
  start_cluster "$storage_backend"
  if ! wait_for_endpoint "$endpoint" 90; then
    echo "cluster did not become healthy for compaction scenario=$1 mode=$mode" >&2
    return 1
  fi
}

prefill_compaction_range() {
  prefix=$1
  prefill_output=$2
  run_benchmark_command \
    put --total="$operations" --key-space-size="$operations" \
    --sequential-keys --prefix="$prefix" --val-size="$value_size" \
    >"$prefill_output" 2>&1
}

run_compaction_pair() {
  scenario=$1
  baseline_output="$result_dir/${mode}-${load}-r${round}-${scenario}-baseline.txt"
  treatment_output="$result_dir/${mode}-${load}-r${round}-${scenario}.txt"
  prefix="/etcd-perf-benchmark/$mode/r${round}/"
  baseline_workload=put
  if [ "$scenario" = range-compaction ]; then
    baseline_workload=range-value-serializable
  fi
  baseline_status=ok
  treatment_status=ok

  if ! start_compaction_cluster "$scenario"; then
    return 1
  fi
  case "$scenario" in
    put-compaction)
      if ! run_official_workload put "$baseline_output" "$prefix"; then
        baseline_status=failed
      fi
      ;;
    range-compaction)
      if ! prefill_compaction_range "$prefix" "$baseline_output.prefill" || \
        ! run_official_workload range-value-serializable "$baseline_output" "$prefix"; then
        baseline_status=failed
      fi
      ;;
  esac
  append_compaction_matrix_row "$mode" "$storage_backend" "$round" "$scenario" baseline \
    "$baseline_workload" "$baseline_output" "$baseline_status"

  if ! start_compaction_cluster "$scenario"; then
    return 1
  fi
  case "$scenario" in
    put-compaction)
      if ! run_put_compaction_workload "$treatment_output" "$prefix"; then
        treatment_status=failed
      fi
      ;;
    range-compaction)
      if ! prefill_compaction_range "$prefix" "$treatment_output.prefill" || \
        ! run_range_compaction_workload "$treatment_output" "$prefix"; then
        treatment_status=failed
      fi
      ;;
  esac
  if [ "$treatment_status" = ok ] && ! verify_cluster "$prefix" "$operations"; then
    treatment_status=failed
  fi
  append_compaction_matrix_row "$mode" "$storage_backend" "$round" "$scenario" treatment \
    "$baseline_workload" "$treatment_output" "$treatment_status"
  stop_cluster
  [ "$baseline_status" = ok ] && [ "$treatment_status" = ok ]
}

run_compaction_scenarios() {
  compaction_status=0
  for scenario in put-compaction range-compaction; do
    if ! run_compaction_pair "$scenario"; then
      compaction_status=1
    fi
  done
  return "$compaction_status"
}

run_mode() {
  mode=$1
  storage_backend=$2
  round=$3
  output="$result_dir/${mode}-${load}-r${round}.txt"

  stop_cluster
  start_cluster "$storage_backend"

  endpoint='http://etcd1:2379,http://etcd2:2379,http://etcd3:2379'
  if ! wait_for_endpoint "$endpoint" 90; then
    echo "cluster did not become healthy for mode=$mode" >&2
    compose logs --no-color >"$output.cluster.log" || true
    exit 1
  fi

  prefix="/etcd-perf-benchmark/$mode/r${round}/"
	  perf_status=0
  for workload in $bench_workloads; do
    case "$workload" in
      range-*)
        if [ "$read_operations" -le 0 ]; then
          printf '%s\t%s\t%s\t%s\tskipped\tskipped\tskipped\tskipped\tskipped\n' \
            "$round" "$mode" "$storage_backend" "$workload" >>"$matrix"
          continue
        fi
        ;;
    esac
    case "$workload" in
      put) workload_output="$output.put" ;;
      range-value-linearizable) workload_output="$output.get" ;;
      *) workload_output="$result_dir/${mode}-${load}-r${round}-${workload}.txt" ;;
    esac
    if ! record_workload "$mode" "$storage_backend" "$round" "$workload" "$workload_output"; then
      perf_status=1
    fi
  done

	capture_member_artifacts_after_all "$mode" "$load" "$round" "$result_dir"

  if [ "$perf_status" -eq 0 ] && ! verify_cluster "$prefix" "$operations"; then
    echo "post-workload state verification failed for mode=$mode" >&2
    return 1
  fi
  stop_cluster

  if [ "$compaction" = true ] && ! run_compaction_scenarios; then
    perf_status=1
  fi

  {
    echo "round=$round"
    echo "mode=$mode"
    echo "load=$load"
    echo "operations=$operations"
    echo "concurrency=$concurrency"
    echo "value_size=$value_size"
    echo "read_operations=$read_operations"
    echo "storage_backend=$storage_backend"
    echo "cpu_limit=$cpu_limit"
    echo "memory_limit=$memory_limit"
    echo "benchmark_tool=etcd/tools/benchmark"
    echo "benchmark_exit_status=$perf_status"
    echo "benchmark_clients=$clients"
    echo "benchmark_conns=$conns"
    echo "workloads=$bench_workloads"
    echo "write_result=$output.put"
    echo "read_result=$output.get"
	  echo "member_artifacts=${mode}-${load}-r${round}-{etcd1,etcd2,etcd3}.after"
  } | tee "$output"

  if [ "$perf_status" -ne 0 ]; then
    echo "official benchmark reported a workload failure for mode=$mode; raw outputs are in $result_dir" >&2
    if [ "$strict_perf" = true ]; then
      return "$perf_status"
    fi
  fi
}

round=1
while [ "$round" -le "$rounds" ]; do
  for mode_spec in $bench_modes; do
    case "$mode_spec" in
      *:*)
        mode=${mode_spec%%:*}
        storage_backend=${mode_spec#*:}
        ;;
      *)
        mode=
        storage_backend=
        ;;
    esac
    if [ -z "$mode" ] || [ -z "$storage_backend" ]; then
      echo "invalid BENCH_MODES entry: $mode_spec (expected name:backend)" >&2
      exit 2
    fi
    run_mode "$mode" "$storage_backend" "$round"
  done
  round=$((round + 1))
done

result_abs=$(CDPATH= cd -- "$result_dir" && pwd)
if [ "$result_abs" = "$lab_root/results" ]; then
  system_report="$result_abs/system-${load}.tsv"
  html_report="$result_abs/report-${load}.html"
  report_generated=false
  if compose ps --services --filter status=running | grep -qx runner; then
    if compose exec -T runner perf-report \
      -input "/results/matrix-${load}.tsv" \
      -compaction-input "/results/compaction-${load}.tsv" \
      -system-output "/results/system-${load}.tsv" \
      -output "/results/report-${load}.md" \
      -html-output "/results/report-${load}.html"; then
      report_generated=true
    fi
  fi
  if [ "$report_generated" != true ] && command -v go >/dev/null 2>&1; then
    echo "runner is not running; using host Go to render the report" >&2
    if ! (cd "$repo_root" && go run ./tools/perf/report \
      -input "$result_abs/matrix-${load}.tsv" \
      -compaction-input "$result_abs/compaction-${load}.tsv" \
      -system-output "$system_report" \
      -output "$result_abs/report-${load}.md" \
      -html-output "$html_report"); then
      echo "warning: could not generate the Markdown performance report" >&2
    fi
  elif [ "$report_generated" != true ]; then
    echo "warning: Go is unavailable; raw workload matrix was kept without a Markdown report" >&2
  fi
elif command -v go >/dev/null 2>&1; then
  system_report="$result_abs/system-${load}.tsv"
  html_report="$result_abs/report-${load}.html"
  if ! (cd "$repo_root" && go run ./tools/perf/report \
    -input "$result_abs/matrix-${load}.tsv" \
    -compaction-input "$result_abs/compaction-${load}.tsv" \
    -system-output "$system_report" \
    -output "$result_abs/report-${load}.md" \
    -html-output "$html_report"); then
    echo "warning: could not generate the Markdown performance report" >&2
  fi
else
  echo "warning: Go is unavailable; raw workload matrix was kept without a Markdown report" >&2
fi

echo "benchmark completed: load=$load results=$result_dir matrix=$matrix"
