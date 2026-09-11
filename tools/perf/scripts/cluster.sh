#!/bin/sh

# Cluster lifecycle and state-validation helpers for the local performance lab.

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
lab_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
repo_root=$(CDPATH= cd -- "$lab_root/../.." && pwd)

compose() {
  docker compose -f "$lab_root/compose.yaml" "$@"
}

stop_cluster() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

start_cluster() {
  backend=$1
  shift
  (
    export STORAGE_BACKEND="$backend"
    for variable in "$@"; do
      export "$variable"
    done
    compose up -d
  )
}

start_member() {
  backend=$1
  member=$2
  shift 2
  (
    export STORAGE_BACKEND="$backend"
    for variable in "$@"; do
      export "$variable"
    done
    compose up -d "$member"
  )
}

wait_for_endpoint() {
  endpoint=$1
  timeout=${2:-90}

  i=0
  while [ "$i" -lt "$timeout" ]; do
    if compose exec -T runner etcdctl --endpoints="$endpoint" endpoint status >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

put_values() {
  endpoint=$1
  prefix=$2
  count=$3
  for i in $(seq 1 "$count"); do
    compose exec -T runner etcdctl --endpoints="$endpoint" \
      put "$prefix$i" "value-$i" >/dev/null
  done
}

verify_values() {
  endpoint=$1
  prefix=$2
  count=$3
  for i in $(seq 1 "$count"); do
    got=$(compose exec -T runner etcdctl --endpoints="$endpoint" \
      get "$prefix$i" --print-value-only | tr -d '\r\n')
    if [ "$got" != "value-$i" ]; then
      echo "value mismatch for ${prefix}${i}: got $got" >&2
      return 1
    fi
  done
}

verify_cluster() {
  prefix=$1
  count=$2
  shift 2
  command_timeout=${VERIFY_COMMAND_TIMEOUT:-30s}
  endpoints='http://etcd1:2379,http://etcd2:2379,http://etcd3:2379'
  members='etcd1 etcd2 etcd3'

  (
    tmp_dir=$(mktemp -d)
    cleanup() {
      rm -rf "$tmp_dir"
    }
    trap cleanup EXIT INT TERM

    status_file="$tmp_dir/status.json"
    converged=false
    last_status_signature=

    for i in $(seq 1 60); do
      if ! compose exec -T runner etcdctl --endpoints="$endpoints" \
        endpoint status -w json >"$status_file" 2>/dev/null; then
        sleep 1
        continue
      fi
      if ! jq -e '
        length == 3 and
        ([.[].Status.header.revision] | unique | length == 1) and
        ([.[].Status.raftIndex] | unique | length == 1) and
        ([.[].Status.raftAppliedIndex] | unique | length == 1) and
        ([.[].Status.raftIndex] | .[0]) == ([.[].Status.raftAppliedIndex] | .[0])
      ' "$status_file" >/dev/null 2>&1; then
        sleep 1
        continue
      fi

      status_signature=$(jq -c \
        '[.[].Status.header.revision, .[].Status.raftIndex, .[].Status.raftAppliedIndex]' \
        "$status_file")
      if [ "$status_signature" != "$last_status_signature" ]; then
        last_status_signature=$status_signature
        sleep 1
        continue
      fi

      converged=true
      break
    done

    if [ "$converged" != true ]; then
      echo "cluster status did not converge: $(tr '\n' ' ' <"$status_file")" >&2
      exit 1
    fi

    revision=$(jq -r '.[0].Status.header.revision' "$status_file")
    raft_index=$(jq -r '.[0].Status.raftIndex' "$status_file")
    raft_applied=$(jq -r '.[0].Status.raftAppliedIndex' "$status_file")

    for member in $members; do
      compose exec -T runner etcdctl --command-timeout="$command_timeout" \
        --endpoints="http://$member:2379" \
        get "$prefix" --prefix -w json >"$tmp_dir/$member.kv.json"
      jq -S '[.kvs[]? | {key, value, create_revision, mod_revision, version}] | sort_by(.key)' \
        "$tmp_dir/$member.kv.json" >"$tmp_dir/$member.kv.normalized.json"
    done

    if ! cmp -s "$tmp_dir/etcd1.kv.normalized.json" \
      "$tmp_dir/etcd2.kv.normalized.json" || \
      ! cmp -s "$tmp_dir/etcd1.kv.normalized.json" \
      "$tmp_dir/etcd3.kv.normalized.json"; then
      echo "KV/revision state differs across members for prefix=$prefix" >&2
      diff -u "$tmp_dir/etcd1.kv.normalized.json" \
        "$tmp_dir/etcd2.kv.normalized.json" || true
      diff -u "$tmp_dir/etcd1.kv.normalized.json" \
        "$tmp_dir/etcd3.kv.normalized.json" || true
      exit 1
    fi

    actual_count=$(jq 'length' "$tmp_dir/etcd1.kv.normalized.json")
    if [ -n "$count" ] && [ "$actual_count" -ne "$count" ]; then
      echo "KV count mismatch for prefix=$prefix: got=$actual_count want=$count" >&2
      exit 1
    fi

    echo "cluster state passed: prefix=$prefix count=$actual_count revision=$revision raft-index=$raft_index raft-applied-index=$raft_applied"
  )
}
