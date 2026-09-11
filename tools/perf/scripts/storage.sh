#!/bin/sh

# Storage-efficiency collection for the performance lab. Only the metrics
# needed for the #22332 comparison are captured; the full /metrics endpoint is
# deliberately not archived by the benchmark.

capture_backend_size() {
  node=$1
  artifact_output=$2
  compose exec -T runner sh -c \
    "curl -fsS http://$node:2381/metrics | awk '\$1 == \"etcd_mvcc_db_total_size_in_use_in_bytes\" {print}'" \
    >"$artifact_output"
}

capture_storage() {
  node=$1
  artifact_output=$2
  compose exec -T runner sh -c \
    "du -sb /data/$node/member/wal /data/$node/member/snap/db 2>/dev/null || true" \
    >"$artifact_output"
}

capture_member_artifacts_after() {
	node=$1
	mode=$2
	load=$3
	round=$4
	result_dir=$5
	stem="$result_dir/${mode}-${load}-r${round}-${node}"
	capture_backend_size "$node" "$stem.backend-size.after"
	capture_storage "$node" "$stem.storage.after"
}

capture_member_artifacts_after_all() {
	mode=$1
	load=$2
	round=$3
	result_dir=$4
	for node in etcd1 etcd2 etcd3; do
	    capture_member_artifacts_after "$node" "$mode" "$load" "$round" "$result_dir"
	done
}
