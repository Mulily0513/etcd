// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestMatrixReport(t *testing.T) {
	input := "round\tmode\tbackend\tworkload\trequests_per_second\tp50_ms\tp95_ms\tp99_ms\tstatus\n" +
		"1\tetcd\tbbolt\tput\t355.838\t10\t20\t30\tok\n" +
		"1\tpebble\tpebble\tput\t386.500\t9\t19\t29\tok\n" +
		"1\trocks\trocksdb\tput\t400.000\t8\t18\t28\tok\n" +
		"1\tetcd\tbbolt\trange-value-serializable\t33954\t1\t2\t3\tok\n" +
		"1\tpebble\tpebble\trange-value-serializable\t11706\t1.5\t2.5\t4\tok\n"
	path := t.TempDir() + "/matrix.tsv"
	if err := os.WriteFile(path, []byte(input), 0o644); err != nil {
		t.Fatal(err)
	}

	matrix, err := readMatrixSamples(path)
	if err != nil {
		t.Fatal(err)
	}
	report := renderMatrixReport(matrix)
	for _, want := range []string{
		"# etcd backend performance comparison",
		"| put | req/s | 355.838 | 386.500 | 400.000 | rocksdb · 12.4% higher",
		"| range-value-serializable | p99 latency (ms) | 3.000 | 4.000 | - | bbolt · 25.0% lower",
		"| backend | throughput wins | p99 latency wins |",
		"| workload | backend | rounds | p50 (ms) | p95 (ms) | p99 (ms) |",
		"| round | backend | workload |",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("matrix report does not contain %q:\n%s", want, report)
		}
	}
}

func TestSystemMetricsReportAggregatesMembers(t *testing.T) {
	dir := t.TempDir()
	matrix := []matrixSample{
		{mode: "etcd", backend: "bbolt", workload: "put", status: "ok"},
		{mode: "pebble", backend: "pebble", workload: "put", status: "ok"},
	}
	metrics := func(inUse float64) string {
		return "etcd_mvcc_db_total_size_in_use_in_bytes " + formatTestFloat(inUse) + "\n"
	}
	for _, backend := range []string{"etcd", "pebble"} {
		for _, node := range []string{"etcd1", "etcd2", "etcd3"} {
			stem := backend + "-l-r1-" + node
			if err := os.WriteFile(filepath.Join(dir, stem+".backend-size.after"), []byte(metrics(100)), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, stem+".storage.after"), []byte("110 /wal\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	report, tsv, err := renderSystemMetricsReport(dir, "matrix-l.tsv", matrix)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(report, "| bbolt | 1 | 330 | 300 | 1.100 |") {
		t.Fatalf("system report did not aggregate bbolt members:\n%s", report)
	}
	if strings.Contains(report, "WAL bytes") {
		t.Fatalf("system report should keep WAL details out of the main table:\n%s", report)
	}
	if !strings.Contains(tsv, "bbolt\t1\tetcd1\t110") {
		t.Fatalf("system TSV did not preserve member rows:\n%s", tsv)
	}
}

func TestHTMLReport(t *testing.T) {
	samples := []matrixSample{
		{round: "1", mode: "etcd", backend: "bbolt", workload: "put", status: "ok", values: map[string]float64{
			"requests_per_second": 1000, "p50_ms": 1, "p95_ms": 2, "p99_ms": 3,
		}},
		{round: "1", mode: "pebble", backend: "pebble", workload: "put", status: "ok", values: map[string]float64{
			"requests_per_second": 1200, "p50_ms": 1, "p95_ms": 2, "p99_ms": 2,
		}},
		{round: "1", mode: "etcd", backend: "bbolt", workload: "range-value-serializable", status: "ok", values: map[string]float64{
			"requests_per_second": 1000, "p50_ms": 1, "p95_ms": 2, "p99_ms": 3,
		}},
		{round: "1", mode: "pebble", backend: "pebble", workload: "range-value-serializable", status: "ok", values: map[string]float64{
			"requests_per_second": 1200, "p50_ms": 1, "p95_ms": 2, "p99_ms": 2,
		}},
	}
	compactionSamples := []compactionSample{
		{round: "1", backend: "bbolt", scenario: "put-compaction", role: "baseline", baseline: "put", status: "ok", values: map[string]float64{
			"requests_per_second": 1000, "p99_ms": 3,
		}},
		{round: "1", backend: "bbolt", scenario: "put-compaction", role: "treatment", baseline: "put", status: "ok", values: map[string]float64{
			"requests_per_second": 900, "p99_ms": 4,
		}},
		{round: "1", backend: "pebble", scenario: "put-compaction", role: "baseline", baseline: "put", status: "ok", values: map[string]float64{
			"requests_per_second": 1200, "p99_ms": 2,
		}},
		{round: "1", backend: "pebble", scenario: "put-compaction", role: "treatment", baseline: "put", status: "ok", values: map[string]float64{
			"requests_per_second": 1100, "p99_ms": 3,
		}},
		{round: "1", backend: "bbolt", scenario: "range-compaction", role: "baseline", baseline: "range-value-serializable", status: "ok", values: map[string]float64{
			"requests_per_second": 1000, "p99_ms": 3,
		}},
		{round: "1", backend: "bbolt", scenario: "range-compaction", role: "treatment", baseline: "range-value-serializable", status: "ok", values: map[string]float64{
			"requests_per_second": 900, "p99_ms": 4,
		}},
		{round: "1", backend: "pebble", scenario: "range-compaction", role: "baseline", baseline: "range-value-serializable", status: "ok", values: map[string]float64{
			"requests_per_second": 1200, "p99_ms": 2,
		}},
		{round: "1", backend: "pebble", scenario: "range-compaction", role: "treatment", baseline: "range-value-serializable", status: "ok", values: map[string]float64{
			"requests_per_second": 1100, "p99_ms": 3,
		}},
	}
	report, err := renderHTMLReport(samples, compactionSamples, map[string]string{"rounds": "1"}, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"etcd backend performance report", "Workload comparison", "pebble · 20.0% higher", "Storage efficiency", "MVCC compaction impact", "put-compaction", "range-compaction", "range-value-serializable", "10.0%", "33.3%", "50.0%"} {
		if !strings.Contains(report, want) {
			t.Fatalf("HTML report does not contain %q", want)
		}
	}
	if strings.Contains(report, "Snapshot and member catch-up") || strings.Contains(report, "Defrag") {
		t.Fatalf("HTML report contains removed scenarios")
	}
}

func formatTestFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
