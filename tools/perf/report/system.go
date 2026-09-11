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
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type systemMetricSample struct {
	mode    string
	backend string
	round   string
	node    string

	physicalBytes float64
	inUseBytes    float64
}

// renderSystemMetricsReport summarizes the storage-efficiency evidence used in
// the published report. The raw member rows remain available in the TSV for
// follow-up analysis, but the Markdown table stays focused on the physical to
// logical storage relationship relevant to a backend comparison.
func renderSystemMetricsReport(dir, matrixName string, samples []matrixSample) (string, string, error) {
	backendByMode := make(map[string]string)
	modeSet := make(map[string]struct{})
	for _, sample := range samples {
		backendByMode[sample.mode] = sample.backend
		modeSet[sample.mode] = struct{}{}
	}
	load := strings.TrimSuffix(strings.TrimPrefix(matrixName, "matrix-"), ".tsv")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", "", err
	}

	var records []systemMetricSample
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".backend-size.after") {
			continue
		}
		mode, round, node, ok := metricFileIdentity(entry.Name(), load, modeSet)
		if !ok {
			continue
		}
		stem := strings.TrimSuffix(entry.Name(), ".backend-size.after")
		afterValues, err := readPrometheusValues(filepath.Join(dir, entry.Name()))
		if err != nil {
			return "", "", err
		}
		afterStorage, err := readStorageBytes(filepath.Join(dir, stem+".storage.after"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", "", err
		}

		records = append(records, systemMetricSample{
			mode:          mode,
			backend:       backendByMode[mode],
			round:         round,
			node:          node,
			physicalBytes: afterStorage,
			inUseBytes:    afterValues["etcd_mvcc_db_total_size_in_use_in_bytes"],
		})
	}
	if len(records) == 0 {
		return "\n## Storage efficiency\n\nNo member storage snapshots were found.\n", "", nil
	}

	cluster := make(map[string]*systemMetricSample)
	for _, record := range records {
		key := record.backend + "\x00" + record.round
		aggregate := cluster[key]
		if aggregate == nil {
			copy := record
			copy.node = "cluster"
			cluster[key] = &copy
			continue
		}
		aggregate.physicalBytes += record.physicalBytes
		aggregate.inUseBytes += record.inUseBytes
	}

	var keys []string
	for key := range cluster {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var report strings.Builder
	report.WriteString("\n## Storage efficiency\n\n")
	report.WriteString("Physical size is the summed WAL and snapshot directory size. Backend in-use is etcd's logical backend allocation metric; it is a storage-efficiency proxy, not raw Kubernetes payload bytes.\n\n")
	report.WriteString("| backend | round | physical bytes | backend in-use bytes | physical/in-use |\n")
	report.WriteString("|---|---:|---:|---:|---:|\n")
	for _, key := range keys {
		record := cluster[key]
		fmt.Fprintf(&report, "| %s | %s | %.0f | %.0f | %s |\n",
			record.backend, record.round, record.physicalBytes, record.inUseBytes,
			formatRatio(record.physicalBytes, record.inUseBytes))
	}

	var tsv strings.Builder
	tsv.WriteString("backend\tround\tnode\tphysical_bytes\tbackend_in_use_bytes\tphysical_in_use_ratio\n")
	for _, record := range records {
		fmt.Fprintf(&tsv, "%s\t%s\t%s\t%.0f\t%.0f\t%s\n",
			record.backend, record.round, record.node, record.physicalBytes,
			record.inUseBytes, formatRatio(record.physicalBytes, record.inUseBytes))
	}
	return report.String(), tsv.String(), nil
}

func readPrometheusValues(path string) (map[string]float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	values := make(map[string]float64)
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		name, _, value, ok := parsePrometheusSample(scanner.Text())
		if ok {
			values[name] += value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func readStorageBytes(path string) (float64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var total float64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		value, parseErr := strconv.ParseFloat(fields[0], 64)
		if parseErr == nil {
			total += value
		}
	}
	return total, scanner.Err()
}

func formatRatio(numerator, denominator float64) string {
	if denominator <= 0 || math.IsNaN(numerator) || math.IsNaN(denominator) {
		return "-"
	}
	return strconv.FormatFloat(numerator/denominator, 'f', 3, 64)
}
