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

// Command perf-report reads the workload matrix emitted by the
// local performance lab and produces a reviewable report. It intentionally
// does not own Docker or workload execution; the official etcd benchmark
// remains the workload generator. Fault-injection scenarios live in the
// official tests/e2e and tests/robustness suites and are not part of report
// generation.
package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type matrixSample struct {
	round    string
	mode     string
	backend  string
	workload string
	status   string
	values   map[string]float64
}

type matrixMetricAggregate struct {
	count  int
	values map[string]float64
	valid  map[string]int
}

func main() {
	input := flag.String("input", "", "workload matrix emitted by benchmark.sh")
	compactionInput := flag.String("compaction-input", "", "paired compaction matrix emitted by benchmark.sh")
	output := flag.String("output", "", "write Markdown to this file instead of stdout")
	htmlOutput := flag.String("html-output", "", "write a self-contained HTML report to this file")
	systemOutput := flag.String("system-output", "", "write the storage metrics TSV to this file")
	baselineMode := flag.String("baseline-mode", "", "mode used as the performance baseline")
	candidateMode := flag.String("candidate-mode", "", "mode checked against the baseline")
	workload := flag.String("workload", "put", "workload used by the regression gate for a matrix input")
	minWriteRPSRatio := flag.Float64("min-write-rps-ratio", 0, "fail when candidate write RPS / baseline write RPS is below this ratio")
	maxWriteP99Ratio := flag.Float64("max-write-p99-ratio", 0, "fail when candidate write p99 / baseline write p99 is above this ratio")
	flag.Parse()

	if *input == "" {
		fmt.Fprintln(os.Stderr, "usage: go run ./tools/perf/report -input results/matrix-l.tsv -output results/report.md -html-output results/report.html")
		os.Exit(2)
	}

	matrixInput, err := isMatrixInput(*input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect performance summary: %v\n", err)
		os.Exit(1)
	}
	if !matrixInput {
		fmt.Fprintln(os.Stderr, "input must be the workload matrix emitted by benchmark.sh")
		os.Exit(2)
	}

	var report string
	var regressionError error
	systemTSV := ""
	matrixSamples, readErr := readMatrixSamples(*input)
	if readErr != nil {
		fmt.Fprintf(os.Stderr, "read performance matrix: %v\n", readErr)
		os.Exit(1)
	}
	if *compactionInput == "" {
		if load := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(*input), "matrix-"), ".tsv"); load != filepath.Base(*input) {
			candidate := filepath.Join(filepath.Dir(*input), "compaction-"+load+".tsv")
			if _, err := os.Stat(candidate); err == nil {
				*compactionInput = candidate
			}
		}
	}
	var compactionSamples []compactionSample
	if *compactionInput != "" {
		compactionSamples, readErr = readCompactionSamples(*compactionInput)
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "read compaction matrix: %v\n", readErr)
			os.Exit(1)
		}
	}
	metadata, metadataErr := readManifest(filepath.Join(filepath.Dir(*input), "manifest.env"))
	if metadataErr != nil && !errors.Is(metadataErr, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "read benchmark manifest: %v\n", metadataErr)
	}
	report = renderMatrixReportWithMetadata(matrixSamples, metadata)
	systemReport, systemData, systemErr := renderSystemMetricsReport(filepath.Dir(*input), filepath.Base(*input), matrixSamples)
	if systemErr != nil {
		fmt.Fprintf(os.Stderr, "read system metrics: %v\n", systemErr)
	} else {
		systemTSV = systemData
		report += systemReport
		if *systemOutput != "" {
			if err := os.WriteFile(*systemOutput, []byte(systemTSV), 0o644); err != nil {
				fmt.Fprintf(os.Stderr, "write system metrics: %v\n", err)
				os.Exit(1)
			}
		}
	}
	report += renderCompactionReport(compactionSamples)
	regressionError = checkMatrixRegression(matrixSamples, *baselineMode, *candidateMode, *workload, *minWriteRPSRatio, *maxWriteP99Ratio)
	if *htmlOutput != "" {
		htmlReport, htmlErr := renderHTMLReport(matrixSamples, compactionSamples, metadata, systemTSV, regressionError)
		if htmlErr != nil {
			fmt.Fprintf(os.Stderr, "render HTML performance report: %v\n", htmlErr)
			os.Exit(1)
		}
		if err := os.WriteFile(*htmlOutput, []byte(htmlReport), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write HTML performance report: %v\n", err)
			os.Exit(1)
		}
	}
	if regressionError != nil {
		report += "\n## Regression gate\n\n"
		report += "**FAILED:** " + regressionError.Error() + "\n"
	}

	if *output == "" {
		fmt.Print(report)
	} else {
		if err := os.WriteFile(*output, []byte(report), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "write performance report: %v\n", err)
			os.Exit(1)
		}
	}
	if regressionError != nil {
		fmt.Fprintln(os.Stderr, regressionError)
		os.Exit(1)
	}
}

func isMatrixInput(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '\t'
	header, err := r.Read()
	if err != nil {
		return false, err
	}
	for _, name := range header {
		if name == "workload" {
			return true, nil
		}
	}
	return false, nil
}

var matrixMetricNames = []string{
	"requests_per_second",
	"p50_ms",
	"p95_ms",
	"p99_ms",
}

func readMatrixSamples(path string) ([]matrixSample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '\t'
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	columns := make(map[string]int, len(header))
	for i, name := range header {
		columns[name] = i
	}
	for _, required := range append([]string{"round", "mode", "backend", "workload", "status"}, matrixMetricNames...) {
		if _, ok := columns[required]; !ok {
			return nil, fmt.Errorf("matrix is missing column %q", required)
		}
	}

	var samples []matrixSample
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(row) != len(header) {
			return nil, fmt.Errorf("row has %d columns, want %d", len(row), len(header))
		}
		s := matrixSample{
			round:    row[columns["round"]],
			mode:     row[columns["mode"]],
			backend:  row[columns["backend"]],
			workload: row[columns["workload"]],
			status:   row[columns["status"]],
			values:   make(map[string]float64),
		}
		for _, name := range matrixMetricNames {
			value := strings.TrimSpace(row[columns[name]])
			if value == "" || value == "skipped" {
				s.values[name] = math.NaN()
				continue
			}
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, fmt.Errorf("parse %s=%q: %w", name, value, err)
			}
			s.values[name] = parsed
		}
		samples = append(samples, s)
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("matrix contains no samples")
	}
	return samples, nil
}

func readCompactionSamples(path string) ([]compactionSample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.Comma = '\t'
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	columns := make(map[string]int, len(header))
	for i, name := range header {
		columns[name] = i
	}
	for _, required := range append([]string{"round", "mode", "backend", "scenario", "role", "baseline", "status"}, matrixMetricNames...) {
		if _, ok := columns[required]; !ok {
			return nil, fmt.Errorf("compaction matrix is missing column %q", required)
		}
	}

	var samples []compactionSample
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(row) != len(header) {
			return nil, fmt.Errorf("compaction row has %d columns, want %d", len(row), len(header))
		}
		s := compactionSample{
			round:    row[columns["round"]],
			mode:     row[columns["mode"]],
			backend:  row[columns["backend"]],
			scenario: row[columns["scenario"]],
			role:     row[columns["role"]],
			baseline: row[columns["baseline"]],
			status:   row[columns["status"]],
			values:   make(map[string]float64),
		}
		for _, name := range matrixMetricNames {
			value := strings.TrimSpace(row[columns[name]])
			if value == "" || value == "skipped" {
				s.values[name] = math.NaN()
				continue
			}
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, fmt.Errorf("parse compaction %s=%q: %w", name, value, err)
			}
			s.values[name] = parsed
		}
		samples = append(samples, s)
	}
	return samples, nil
}

func aggregateMatrixSamples(samples []matrixSample) (map[string]map[string]*matrixMetricAggregate, []string, []string) {
	byWorkload := make(map[string]map[string]*matrixMetricAggregate)
	var workloads []string
	seenWorkloads := make(map[string]struct{})
	backends := make(map[string]struct{})

	for _, s := range samples {
		if _, ok := seenWorkloads[s.workload]; !ok {
			seenWorkloads[s.workload] = struct{}{}
			workloads = append(workloads, s.workload)
		}
		backends[s.backend] = struct{}{}
		if byWorkload[s.workload] == nil {
			byWorkload[s.workload] = make(map[string]*matrixMetricAggregate)
		}
		a := byWorkload[s.workload][s.backend]
		if a == nil {
			a = &matrixMetricAggregate{values: make(map[string]float64), valid: make(map[string]int)}
			byWorkload[s.workload][s.backend] = a
		}
		if s.status != "ok" {
			continue
		}
		a.count++
		for _, name := range matrixMetricNames {
			if !math.IsNaN(s.values[name]) {
				a.values[name] += s.values[name]
				a.valid[name]++
			}
		}
	}

	for _, byBackend := range byWorkload {
		for _, a := range byBackend {
			for _, name := range matrixMetricNames {
				if a.valid[name] == 0 {
					a.values[name] = math.NaN()
				} else {
					a.values[name] /= float64(a.valid[name])
				}
			}
		}
	}

	backendList := make([]string, 0, len(backends))
	for backend := range backends {
		backendList = append(backendList, backend)
	}
	sort.Strings(backendList)
	return byWorkload, workloads, backendList
}

func readManifest(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	manifest := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok && key != "" {
			manifest[key] = value
		}
	}
	return manifest, nil
}

func renderMatrixReport(samples []matrixSample) string {
	return renderMatrixReportWithMetadata(samples, nil)
}

func renderMatrixReportWithMetadata(samples []matrixSample, metadata map[string]string) string {
	byWorkload, workloads, backends := aggregateMatrixSamples(samples)
	var b strings.Builder
	b.WriteString("# etcd backend performance comparison\n\n")
	b.WriteString("Throughput is higher-is-better; latency is lower-is-better. Values are arithmetic means across successful rounds.\n\n")

	rounds := countMatrixRounds(samples)
	b.WriteString("## Test configuration\n\n")
	b.WriteString("| parameter | value |\n|---|---|\n")
	fmt.Fprintf(&b, "| backends | %s |\n", strings.Join(backends, ", "))
	writeManifestRow(&b, metadata, "rounds", "rounds", strconv.Itoa(rounds))
	writeManifestRow(&b, metadata, "operations", "operations", "-")
	writeManifestRow(&b, metadata, "read_operations", "read operations", "-")
	writeManifestRow(&b, metadata, "clients", "clients", "-")
	writeManifestRow(&b, metadata, "connections", "connections", "-")
	writeManifestRow(&b, metadata, "value_size", "value size (bytes)", "-")
	if metadata != nil && metadata["git_sha"] != "" {
		fmt.Fprintf(&b, "| git sha | `%s` |\n", metadata["git_sha"])
	}

	b.WriteString("\n## Summary\n\n")
	b.WriteString("| backend | throughput wins | p99 latency wins |\n|---|---:|---:|\n")
	throughputWins, latencyWins := matrixWinCounts(byWorkload, workloads, backends)
	for _, backend := range backends {
		fmt.Fprintf(&b, "| %s | %d | %d |\n", backend, throughputWins[backend], latencyWins[backend])
	}

	b.WriteString("\n## Workload comparison\n\n")
	groupOrder := []string{"Write workloads", "Read workloads", "Transaction workloads", "Watch and coordination workloads", "Other workloads"}
	grouped := make(map[string][]string)
	for _, workload := range workloads {
		grouped[matrixWorkloadGroup(workload)] = append(grouped[matrixWorkloadGroup(workload)], workload)
	}
	for _, group := range groupOrder {
		groupWorkloads := grouped[group]
		if len(groupWorkloads) == 0 {
			continue
		}
		fmt.Fprintf(&b, "### %s\n\n", group)
		b.WriteString("| workload | metric | ")
		for _, backend := range backends {
			fmt.Fprintf(&b, "%s | ", backend)
		}
		b.WriteString("result | rounds |\n|---|---|")
		for range backends {
			b.WriteString("---:|")
		}
		b.WriteString("---|---:|\n")
		for _, workload := range groupWorkloads {
			byBackend := byWorkload[workload]
			fmt.Fprintf(&b, "| %s | req/s | ", workload)
			for _, backend := range backends {
				fmt.Fprintf(&b, "%s | ", formatMatrixMetric(byBackend[backend], "requests_per_second"))
			}
			fmt.Fprintf(&b, "%s | %d |\n", formatMatrixComparison(byBackend, backends, "requests_per_second", false), matrixRoundCount(byBackend))

			fmt.Fprintf(&b, "| %s | p99 latency (ms) | ", workload)
			for _, backend := range backends {
				fmt.Fprintf(&b, "%s | ", formatMatrixMetric(byBackend[backend], "p99_ms"))
			}
			fmt.Fprintf(&b, "%s | %d |\n", formatMatrixComparison(byBackend, backends, "p99_ms", true), matrixRoundCount(byBackend))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Detailed latency percentiles\n\n")
	b.WriteString("| workload | backend | rounds | p50 (ms) | p95 (ms) | p99 (ms) |\n|---|---|---:|---:|---:|---:|\n")
	for _, workload := range workloads {
		for _, backend := range backends {
			a := byWorkload[workload][backend]
			if a == nil {
				continue
			}
			fmt.Fprintf(&b, "| %s | %s | %d | %s | %s | %s |\n",
				workload,
				backend,
				a.count,
				formatMatrixMetric(a, "p50_ms"),
				formatMatrixMetric(a, "p95_ms"),
				formatMatrixMetric(a, "p99_ms"),
			)
		}
	}

	b.WriteString("\n<details>\n<summary>Raw rounds</summary>\n\n")
	b.WriteString("| round | backend | workload | req/s | p50 (ms) | p95 (ms) | p99 (ms) | status |\n")
	b.WriteString("|---:|---|---|---:|---:|---:|---:|---|\n")
	for _, s := range samples {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n",
			s.round,
			s.backend,
			s.workload,
			formatFloat(s.values["requests_per_second"]),
			formatFloat(s.values["p50_ms"]),
			formatFloat(s.values["p95_ms"]),
			formatFloat(s.values["p99_ms"]),
			s.status,
		)
	}
	b.WriteString("\n</details>\n")
	return b.String()
}

func writeManifestRow(b *strings.Builder, metadata map[string]string, key, label, fallback string) {
	value := fallback
	if metadata != nil && metadata[key] != "" {
		value = metadata[key]
	}
	fmt.Fprintf(b, "| %s | %s |\n", label, value)
}

func countMatrixRounds(samples []matrixSample) int {
	rounds := make(map[string]struct{})
	for _, sample := range samples {
		rounds[sample.round] = struct{}{}
	}
	return len(rounds)
}

func matrixWinCounts(byWorkload map[string]map[string]*matrixMetricAggregate, workloads, backends []string) (map[string]int, map[string]int) {
	throughputWins := make(map[string]int)
	latencyWins := make(map[string]int)
	for _, workload := range workloads {
		byBackend := byWorkload[workload]
		throughput := compareMatrixValues(byBackend, backends, "requests_per_second", false)
		if throughput.winner != "" && !throughput.tie {
			throughputWins[throughput.winner]++
		}
		latency := compareMatrixValues(byBackend, backends, "p99_ms", true)
		if latency.winner != "" && !latency.tie {
			latencyWins[latency.winner]++
		}
	}
	return throughputWins, latencyWins
}

func matrixWorkloadGroup(workload string) string {
	switch {
	case workload == "put":
		return "Write workloads"
	case strings.HasPrefix(workload, "range-"):
		return "Read workloads"
	case strings.HasPrefix(workload, "txn-"):
		return "Transaction workloads"
	case strings.HasPrefix(workload, "watch-") || workload == "watch" || workload == "lease-keepalive" || workload == "stm":
		return "Watch and coordination workloads"
	default:
		return "Other workloads"
	}
}

func metricFileIdentity(name, load string, modes map[string]struct{}) (string, string, string, bool) {
	stem := strings.TrimSuffix(name, ".backend-size.after")
	modeList := make([]string, 0, len(modes))
	for mode := range modes {
		modeList = append(modeList, mode)
	}
	sort.Slice(modeList, func(i, j int) bool { return len(modeList[i]) > len(modeList[j]) })
	for _, mode := range modeList {
		prefix := mode + "-" + load + "-r"
		if strings.HasPrefix(stem, prefix) {
			suffix := strings.TrimPrefix(stem, prefix)
			if i := strings.LastIndexByte(suffix, '-'); i >= 0 {
				member := suffix[i+1:]
				if member == "etcd1" || member == "etcd2" || member == "etcd3" {
					return mode, suffix[:i], member, true
				}
			}
			return mode, suffix, "unknown", true
		}
	}
	return "", "", "", false
}

func parsePrometheusSample(line string) (string, map[string]string, float64, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", nil, 0, false
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", nil, 0, false
	}
	value, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return "", nil, 0, false
	}
	metric := fields[0]
	labels := make(map[string]string)
	if open := strings.IndexByte(metric, '{'); open >= 0 {
		close := strings.LastIndexByte(metric, '}')
		if close <= open {
			return "", nil, 0, false
		}
		labelText := metric[open+1 : close]
		for _, label := range strings.Split(labelText, ",") {
			parts := strings.SplitN(label, "=", 2)
			if len(parts) != 2 {
				continue
			}
			value, err := strconv.Unquote(parts[1])
			if err != nil {
				continue
			}
			labels[parts[0]] = value
		}
		metric = metric[:open]
	}
	return metric, labels, value, true
}

func matrixRoundCount(byBackend map[string]*matrixMetricAggregate) int {
	count := 0
	for _, a := range byBackend {
		if a.count > count {
			count = a.count
		}
	}
	return count
}

func formatMatrixMetric(a *matrixMetricAggregate, metric string) string {
	if a == nil {
		return "-"
	}
	return formatFloat(a.values[metric])
}

func formatFloat(value float64) string {
	if math.IsNaN(value) {
		return "-"
	}
	return strconv.FormatFloat(value, 'f', 3, 64)
}

type matrixComparison struct {
	winner string
	gap    float64
	tie    bool
}

func formatMatrixComparison(byBackend map[string]*matrixMetricAggregate, backends []string, metric string, lowerIsBetter bool) string {
	comparison := compareMatrixValues(byBackend, backends, metric, lowerIsBetter)
	if comparison.winner == "" {
		return "-"
	}
	if comparison.tie {
		return "tie"
	}
	if math.IsNaN(comparison.gap) {
		return comparison.winner + " (only)"
	}
	if lowerIsBetter {
		return fmt.Sprintf("%s · %.1f%% lower", comparison.winner, comparison.gap)
	}
	return fmt.Sprintf("%s · %.1f%% higher", comparison.winner, comparison.gap)
}

func compareMatrixValues(byBackend map[string]*matrixMetricAggregate, backends []string, metric string, lowerIsBetter bool) matrixComparison {
	values := make(map[string]float64)
	for _, backend := range backends {
		a := byBackend[backend]
		if a != nil && !math.IsNaN(a.values[metric]) {
			values[backend] = a.values[metric]
		}
	}
	return compareFloatValues(values, backends, lowerIsBetter)
}

func compareMatrixMetric(byBackend map[string]*matrixMetricAggregate, backends []string, metric string, lowerIsBetter bool) string {
	comparison := compareMatrixValues(byBackend, backends, metric, lowerIsBetter)
	if comparison.winner == "" {
		return "-"
	}
	if comparison.tie {
		return "tie"
	}
	if math.IsNaN(comparison.gap) {
		return comparison.winner
	}
	return fmt.Sprintf("%s +%.1f%%", comparison.winner, comparison.gap)
}

func compareFloatValues(values map[string]float64, backends []string, lowerIsBetter bool) matrixComparison {
	var bestBackend, worstBackend string
	var best, worst float64
	validValues := 0
	for _, backend := range backends {
		value, ok := values[backend]
		if !ok || math.IsNaN(value) {
			continue
		}
		validValues++
		if bestBackend == "" || (lowerIsBetter && value < best) || (!lowerIsBetter && value > best) {
			bestBackend, best = backend, value
		}
		if worstBackend == "" || (lowerIsBetter && value > worst) || (!lowerIsBetter && value < worst) {
			worstBackend, worst = backend, value
		}
	}
	if bestBackend == "" {
		return matrixComparison{}
	}
	if validValues == 1 {
		return matrixComparison{winner: bestBackend, gap: math.NaN()}
	}
	if best == worst {
		return matrixComparison{winner: bestBackend, tie: true}
	}
	if worst == 0 {
		return matrixComparison{winner: bestBackend, gap: math.NaN()}
	}
	gap := (best - worst) / worst * 100
	if lowerIsBetter {
		gap = (worst - best) / worst * 100
	}
	return matrixComparison{winner: bestBackend, gap: gap}
}

func checkMatrixRegression(samples []matrixSample, baselineBackend, candidateBackend, workload string, minWriteRPSRatio, maxWriteP99Ratio float64) error {
	if baselineBackend == "" || candidateBackend == "" || (minWriteRPSRatio <= 0 && maxWriteP99Ratio <= 0) {
		return nil
	}
	byWorkload, _, _ := aggregateMatrixSamples(samples)
	byBackend := byWorkload[workload]
	if byBackend == nil {
		return fmt.Errorf("regression workload not found: %q", workload)
	}
	baseline := byBackend[baselineBackend]
	candidate := byBackend[candidateBackend]
	if baseline == nil || candidate == nil {
		return fmt.Errorf("regression backends not found for workload %q: baseline=%q candidate=%q", workload, baselineBackend, candidateBackend)
	}

	var failures []string
	if minWriteRPSRatio > 0 {
		base := baseline.values["requests_per_second"]
		candidateValue := candidate.values["requests_per_second"]
		if math.IsNaN(base) || math.IsNaN(candidateValue) || base <= 0 || candidateValue/base < minWriteRPSRatio {
			failures = append(failures, fmt.Sprintf("%s request rate ratio %.3f is below %.3f", workload, candidateValue/base, minWriteRPSRatio))
		}
	}
	if maxWriteP99Ratio > 0 {
		base := baseline.values["p99_ms"]
		candidateValue := candidate.values["p99_ms"]
		if math.IsNaN(base) || math.IsNaN(candidateValue) || base <= 0 || candidateValue/base > maxWriteP99Ratio {
			failures = append(failures, fmt.Sprintf("%s p99 ratio %.3f is above %.3f", workload, candidateValue/base, maxWriteP99Ratio))
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(failures, "; "))
}
