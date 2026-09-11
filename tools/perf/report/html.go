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
	"encoding/csv"
	"errors"
	"fmt"
	"html/template"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

type htmlReport struct {
	Title       string
	Description string
	Metadata    []htmlMetadata
	Backends    []string
	Summary     []htmlSummary
	Workloads   []htmlWorkload
	Storage     []htmlStorage
	Compaction  []htmlCompaction
	RawRounds   []htmlRound
	Regression  string
}

type htmlMetadata struct {
	Label string
	Value string
}

type htmlSummary struct {
	Backend        string
	ThroughputWins int
	LatencyWins    int
}

type htmlBackendMetric struct {
	Backend  string
	Requests string
	P50      string
	P95      string
	P99      string
}

type htmlWorkload struct {
	Group            string
	Name             string
	Metrics          []htmlBackendMetric
	ThroughputResult string
	ThroughputWinner string
	P99Result        string
	P99Winner        string
}

type htmlStorage struct {
	Backend  string
	Round    string
	Physical string
	InUse    string
	Ratio    string
}

type htmlCompaction struct {
	Backend          string
	Scenario         string
	Baseline         string
	Rounds           int
	ThroughputRatio  string
	P99Ratio         string
	ThroughputImpact string
	P99Increase      string
}

type htmlRound struct {
	Round    string
	Backend  string
	Workload string
	Requests string
	P50      string
	P95      string
	P99      string
	Status   string
}

func renderHTMLReport(samples []matrixSample, compactionSamples []compactionSample, metadata map[string]string, systemTSV string, regressionError error) (string, error) {
	byWorkload, workloads, backends := aggregateMatrixSamples(samples)
	report := htmlReport{
		Title:       "etcd backend performance report",
		Description: "A static comparison of configured etcd storage backends under the same benchmark conditions.",
		Backends:    backends,
		Metadata:    htmlMetadataRows(metadata, len(samples), backends),
		RawRounds:   make([]htmlRound, 0, len(samples)),
	}

	throughputWins, latencyWins := matrixWinCounts(byWorkload, workloads, backends)
	for _, backend := range backends {
		report.Summary = append(report.Summary, htmlSummary{
			Backend:        backend,
			ThroughputWins: throughputWins[backend],
			LatencyWins:    latencyWins[backend],
		})
	}

	for _, workload := range workloads {
		byBackend := byWorkload[workload]
		throughput := compareMatrixValues(byBackend, backends, "requests_per_second", false)
		p99 := compareMatrixValues(byBackend, backends, "p99_ms", true)
		report.Workloads = append(report.Workloads, htmlWorkload{
			Group:            matrixWorkloadGroup(workload),
			Name:             workload,
			Metrics:          htmlWorkloadMetrics(byBackend, backends),
			ThroughputResult: formatHTMLComparison(throughput, false),
			ThroughputWinner: throughput.winner,
			P99Result:        formatHTMLComparison(p99, true),
			P99Winner:        p99.winner,
		})
	}
	for _, sample := range samples {
		report.RawRounds = append(report.RawRounds, htmlRound{
			Round: sample.round, Backend: sample.backend, Workload: sample.workload,
			Requests: formatFloat(sample.values["requests_per_second"]),
			P50:      formatFloat(sample.values["p50_ms"]),
			P95:      formatFloat(sample.values["p95_ms"]),
			P99:      formatFloat(sample.values["p99_ms"]), Status: sample.status,
		})
	}

	var err error
	report.Storage, err = parseHTMLStorage(systemTSV)
	if err != nil {
		return "", err
	}
	for _, impact := range collectCompactionImpacts(compactionSamples) {
		report.Compaction = append(report.Compaction, htmlCompaction{
			Backend: impact.backend, Scenario: impact.scenario, Baseline: impact.baseline, Rounds: impact.rounds,
			ThroughputRatio:  formatFloat(impact.throughputRatio),
			P99Ratio:         formatFloat(impact.p99Ratio),
			ThroughputImpact: formatHTMLPercent(impact.throughputImpact),
			P99Increase:      formatHTMLPercent(impact.p99Increase),
		})
	}
	if regressionError != nil {
		report.Regression = regressionError.Error()
	}

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"join": strings.Join,
	}).Parse(htmlReportTemplate)
	if err != nil {
		return "", err
	}
	var output strings.Builder
	if err := tmpl.Execute(&output, report); err != nil {
		return "", err
	}
	return output.String(), nil
}

func htmlMetadataRows(metadata map[string]string, sampleCount int, backends []string) []htmlMetadata {
	rows := []htmlMetadata{{Label: "Backends", Value: strings.Join(backends, ", ")}}
	keys := []struct{ key, label string }{
		{"rounds", "Rounds"},
		{"operations", "Operations"},
		{"read_operations", "Read operations"},
		{"clients", "Clients"},
		{"connections", "Connections"},
		{"value_size", "Value size"},
		{"git_sha", "Git SHA"},
	}
	for _, item := range keys {
		value := "-"
		if metadata != nil && metadata[item.key] != "" {
			value = metadata[item.key]
		}
		rows = append(rows, htmlMetadata{Label: item.label, Value: value})
	}
	if len(metadata) == 0 {
		rows = append(rows, htmlMetadata{Label: "Samples", Value: strconv.Itoa(sampleCount)})
	}
	return rows
}

func htmlWorkloadMetrics(byBackend map[string]*matrixMetricAggregate, backends []string) []htmlBackendMetric {
	metrics := make([]htmlBackendMetric, 0, len(backends))
	for _, backend := range backends {
		a := byBackend[backend]
		metrics = append(metrics, htmlBackendMetric{
			Backend:  backend,
			Requests: formatMatrixMetric(a, "requests_per_second"),
			P50:      formatMatrixMetric(a, "p50_ms"),
			P95:      formatMatrixMetric(a, "p95_ms"),
			P99:      formatMatrixMetric(a, "p99_ms"),
		})
	}
	return metrics
}

func formatHTMLComparison(comparison matrixComparison, lowerIsBetter bool) string {
	if comparison.winner == "" {
		return "-"
	}
	if comparison.tie {
		return "Tie"
	}
	if math.IsNaN(comparison.gap) {
		return comparison.winner + " only"
	}
	if lowerIsBetter {
		return fmt.Sprintf("%s · %.1f%% lower", comparison.winner, comparison.gap)
	}
	return fmt.Sprintf("%s · %.1f%% higher", comparison.winner, comparison.gap)
}

func parseHTMLStorage(input string) ([]htmlStorage, error) {
	if strings.TrimSpace(input) == "" {
		return nil, nil
	}
	r := csv.NewReader(strings.NewReader(input))
	r.Comma = '\t'
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	columns := htmlColumnIndexes(header)
	type storageAggregate struct{ physical, inUse float64 }
	aggregates := make(map[string]*storageAggregate)
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		key := row[columns["backend"]] + "\x00" + row[columns["round"]]
		a := aggregates[key]
		if a == nil {
			a = &storageAggregate{}
			aggregates[key] = a
		}
		a.physical += parseHTMLNumber(row[columns["physical_bytes"]])
		a.inUse += parseHTMLNumber(row[columns["backend_in_use_bytes"]])
	}
	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]htmlStorage, 0, len(keys))
	for _, key := range keys {
		parts := strings.Split(key, "\x00")
		a := aggregates[key]
		ratio := "-"
		if a.inUse > 0 {
			ratio = formatFloat(a.physical / a.inUse)
		}
		result = append(result, htmlStorage{
			Backend: parts[0], Round: parts[1],
			Physical: formatFloat(a.physical), InUse: formatFloat(a.inUse), Ratio: ratio,
		})
	}
	return result, nil
}

func htmlColumnIndexes(header []string) map[string]int {
	columns := make(map[string]int, len(header))
	for i, name := range header {
		columns[name] = i
	}
	return columns
}

func parseHTMLNumber(value string) float64 {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func formatHTMLPercent(value float64) string {
	return fmt.Sprintf("%.1f%%", value*100)
}

const htmlReportTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}}</title>
<style>
:root { color-scheme: light; --ink:#182230; --muted:#667085; --line:#e4e7ec; --surface:#fff; --canvas:#f5f7fb; --blue:#2563eb; --green:#087443; --green-bg:#e9f8ef; --amber:#9a6700; --amber-bg:#fff6db; }
* { box-sizing:border-box; }
body { margin:0; background:var(--canvas); color:var(--ink); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
.page { max-width:1440px; margin:0 auto; padding:32px 28px 64px; }
.hero { background:linear-gradient(135deg,#172554,#1d4ed8); color:#fff; border-radius:18px; padding:30px 34px; box-shadow:0 12px 30px rgba(29,78,216,.18); }
.eyebrow { font-size:11px; letter-spacing:.14em; opacity:.72; font-weight:700; }
h1 { margin:6px 0 8px; font-size:30px; letter-spacing:-.03em; }
.hero p { margin:0; color:#dbeafe; }
.meta-grid { display:flex; flex-wrap:wrap; gap:8px 22px; margin-top:22px; color:#dbeafe; font-size:12px; }
.meta-grid span strong { color:#fff; font-weight:650; }
h2 { margin:34px 0 14px; font-size:20px; letter-spacing:-.02em; }
h3 { margin:26px 0 12px; font-size:15px; }
.summary-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:14px; margin-top:22px; }
.card { background:var(--surface); border:1px solid var(--line); border-radius:14px; padding:18px; box-shadow:0 3px 12px rgba(16,24,40,.04); }
.card-label { color:var(--muted); font-size:12px; }
.card-value { margin-top:3px; font-size:23px; font-weight:750; }
.card-sub { color:var(--muted); font-size:12px; }
.panel { background:var(--surface); border:1px solid var(--line); border-radius:14px; overflow:hidden; box-shadow:0 3px 12px rgba(16,24,40,.04); }
.table-wrap { overflow-x:auto; }
table { width:100%; border-collapse:collapse; min-width:760px; }
th,td { padding:12px 14px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; white-space:nowrap; }
th { background:#f8fafc; color:#475467; font-size:11px; text-transform:uppercase; letter-spacing:.06em; position:sticky; top:0; z-index:1; }
tbody tr:last-child td { border-bottom:0; }
tbody tr:hover { background:#f8fbff; }
.workload { font-weight:700; }
.group { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.05em; }
.backend-cell strong { display:block; font-size:15px; }
.backend-cell span { display:block; color:var(--muted); font-size:11px; margin-top:3px; }
.result { font-weight:650; color:var(--green); }
.result.tie { color:var(--amber); }
.muted { color:var(--muted); }
.section-note { color:var(--muted); margin:-4px 0 14px; }
.details { margin-top:14px; }
details { background:var(--surface); border:1px solid var(--line); border-radius:12px; padding:13px 16px; }
summary { cursor:pointer; color:#344054; font-weight:650; }
.config { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:10px 20px; padding:18px; }
.config div { border-bottom:1px solid var(--line); padding-bottom:8px; }
.config dt { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.05em; }
.config dd { margin:2px 0 0; font-weight:600; overflow:hidden; text-overflow:ellipsis; }
.status-ok { color:var(--green); font-weight:650; }
.status-failed { color:#b42318; font-weight:650; }
.callout { margin-top:20px; padding:14px 16px; border-radius:10px; background:var(--amber-bg); color:var(--amber); }
input { width:260px; max-width:100%; border:1px solid #d0d5dd; border-radius:8px; padding:9px 11px; font:inherit; }
.toolbar { display:flex; justify-content:space-between; align-items:center; gap:12px; margin-bottom:10px; }
@media (max-width:700px) { .page { padding:18px 12px 40px; } .hero { padding:24px 20px; } h1 { font-size:24px; } .toolbar { display:block; } input { margin-top:8px; } }
</style>
</head>
<body>
<main class="page">
<header class="hero">
  <div class="eyebrow">ETCD PERFORMANCE REPORT</div>
  <h1>{{.Title}}</h1>
  <p>{{.Description}}</p>
  <div class="meta-grid"><span>Backends: <strong>{{join .Backends ", "}}</strong></span><span>Rounds: <strong>{{range .Metadata}}{{if eq .Label "Rounds"}}{{.Value}}{{end}}{{end}}</strong></span><span>Evidence: <strong>benchmark TSV + storage artifacts</strong></span></div>
</header>

<section class="summary-grid">
{{range .Summary}}<article class="card"><div class="card-label">{{.Backend}}</div><div class="card-value">{{.ThroughputWins}} / {{.LatencyWins}}</div><div class="card-sub">throughput wins / p99 wins</div></article>{{end}}
</section>

<section><h2>Workload comparison</h2><p class="section-note">Requests per second is higher-is-better. Latency values are milliseconds and lower-is-better.</p>
<div class="toolbar"><span class="muted">Official etcd benchmark workloads</span><input id="workload-filter" placeholder="Filter workloads..." oninput="filterTable('workload-table', this.value)"></div>
<div class="panel table-wrap"><table id="workload-table"><thead><tr><th>Workload</th>{{range .Backends}}<th>{{.}}</th>{{end}}<th>Throughput result</th><th>p99 result</th></tr></thead><tbody>
{{range .Workloads}}<tr><td><div class="workload">{{.Name}}</div><div class="group">{{.Group}}</div></td>{{range .Metrics}}<td class="backend-cell"><strong>{{.Requests}} req/s</strong><span>p50 {{.P50}} · p95 {{.P95}} · p99 {{.P99}}</span></td>{{end}}<td class="result">{{.ThroughputResult}}</td><td class="result">{{.P99Result}}</td></tr>{{end}}
</tbody></table></div></section>

<section><h2>Storage efficiency</h2><p class="section-note">Physical size is compared with etcd's logical backend allocation. This is a storage-efficiency proxy, not application payload size.</p>
<div class="panel table-wrap"><table><thead><tr><th>Backend</th><th>Round</th><th>Physical bytes</th><th>Backend in-use</th><th>Physical / in-use</th></tr></thead><tbody>{{range .Storage}}<tr><td class="workload">{{.Backend}}</td><td>{{.Round}}</td><td>{{.Physical}}</td><td>{{.InUse}}</td><td class="result">{{.Ratio}}</td></tr>{{end}}</tbody></table></div></section>

{{if .Compaction}}<section><h2>MVCC compaction impact</h2><p class="section-note">Each row compares paired baseline and treatment runs from fresh clusters. Lower foreground impact is better; this is a performance measurement, not a correctness test.</p><div class="panel table-wrap"><table><thead><tr><th>Backend</th><th>Workload</th><th>Baseline</th><th>Paired rounds</th><th>Throughput ratio</th><th>p99 ratio</th><th>Throughput impact</th><th>p99 increase</th></tr></thead><tbody>{{range .Compaction}}<tr><td>{{.Backend}}</td><td>{{.Scenario}}</td><td>{{.Baseline}}</td><td>{{.Rounds}}</td><td>{{.ThroughputRatio}}</td><td>{{.P99Ratio}}</td><td class="result">{{.ThroughputImpact}}</td><td class="result">{{.P99Increase}}</td></tr>{{end}}</tbody></table></div></section>{{end}}

{{if .Regression}}<div class="callout"><strong>Regression gate failed:</strong> {{.Regression}}</div>{{end}}

<section><h2>Test configuration</h2><div class="panel config">{{range .Metadata}}<div><dl><dt>{{.Label}}</dt><dd>{{.Value}}</dd></dl></div>{{end}}</div></section>

<section class="details"><details><summary>Raw rounds</summary><div class="table-wrap"><table><thead><tr><th>Round</th><th>Backend</th><th>Workload</th><th>Req/s</th><th>p50</th><th>p95</th><th>p99</th><th>Status</th></tr></thead><tbody>{{range .RawRounds}}<tr><td>{{.Round}}</td><td>{{.Backend}}</td><td>{{.Workload}}</td><td>{{.Requests}}</td><td>{{.P50}}</td><td>{{.P95}}</td><td>{{.P99}}</td><td class="status-{{.Status}}">{{.Status}}</td></tr>{{end}}</tbody></table></div></details></section>
</main>
<script>function filterTable(id, q){q=q.toLowerCase();document.querySelectorAll('#'+id+' tbody tr').forEach(function(r){r.style.display=r.textContent.toLowerCase().indexOf(q)>=0?'':'none';});}</script>
</body></html>`
