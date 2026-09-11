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
	"fmt"
	"strings"
)

type compactionImpact struct {
	backend          string
	scenario         string
	baseline         string
	rounds           int
	throughputRatio  float64
	p99Ratio         float64
	throughputImpact float64
	p99Increase      float64
}

type compactionSample struct {
	round    string
	mode     string
	backend  string
	scenario string
	role     string
	baseline string
	status   string
	values   map[string]float64
}

type compactionPair struct {
	baseline  *compactionSample
	treatment *compactionSample
}

func collectCompactionImpacts(samples []compactionSample) []compactionImpact {
	pairs := make(map[string]*compactionPair)
	for _, sample := range samples {
		if sample.status != "ok" || sample.role == "" {
			continue
		}
		key := strings.Join([]string{sample.round, sample.backend, sample.scenario}, "\x00")
		pair := pairs[key]
		if pair == nil {
			pair = &compactionPair{}
			pairs[key] = pair
		}
		sampleCopy := sample
		switch sample.role {
		case "baseline":
			pair.baseline = &sampleCopy
		case "treatment":
			pair.treatment = &sampleCopy
		}
	}

	grouped := make(map[string]map[string][]compactionPair)
	for _, pair := range pairs {
		if pair.baseline == nil || pair.treatment == nil {
			continue
		}
		if pair.baseline.values["requests_per_second"] <= 0 || pair.treatment.values["requests_per_second"] <= 0 ||
			pair.baseline.values["p99_ms"] <= 0 || pair.treatment.values["p99_ms"] <= 0 {
			continue
		}
		scenario := pair.treatment.scenario
		if grouped[scenario] == nil {
			grouped[scenario] = make(map[string][]compactionPair)
		}
		grouped[scenario][pair.treatment.backend] = append(grouped[scenario][pair.treatment.backend], *pair)
	}

	if len(grouped) == 0 {
		return nil
	}

	var scenarios []string
	for scenario := range grouped {
		scenarios = append(scenarios, scenario)
	}
	sortStrings(scenarios)
	impacts := make([]compactionImpact, 0)
	for _, scenario := range scenarios {
		backends := make([]string, 0, len(grouped[scenario]))
		for backend := range grouped[scenario] {
			backends = append(backends, backend)
		}
		sortStrings(backends)
		for _, backend := range backends {
			pairs := grouped[scenario][backend]
			var throughputRatio, p99Ratio float64
			for _, pair := range pairs {
				throughputRatio += pair.treatment.values["requests_per_second"] / pair.baseline.values["requests_per_second"]
				p99Ratio += pair.treatment.values["p99_ms"] / pair.baseline.values["p99_ms"]
			}
			throughputRatio /= float64(len(pairs))
			p99Ratio /= float64(len(pairs))
			impacts = append(impacts, compactionImpact{
				backend: backend, scenario: scenario, baseline: pairs[0].baseline.baseline,
				rounds:          len(pairs),
				throughputRatio: throughputRatio, p99Ratio: p99Ratio,
				throughputImpact: 1 - throughputRatio, p99Increase: p99Ratio - 1,
			})
		}
	}
	return impacts
}

func renderCompactionReport(samples []compactionSample) string {
	impacts := collectCompactionImpacts(samples)
	if len(impacts) == 0 {
		return ""
	}
	var report strings.Builder
	report.WriteString("\n## MVCC compaction impact\n\n")
	report.WriteString("The impact ratio compares paired baseline and treatment runs from fresh clusters. Ratios are averaged per round; lower impact is better. This is a performance measurement, not a correctness test.\n\n")
	report.WriteString("| backend | workload | baseline | paired rounds | throughput ratio | p99 ratio | throughput impact | p99 increase |\n|---|---|---|---:|---:|---:|---:|---:|\n")
	for _, impact := range impacts {
		fmt.Fprintf(&report, "| %s | %s | %s | %d | %.3f | %.3f | %.1f%% | %.1f%% |\n",
			impact.backend, impact.scenario, impact.baseline, impact.rounds,
			impact.throughputRatio, impact.p99Ratio, impact.throughputImpact*100, impact.p99Increase*100)
	}
	return report.String()
}

func sortStrings(values []string) {
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}
