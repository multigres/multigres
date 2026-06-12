// Copyright 2026 Supabase, Inc.
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

package benchmarking

import (
	"fmt"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// WriteJSONReport writes the benchmark results as JSON.
func WriteJSONReport(t *testing.T, outputDir string, report *BenchmarkReport) (string, error) {
	t.Helper()
	return suiteutil.WriteJSON(outputDir, "results.json", report)
}

// WriteMarkdownReport generates a markdown comparison report.
func WriteMarkdownReport(t *testing.T, outputDir string, report *BenchmarkReport) (string, error) {
	t.Helper()

	var sb strings.Builder

	sb.WriteString("# pgbench Benchmark Report\n\n")
	fmt.Fprintf(&sb, "**Timestamp:** %s\n", report.Timestamp)
	fmt.Fprintf(&sb, "**pgbench:** %s\n", report.Environment.PgBenchVersion)
	fmt.Fprintf(&sb, "**PostgreSQL:** %s\n", report.Environment.PostgresVersion)
	fmt.Fprintf(&sb, "**OS:** %s | **GOMAXPROCS:** %d\n\n", report.Environment.OS, report.Environment.GOMAXPROCS)

	// Group results by scenario type (sustained vs churn) and protocol.
	type scenarioGroup struct {
		title string
		churn bool
		proto string
	}
	groups := []scenarioGroup{
		{"Sustained Load — Extended Protocol", false, "extended"},
		{"Sustained Load — Simple Protocol", false, "simple"},
		{"Connection Churn — Extended Protocol", true, "extended"},
		{"Connection Churn — Simple Protocol", true, "simple"},
	}

	// Determine which targets are present.
	targetNames := uniqueTargets(report.Results)

	for _, group := range groups {
		rows := filterResults(report.Results, group.churn, group.proto)
		if len(rows) == 0 {
			continue
		}

		fmt.Fprintf(&sb, "## %s\n\n", group.title)

		// Layout: metrics on rows, client counts on columns. With ~3 client counts
		// and 3 targets × 3 metrics + overhead, this is much narrower than the
		// transposed view (especially when pgbouncer is present).
		clientCounts := uniqueClients(rows)

		// Header
		var header, divider strings.Builder
		header.WriteString("| Metric")
		divider.WriteString("|---")
		for _, c := range clientCounts {
			fmt.Fprintf(&header, " | %d clients", c)
			divider.WriteString("|---")
		}
		header.WriteString(" |")
		divider.WriteString("|")
		fmt.Fprintf(&sb, "%s\n%s\n", header.String(), divider.String())

		// Per-target rows: TPS, Avg, P99.
		formatters := []struct {
			label string
			value func(*ScenarioResult) string
		}{
			{"TPS", func(r *ScenarioResult) string { return fmt.Sprintf("%.0f", r.TPS) }},
			{"Avg (ms)", func(r *ScenarioResult) string { return fmt.Sprintf("%.2f", r.LatencyAvg) }},
			{"P99 (ms)", func(r *ScenarioResult) string { return fmt.Sprintf("%.2f", r.LatencyP99) }},
		}

		for _, tgt := range targetNames {
			for _, f := range formatters {
				fmt.Fprintf(&sb, "| %s %s", tgt, f.label)
				for _, c := range clientCounts {
					r := findResult(rows, tgt, c)
					if r == nil {
						sb.WriteString(" | -")
						continue
					}
					fmt.Fprintf(&sb, " | %s", f.value(r))
				}
				sb.WriteString(" |\n")
			}
		}

		// Overhead row: multigateway TPS vs postgres TPS, per client count.
		if len(targetNames) >= 2 {
			sb.WriteString("| Overhead vs postgres")
			for _, c := range clientCounts {
				pg := findResult(rows, "postgres", c)
				mgw := findResult(rows, "multigateway", c)
				if pg == nil || mgw == nil || pg.TPS == 0 {
					sb.WriteString(" | -")
					continue
				}
				overhead := (1 - mgw.TPS/pg.TPS) * 100
				fmt.Fprintf(&sb, " | %.1f%%", overhead)
			}
			sb.WriteString(" |\n")
		}
		sb.WriteString("\n")
	}

	return suiteutil.WriteMarkdown(outputDir, "benchmark-report.md", sb.String())
}

// WriteSysBenchMarkdownReport renders a sysbench-shaped markdown report
// (per-scenario rows with a ps_mode column) under <outputDir>/benchmark-report.md.
//
// Kept separate from WriteMarkdownReport because the sysbench harness has a
// different scenario shape (ps_mode, no churn/protocol axis) and conflating
// the two renderers obscures both.
func WriteSysBenchMarkdownReport(t *testing.T, outputDir string, report *BenchmarkReport) (string, error) {
	t.Helper()

	var sb strings.Builder
	sb.WriteString("# sysbench oltp_point_select — multigres\n\n")
	fmt.Fprintf(&sb, "**Timestamp:** %s\n", report.Timestamp)
	if report.Environment.SysBenchVersion != "" {
		fmt.Fprintf(&sb, "**sysbench:** %s\n", report.Environment.SysBenchVersion)
	}
	if report.Environment.PostgresVersion != "" {
		fmt.Fprintf(&sb, "**PostgreSQL:** %s\n", report.Environment.PostgresVersion)
	}
	if report.Environment.MultigresVersion != "" {
		fmt.Fprintf(&sb, "**multigres:** %s\n", report.Environment.MultigresVersion)
	}
	fmt.Fprintf(&sb, "**OS/Arch:** %s/%s | **GOMAXPROCS:** %d\n",
		report.Environment.OS, report.Environment.Arch, report.Environment.GOMAXPROCS)
	if report.Environment.PlanCacheBytes > 0 {
		fmt.Fprintf(&sb, "**multigateway plan cache:** %d bytes\n", report.Environment.PlanCacheBytes)
	}
	sb.WriteString("\n")

	// Render one section per ps_mode, with a row per (target, scenario).
	psModes := uniquePSModes(report.Results)
	for _, mode := range psModes {
		fmt.Fprintf(&sb, "## ps_mode = %s\n\n", mode)
		sb.WriteString("| target | clients | TPS | avg ms | p99 ms | txns |\n")
		sb.WriteString("|---|---|---|---|---|---|\n")
		for _, r := range report.Results {
			if r.PSMode != mode {
				continue
			}
			fmt.Fprintf(&sb, "| %s | %d | %.0f | %.2f | %.2f | %d |\n",
				r.Target, r.Clients, r.TPS, r.LatencyAvg, r.LatencyP99, r.Transactions)
		}
		sb.WriteString("\n")
	}

	return suiteutil.WriteMarkdown(outputDir, "benchmark-report.md", sb.String())
}

// uniquePSModes returns ps_mode values in the order they first appear.
func uniquePSModes(results []ScenarioResult) []string {
	seen := make(map[string]bool)
	var out []string
	for _, r := range results {
		if r.PSMode != "" && !seen[r.PSMode] {
			seen[r.PSMode] = true
			out = append(out, r.PSMode)
		}
	}
	return out
}

// uniqueTargets returns deduplicated target names in the order they first appear.
func uniqueTargets(results []ScenarioResult) []string {
	seen := make(map[string]bool)
	var names []string
	for _, r := range results {
		if !seen[r.Target] {
			seen[r.Target] = true
			names = append(names, r.Target)
		}
	}
	return names
}

// uniqueClients returns deduplicated client counts in sorted order from results matching the filter.
func uniqueClients(results []ScenarioResult) []int {
	seen := make(map[int]bool)
	var counts []int
	for _, r := range results {
		if !seen[r.Clients] {
			seen[r.Clients] = true
			counts = append(counts, r.Clients)
		}
	}
	// Results are already generated in order, so this preserves order.
	return counts
}

// filterResults returns results matching the given churn mode and protocol.
func filterResults(results []ScenarioResult, churn bool, protocol string) []ScenarioResult {
	var filtered []ScenarioResult
	for _, r := range results {
		if r.ConnChurn == churn && r.Protocol == protocol {
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// findResult locates a result for the given target and client count.
func findResult(results []ScenarioResult, target string, clients int) *ScenarioResult {
	for i := range results {
		if results[i].Target == target && results[i].Clients == clients {
			return &results[i]
		}
	}
	return nil
}
