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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// WriteJSONReport writes the benchmark results as JSON.
func WriteJSONReport(t *testing.T, outputDir string, report *BenchmarkReport) (string, error) {
	t.Helper()

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal report: %w", err)
	}

	path := filepath.Join(outputDir, "results.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("failed to write results.json: %w", err)
	}
	return path, nil
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

		// Table header
		var header, divider strings.Builder
		header.WriteString("| Clients")
		divider.WriteString("|---")
		for _, tgt := range targetNames {
			fmt.Fprintf(&header, " | %s TPS", tgt)
			divider.WriteString("|---")
			fmt.Fprintf(&header, " | %s Avg Latency", tgt)
			divider.WriteString("|---")
		}
		if len(targetNames) >= 2 {
			header.WriteString(" | Overhead")
			divider.WriteString("|---")
		}
		header.WriteString(" |")
		divider.WriteString("|")

		fmt.Fprintf(&sb, "%s\n%s\n", header.String(), divider.String())

		// Collect unique client counts for this group.
		clientCounts := uniqueClients(rows)

		for _, clients := range clientCounts {
			var row strings.Builder
			fmt.Fprintf(&row, "| %d", clients)

			var pgTPS float64
			var mgwTPS float64

			for _, tgt := range targetNames {
				r := findResult(rows, tgt, clients)
				if r != nil {
					fmt.Fprintf(&row, " | %.0f", r.TPS)
					fmt.Fprintf(&row, " | %.2f ms", r.LatencyAvg)
					if tgt == "postgres" {
						pgTPS = r.TPS
					}
					if tgt == "multigateway" {
						mgwTPS = r.TPS
					}
				} else {
					row.WriteString(" | - | -")
				}
			}

			if len(targetNames) >= 2 && pgTPS > 0 {
				overhead := (1 - mgwTPS/pgTPS) * 100
				fmt.Fprintf(&row, " | %.1f%%", overhead)
			} else if len(targetNames) >= 2 {
				row.WriteString(" | -")
			}
			row.WriteString(" |")

			fmt.Fprintf(&sb, "%s\n", row.String())
		}
		sb.WriteString("\n")
	}

	content := sb.String()
	path := filepath.Join(outputDir, "benchmark-report.md")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", fmt.Errorf("failed to write benchmark-report.md: %w", err)
	}

	// Write to GITHUB_STEP_SUMMARY if running in CI.
	if summaryPath := os.Getenv("GITHUB_STEP_SUMMARY"); summaryPath != "" {
		f, err := os.OpenFile(summaryPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.WriteString(content)
			f.Close()
		}
	}

	return path, nil
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
