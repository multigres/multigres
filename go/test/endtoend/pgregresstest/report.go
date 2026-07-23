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

package pgregresstest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// SuiteResult holds results for one test suite.
type SuiteResult struct {
	Name          string       // e.g. "Regression Tests", "Isolation Tests"
	Results       *TestResults // parsed TAP results
	ExpectedTests int          // total tests from schedule file (0 = unknown)
}

// githubBlobURLPrefix returns an absolute URL prefix of the form
// "https://github.com/<owner>/<repo>/blob/<sha>/" when running inside a
// GitHub Actions job, or an empty string otherwise. Repo-relative paths
// concatenated onto this prefix resolve to the blob view of that file at
// the exact commit the job is executing against.
func githubBlobURLPrefix() string {
	serverURL := os.Getenv("GITHUB_SERVER_URL")
	repo := os.Getenv("GITHUB_REPOSITORY")
	sha := os.Getenv("GITHUB_SHA")
	if serverURL == "" || repo == "" || sha == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/blob/%s/", serverURL, repo, sha)
}

// WriteMarkdownSummary generates a unified markdown report covering one or more
// test suites. It writes the report to pb.OutputDir/compatibility-report.md and
// appends it to GITHUB_STEP_SUMMARY when running in CI.
//
// Each suite gets its own badge showing pass rate and timeout status. Full diffs
// are available in the CI artifact (regression.diffs).
func (pb *PostgresBuilder) WriteMarkdownSummary(t *testing.T, suites []SuiteResult) (string, error) {
	t.Helper()

	var sb strings.Builder

	sb.WriteString("## PostgreSQL Compatibility Report\n\n")

	for _, s := range suites {
		label := strings.TrimSuffix(s.Name, " Tests")
		sb.WriteString(suiteutil.BadgeMarkdown(
			label,
			s.Results.PassedTests,
			s.Results.TotalTests,
			s.ExpectedTests,
			s.Results.TimedOut,
		))
		sb.WriteString(" ")
	}
	sb.WriteString("\n\n")

	fmt.Fprintf(&sb, "**PostgreSQL Version:** `%s`\n", PostgresVersion)
	fmt.Fprintf(&sb, "**Timestamp:** %s\n\n", time.Now().UTC().Format(time.RFC3339))

	// Build an absolute URL prefix for patch links when running in CI.
	// Without this, markdown like [patch](go/test/.../foo.patch) is resolved
	// relative to the step-summary page (/actions/runs/<id>/) and produces a
	// 404 URL like .../actions/runs/go/test/.../foo.patch. Falls back to a
	// relative link when the GitHub Actions env vars aren't set (local runs).
	patchURLPrefix := githubBlobURLPrefix()

	for _, s := range suites {
		// The contrib and external suites' per-test detail is rendered by the
		// richer Extension Coverage table below (it carries the same per-test
		// results plus catalog context), so skip the generic per-test section
		// here. Their badges above are still shown.
		if s.Name == "Contrib Extension Tests" || s.Name == "External Extension Tests" {
			continue
		}

		fmt.Fprintf(&sb, "### %s\n\n", s.Name)

		if s.Results.TimedOut {
			ran := s.Results.TotalTests
			if s.ExpectedTests > 0 {
				fmt.Fprintf(&sb, "> **Timed out** — %d of %d scheduled tests executed before the deadline.\n\n", ran, s.ExpectedTests)
			} else {
				fmt.Fprintf(&sb, "> **Timed out** — %d tests executed before the deadline.\n\n", ran)
			}
		}

		sb.WriteString("| # | Test | Status | Patch | Duration |\n")
		sb.WriteString("|---|------|--------|-------|----------|\n")

		for i, test := range s.Results.Tests {
			status := "✅ compatible"
			switch test.Status {
			case "fail":
				status = "❌ residual failure"
			case "skip":
				status = "⏭️ skip"
			default:
				if test.PatchApplied {
					status = "✅ accepted divergence"
				}
			}
			duration := test.Duration
			if duration == "" {
				duration = "-"
			}
			patchCell := "-"
			if test.PatchApplied {
				if test.PatchPath != "" {
					patchCell = fmt.Sprintf("📎 [patch](%s%s)", patchURLPrefix, test.PatchPath)
				} else {
					patchCell = "📎 applied"
				}
			}
			fmt.Fprintf(&sb, "| %d | %s | %s | %s | %s |\n", i+1, test.Name, status, patchCell, duration)
		}
		sb.WriteString("\n")
	}

	// Extension coverage map: the full catalog (covered / pending / unsupported
	// / external) merged with this run's per-test results from both the contrib
	// and external suites. Rendered whenever either ran so the report doubles as
	// the living coverage tracker (see extensions.go).
	var contribResults, externalResults *TestResults
	for _, s := range suites {
		switch s.Name {
		case "Contrib Extension Tests":
			contribResults = s.Results
		case "External Extension Tests":
			externalResults = s.Results
		}
	}
	if contribResults != nil || externalResults != nil {
		sb.WriteString(ExtensionCoverageMarkdown(contribResults, externalResults))
	}

	summary := sb.String()
	summaryPath, err := suiteutil.WriteMarkdown(pb.OutputDir, "compatibility-report.md", summary)
	if err != nil {
		return summary, err
	}
	t.Logf("Markdown summary written to: %s", summaryPath)
	return summary, nil
}

// jsonSuiteResult is the JSON-serializable representation of a single test suite's results.
type jsonSuiteResult struct {
	Name  string                 `json:"name"`
	Tests []IndividualTestResult `json:"tests"`
}

// WriteJSONResults serializes suite results to pb.OutputDir/results.json.
// This file is consumed by CI scripts that compare runs to detect regressions.
func (pb *PostgresBuilder) WriteJSONResults(t *testing.T, suites []SuiteResult) (string, error) {
	t.Helper()

	var out []jsonSuiteResult
	for _, s := range suites {
		out = append(out, jsonSuiteResult{
			Name:  s.Name,
			Tests: s.Results.Tests,
		})
	}

	resultsPath, err := suiteutil.WriteJSON(pb.OutputDir, "results.json", out)
	if err != nil {
		return "", err
	}
	t.Logf("JSON results written to: %s", resultsPath)
	return resultsPath, nil
}

// WriteBadgeEndpoints writes one shields.io endpoint JSON per suite plus a
// combined "overall.json" into pb.OutputDir/badges. CI publishes this directory
// to GitHub Pages so the README and blog badges render the live pass count
// (republishing the JSON updates the badge with no markdown edits).
//
// Filenames are the suite label slug: regression.json, isolation.json,
// contrib-extension.json, and overall.json. These names are part of the public
// badge URL, so keep them stable.
func (pb *PostgresBuilder) WriteBadgeEndpoints(t *testing.T, suites []SuiteResult) error {
	t.Helper()

	badgeDir := filepath.Join(pb.OutputDir, "badges")

	var totalPassed, totalTests, totalExpected int
	var anyTimedOut bool
	for _, s := range suites {
		label := strings.TrimSuffix(s.Name, " Tests")
		endpoint := suiteutil.NewBadgeEndpoint(label, s.Results.PassedTests, s.Results.TotalTests, s.ExpectedTests, s.Results.TimedOut)
		if _, err := suiteutil.WriteJSON(badgeDir, suiteutil.BadgeSlug(label)+".json", endpoint); err != nil {
			return err
		}
		totalPassed += s.Results.PassedTests
		totalTests += s.Results.TotalTests
		totalExpected += s.ExpectedTests
		anyTimedOut = anyTimedOut || s.Results.TimedOut
	}

	overall := suiteutil.NewBadgeEndpoint("Overall", totalPassed, totalTests, totalExpected, anyTimedOut)
	if _, err := suiteutil.WriteJSON(badgeDir, "overall.json", overall); err != nil {
		return err
	}

	t.Logf("Badge endpoints written to: %s", badgeDir)
	return nil
}
