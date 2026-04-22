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

package sqllogictest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fileReport is the per-file record in the serialized report. It captures
// the file path, how each target fared, and basic counters callers can
// aggregate. The field layout intentionally mirrors pgregresstest's
// IndividualTestResult so the existing .github/scripts/detect-regressions.sh
// can consume our output with minimal special-casing.
type fileReport struct {
	Name     string `json:"name"`           // relative path, e.g. "test/select1.test"
	Status   string `json:"status"`         // "pass" | "fail"
	Duration string `json:"duration"`       // combined duration across targets
	Postgres perRun `json:"postgres"`       // baseline (direct PG)
	Gateway  perRun `json:"multigateway"`   // candidate (multigres)
	Note     string `json:"note,omitempty"` // human note (timeout, exec err, divergence)
}

// perRun is one target's outcome for a single file.
type perRun struct {
	Passed   bool   `json:"passed"`
	TimedOut bool   `json:"timed_out,omitempty"`
	Duration string `json:"duration"`
	Output   string `json:"output,omitempty"`
	ExecErr  string `json:"exec_err,omitempty"`
}

// suiteReport is what we serialize. Fields line up with pgregresstest so
// downstream tooling can treat the two identically. One suiteReport is
// produced per wire protocol (simple / extended) so regression tracking
// in detect-regressions.sh is scoped per protocol.
type suiteReport struct {
	Name          string       `json:"name"` // "SQLLogicTest-simple" / "SQLLogicTest-extended"
	CorpusDir     string       `json:"corpus_dir"`
	CorpusCommit  string       `json:"corpus_commit"`
	TimedOut      bool         `json:"timed_out"`
	TotalFiles    int          `json:"total_files"`
	PassedBoth    int          `json:"passed_both"`
	PassedPGOnly  int          `json:"passed_pg_only"`
	PassedMGOnly  int          `json:"passed_mg_only"`
	FailedBoth    int          `json:"failed_both"`
	PGPassed      int          `json:"postgres_passed"`
	GatewayPassed int          `json:"multigateway_passed"`
	StartedAt     time.Time    `json:"started_at"`
	Duration      string       `json:"duration"`
	Tests         []fileReport `json:"tests"`
}

// newSuiteReport builds one suite report from paired per-file results.
// pgResults and mgResults must align on File path.
func newSuiteReport(name, corpusRoot string, pgResults, mgResults []*runResult, startedAt time.Time, timedOut bool) *suiteReport {
	if len(pgResults) != len(mgResults) {
		panic(fmt.Sprintf("sqllogictest: mismatched result slices for %s: pg=%d mg=%d", name, len(pgResults), len(mgResults)))
	}

	report := &suiteReport{
		Name:         name,
		CorpusDir:    corpusRoot,
		CorpusCommit: CorpusCommit,
		TimedOut:     timedOut,
		TotalFiles:   len(pgResults),
		StartedAt:    startedAt,
		Duration:     time.Since(startedAt).Round(time.Second).String(),
	}

	for i := range pgResults {
		pg := pgResults[i]
		mg := mgResults[i]

		fileName := pg.File
		if corpusRoot != "" {
			if rel, err := filepath.Rel(corpusRoot, pg.File); err == nil {
				fileName = rel
			}
		}

		if pg.Passed {
			report.PGPassed++
		}
		if mg.Passed {
			report.GatewayPassed++
		}

		status := "fail"
		switch {
		case pg.Passed && mg.Passed:
			report.PassedBoth++
			status = "pass"
		case pg.Passed && !mg.Passed:
			report.PassedPGOnly++
		case !pg.Passed && mg.Passed:
			report.PassedMGOnly++
		default:
			report.FailedBoth++
		}

		note := ""
		switch {
		case pg.ExecErr != nil:
			note = "postgres runner exec error: " + pg.ExecErr.Error()
		case mg.ExecErr != nil:
			note = "multigateway runner exec error: " + mg.ExecErr.Error()
		case pg.TimedOut || mg.TimedOut:
			note = "timed out (per-file deadline)"
		case pg.Passed && !mg.Passed:
			note = "divergence: passes on postgres, fails on multigateway"
		case !pg.Passed && mg.Passed:
			note = "divergence: fails on postgres, passes on multigateway (likely corpus bug)"
		}

		combined := pg.Duration + mg.Duration

		report.Tests = append(report.Tests, fileReport{
			Name:     fileName,
			Status:   status,
			Duration: combined.Round(time.Millisecond).String(),
			Postgres: toPerRun(pg),
			Gateway:  toPerRun(mg),
			Note:     note,
		})
	}

	return report
}

func toPerRun(r *runResult) perRun {
	out := perRun{
		Passed:   r.Passed,
		TimedOut: r.TimedOut,
		Duration: r.Duration.Round(time.Millisecond).String(),
	}
	// Only keep captured output for failing runs — passing files produce a
	// one-line success banner we don't need in results.json.
	if !r.Passed {
		out.Output = r.Output
	}
	if r.ExecErr != nil {
		out.ExecErr = r.ExecErr.Error()
	}
	return out
}

// writeJSON writes the merged suite list to <outputDir>/results.json as a
// pretty-printed array of suite objects. The array shape is exactly what
// .github/scripts/detect-regressions.sh expects, so regression tracking
// is cross-run-portable per protocol.
func writeJSON(outputDir string, reports []*suiteReport) (string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", outputDir, err)
	}
	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal reports: %w", err)
	}
	path := filepath.Join(outputDir, "results.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}

// writeMarkdownSummary emits one section per suite (i.e. per protocol),
// with shields.io badges, counters, and the proxy-divergence list. The
// summary is mirrored to GITHUB_STEP_SUMMARY when set so the CI job page
// shows pass rates without downloading artifacts.
func writeMarkdownSummary(t *testing.T, outputDir string, reports []*suiteReport) (string, error) {
	t.Helper()
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", outputDir, err)
	}

	var sb strings.Builder
	sb.WriteString("## SQLLogicTest Pass Rate\n\n")

	if len(reports) > 0 {
		r0 := reports[0]
		if r0.CorpusCommit != "" {
			fmt.Fprintf(&sb, "**Corpus:** [gregrahn/sqllogictest@%.12s](%s/tree/%s) (MIT, per upstream COPYRIGHT.md)\n",
				r0.CorpusCommit, CorpusRepoURL, r0.CorpusCommit)
		} else {
			fmt.Fprintf(&sb, "**Corpus:** `%s`\n", r0.CorpusDir)
		}
		fmt.Fprintf(&sb, "**Started:** %s UTC — **Duration:** %s\n\n",
			r0.StartedAt.UTC().Format(time.RFC3339), r0.Duration)
	}

	for _, r := range reports {
		fmt.Fprintf(&sb, "### %s\n\n", r.Name)

		pgLabel := r.Name + "-postgres"
		mgLabel := r.Name + "-multigateway"
		fmt.Fprintf(&sb, "%s %s\n\n",
			badgeMarkdown(pgLabel, r.PGPassed, r.TotalFiles, r.TimedOut),
			badgeMarkdown(mgLabel, r.GatewayPassed, r.TotalFiles, r.TimedOut),
		)

		if r.TimedOut {
			fmt.Fprintf(&sb, "> **Timed out** — %d files executed before the deadline.\n\n", r.TotalFiles)
		}

		sb.WriteString("| Metric | Count |\n|---|---|\n")
		fmt.Fprintf(&sb, "| Files in run | %d |\n", r.TotalFiles)
		fmt.Fprintf(&sb, "| Passed on both | %d |\n", r.PassedBoth)
		fmt.Fprintf(&sb, "| Passed on postgres only (proxy divergence) | %d |\n", r.PassedPGOnly)
		fmt.Fprintf(&sb, "| Passed on multigateway only | %d |\n", r.PassedMGOnly)
		fmt.Fprintf(&sb, "| Failed on both (baseline corpus failure) | %d |\n\n", r.FailedBoth)

		divergences := 0
		for _, tr := range r.Tests {
			if tr.Postgres.Passed && !tr.Gateway.Passed {
				divergences++
			}
		}
		if divergences > 0 {
			fmt.Fprintf(&sb, "#### %d proxy divergence(s)\n\n", divergences)
			sb.WriteString("| File | multigateway note |\n|---|---|\n")
			for _, tr := range r.Tests {
				if tr.Postgres.Passed && !tr.Gateway.Passed {
					fmt.Fprintf(&sb, "| `%s` | %s |\n", tr.Name, firstLine(tr.Gateway.Output))
				}
			}
			sb.WriteString("\n")
		}
	}

	summary := sb.String()
	path := filepath.Join(outputDir, "compatibility-report.md")
	if err := os.WriteFile(path, []byte(summary), 0o644); err != nil {
		return summary, fmt.Errorf("write %s: %w", path, err)
	}
	t.Logf("Markdown summary written to: %s", path)

	if f := os.Getenv("GITHUB_STEP_SUMMARY"); f != "" {
		if fh, err := os.OpenFile(f, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			_, _ = fh.WriteString(summary)
			_ = fh.Close()
			t.Logf("Appended summary to GITHUB_STEP_SUMMARY (%d bytes)", len(summary))
		}
	}

	return summary, nil
}

// badgeMarkdown renders a shields.io badge URL. Colour goes green at 100%,
// yellow from 80%+, orange from 50%+, red below. Timed-out runs get
// downgraded one level to visually flag "not representative".
func badgeMarkdown(label string, passed, total int, timedOut bool) string {
	colour := "lightgrey"
	if total > 0 {
		pct := passed * 100 / total
		switch {
		case pct == 100:
			colour = "brightgreen"
		case pct >= 80:
			colour = "yellow"
		case pct >= 50:
			colour = "orange"
		default:
			colour = "red"
		}
	}
	value := fmt.Sprintf("%d%%2F%d_passed", passed, total)
	if timedOut {
		value += "_(timed_out)"
		if colour == "brightgreen" {
			colour = "yellow"
		}
	}
	return fmt.Sprintf("![%s](https://img.shields.io/badge/%s-%s-%s)", label, label, value, colour)
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	first, _, _ := strings.Cut(s, "\n")
	return strings.TrimSpace(first)
}

// logSummary dumps a compact summary of one suite to the test log.
func (r *suiteReport) logSummary(t *testing.T) {
	t.Helper()
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("%s summary (%d files, %s):", r.Name, r.TotalFiles, r.Duration)
	if r.TimedOut {
		t.Logf("  status:                       timed out")
	}
	t.Logf("  postgres passed:              %d/%d", r.PGPassed, r.TotalFiles)
	t.Logf("  multigateway passed:          %d/%d", r.GatewayPassed, r.TotalFiles)
	t.Logf("  both passed:                  %d", r.PassedBoth)
	t.Logf("  proxy divergences (PG pass, MG fail): %d", r.PassedPGOnly)
	t.Logf("  unexpected MG-only passes:    %d", r.PassedMGOnly)
	t.Logf("  baseline failures (both fail): %d", r.FailedBoth)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}
