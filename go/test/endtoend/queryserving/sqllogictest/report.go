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
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// The serialized report shapes (PerRun / FileReport / SuiteReport) and the
// pass/divergence counting live in suiteutil; this file holds the
// sqllogictest-specific parts: the intrinsic pass verdict (the runner exited 0),
// the human notes, and the per-protocol markdown/log rendering.

// newSuiteReport builds one suite report from paired per-file results.
// pgResults and mgResults must align on File path.
func newSuiteReport(name, corpusRoot string, pgResults, mgResults []*runResult, startedAt time.Time, timedOut bool) *suiteutil.SuiteReport {
	if len(pgResults) != len(mgResults) {
		panic(fmt.Sprintf("sqllogictest: mismatched result slices for %s: pg=%d mg=%d", name, len(pgResults), len(mgResults)))
	}

	files := make([]suiteutil.DiffFile, 0, len(pgResults))
	for i := range pgResults {
		pg := pgResults[i]
		mg := mgResults[i]

		fileName := pg.File
		if corpusRoot != "" {
			if rel, err := filepath.Rel(corpusRoot, pg.File); err == nil {
				fileName = rel
			}
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

		files = append(files, suiteutil.DiffFile{
			Name:     fileName,
			PGPassed: pg.Passed,
			MGPassed: mg.Passed,
			Postgres: toPerRun(pg),
			Gateway:  toPerRun(mg),
			Note:     note,
			Combined: pg.Duration + mg.Duration,
		})
	}

	return suiteutil.NewSuiteReport(suiteutil.SuiteMeta{
		Name:         name,
		CorpusDir:    corpusRoot,
		CorpusCommit: CorpusCommit,
		StartedAt:    startedAt,
		TimedOut:     timedOut,
	}, files)
}

func toPerRun(r *runResult) suiteutil.PerRun {
	out := suiteutil.PerRun{
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
func writeJSON(outputDir string, reports []*suiteutil.SuiteReport) (string, error) {
	return suiteutil.WriteJSON(outputDir, "results.json", reports)
}

// writeMarkdownSummary emits one section per suite (i.e. per protocol),
// with shields.io badges, counters, and the proxy-divergence list. The
// summary is mirrored to GITHUB_STEP_SUMMARY when set so the CI job page
// shows pass rates without downloading artifacts.
func writeMarkdownSummary(t *testing.T, outputDir string, reports []*suiteutil.SuiteReport) (string, error) {
	t.Helper()

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
			suiteutil.BadgeMarkdown(pgLabel, r.PGPassed, r.TotalFiles, 0, r.TimedOut),
			suiteutil.BadgeMarkdown(mgLabel, r.GatewayPassed, r.TotalFiles, 0, r.TimedOut),
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
					fmt.Fprintf(&sb, "| `%s` | %s |\n", tr.Name, suiteutil.FirstLine(tr.Gateway.Output))
				}
			}
			sb.WriteString("\n")
		}
	}

	summary := sb.String()
	path, err := suiteutil.WriteMarkdown(outputDir, "compatibility-report.md", summary)
	if err != nil {
		return summary, err
	}
	t.Logf("Markdown summary written to: %s", path)
	return summary, nil
}

// logSummary dumps a compact summary of one suite to the test log.
func logSummary(t *testing.T, r *suiteutil.SuiteReport) {
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
