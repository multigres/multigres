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

package pgproto

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// The serialized report shapes (PerRun / FileReport / SuiteReport) and the
// pass/divergence counting live in suiteutil; this file holds the pgproto-
// specific parts: how the differential verdict is computed (PostgreSQL is the
// oracle — the multigateway "passes" when its normalized trace matches the
// possibly-patched PG baseline), the human notes, and the markdown/log rendering.

// newSuiteReport builds the suite report from paired per-file results and their
// patch outcomes. pgResults, mgResults, and outcomes must align by index.
//
// PostgreSQL is the oracle: postgres.passed = the PG run produced a trace
// (pg.Ran); multigateway.passed = the MG run produced a trace that matches the
// (possibly patched) PG baseline. A file passing via a recorded patch is a
// known, accepted divergence — counted as a pass but flagged with PatchApplied.
// An unpatched residual divergence is a proxy bug (passed_pg_only). A file where
// PG itself could not produce a baseline (pg.Ran == false) is recorded but never
// counts as a proxy divergence — that is a harness/corpus problem.
func newSuiteReport(name, corpusRoot string, pgResults, mgResults []*runResult, outcomes []patchOutcome, startedAt time.Time, timedOut bool) *suiteutil.SuiteReport {
	if len(pgResults) != len(mgResults) || len(pgResults) != len(outcomes) {
		panic(fmt.Sprintf("pgproto: mismatched slices for %s: pg=%d mg=%d outcomes=%d", name, len(pgResults), len(mgResults), len(outcomes)))
	}

	files := make([]suiteutil.DiffFile, 0, len(pgResults))
	for i := range pgResults {
		pg := pgResults[i]
		mg := mgResults[i]
		oc := outcomes[i]

		fileName := pg.File
		if corpusRoot != "" {
			if rel, err := filepath.Rel(corpusRoot, pg.File); err == nil {
				fileName = rel
			}
		}

		pgPassed := pg.Ran
		// The multigateway "passes" when its trace matches the (possibly patched)
		// PostgreSQL baseline — see verifyTracePatch.
		mgPassed := mg.Ran && oc.Matched

		note := ""
		switch {
		case pg.ExecErr != nil:
			note = "postgres baseline error (not a proxy bug): " + pg.ExecErr.Error()
		case mg.ExecErr != nil:
			note = "multigateway runner error: " + mg.ExecErr.Error()
		case pg.TimedOut || mg.TimedOut:
			note = "timed out (per-file deadline)"
		case pgPassed && mgPassed && oc.PatchApplied:
			note = "known divergence absorbed by patch: " + oc.PatchPath
		case pgPassed && !mgPassed:
			note = "divergence: multigateway trace differs from postgres (not covered by a patch)"
		}

		pgRun := toPerRun(pg, pgPassed)
		mgRun := toPerRun(mg, mgPassed)
		// On an unpatched divergence, attach the residual diff (patched-postgres
		// vs multigateway) so the report shows exactly what is new/uncovered.
		if pgPassed && !mgPassed && oc.ResidualDiff != "" {
			mgRun.Output = suiteutil.TruncateOutput(oc.ResidualDiff, maxOutputBytes)
		}

		files = append(files, suiteutil.DiffFile{
			Name:         fileName,
			PGPassed:     pgPassed,
			MGPassed:     mgPassed,
			Postgres:     pgRun,
			Gateway:      mgRun,
			Note:         note,
			Combined:     pg.Duration + mg.Duration,
			PatchApplied: oc.PatchApplied,
			PatchPath:    oc.PatchPath,
		})
	}

	return suiteutil.NewSuiteReport(suiteutil.SuiteMeta{
		Name:         name,
		CorpusDir:    corpusRoot,
		CorpusCommit: PgprotoCommit,
		StartedAt:    startedAt,
		TimedOut:     timedOut,
	}, files)
}

// toPerRun converts a runResult into the serialized per-target record. The
// passed verdict is supplied by the caller because it is differential, not a
// property of the run in isolation.
func toPerRun(r *runResult, passed bool) suiteutil.PerRun {
	out := suiteutil.PerRun{
		Passed:   passed,
		TimedOut: r.TimedOut,
		Duration: r.Duration.Round(time.Millisecond).String(),
	}
	// Keep the raw trace only when this target did not pass — a passing run's
	// trace matches the baseline and adds nothing to the report.
	if !passed {
		out.Output = r.RawTrace
	}
	if r.ExecErr != nil {
		out.ExecErr = r.ExecErr.Error()
	}
	return out
}

// writeJSON writes the suite (wrapped in a single-element array, matching the
// sibling suites' shape) to <outputDir>/results.json.
func writeJSON(outputDir string, report *suiteutil.SuiteReport) (string, error) {
	return suiteutil.WriteJSON(outputDir, "results.json", []*suiteutil.SuiteReport{report})
}

// writeMarkdownSummary emits the suite summary with shields.io badges,
// counters, and the proxy-divergence list. Mirrored to GITHUB_STEP_SUMMARY when
// set so the CI job page shows pass rates without downloading artifacts.
func writeMarkdownSummary(t *testing.T, outputDir string, r *suiteutil.SuiteReport) (string, error) {
	t.Helper()

	var sb strings.Builder
	sb.WriteString("## pgproto Wire-Protocol Conformance\n\n")
	fmt.Fprintf(&sb, "**Tool:** [pgpool-II %s](%s) (src/tools/pgproto)\n",
		r.CorpusCommit, PgprotoRepoURL)
	fmt.Fprintf(&sb, "**Corpus:** `%s`\n", r.CorpusDir)
	fmt.Fprintf(&sb, "**Started:** %s UTC — **Duration:** %s\n\n",
		r.StartedAt.UTC().Format(time.RFC3339), r.Duration)

	// shields.io parses the static-badge path as label-message-color and renders
	// "_" as a space, so build the label with "_" instead of "-". This keeps the
	// label free of literal dashes (which add extra path segments that can break
	// rendering of the badge image in GitHub summaries).
	pgLabel := r.Name + "_postgres"
	mgLabel := r.Name + "_multigateway"
	fmt.Fprintf(&sb, "%s %s\n\n",
		suiteutil.BadgeMarkdown(pgLabel, r.PGPassed, r.TotalFiles, 0, r.TimedOut),
		suiteutil.BadgeMarkdown(mgLabel, r.GatewayPassed, r.TotalFiles, 0, r.TimedOut),
	)

	if r.TimedOut {
		fmt.Fprintf(&sb, "> **Timed out** — %d files executed before the deadline.\n\n", r.TotalFiles)
	}

	sb.WriteString("| Metric | Count |\n|---|---|\n")
	fmt.Fprintf(&sb, "| Files in run | %d |\n", r.TotalFiles)
	fmt.Fprintf(&sb, "| Matched both (multigateway == postgres) | %d |\n", r.PassedBoth)
	fmt.Fprintf(&sb, "| — of which known divergences absorbed by a patch | %d |\n", r.PassedViaPatch)
	fmt.Fprintf(&sb, "| New proxy divergence (not covered by a patch) | %d |\n", r.PassedPGOnly)
	fmt.Fprintf(&sb, "| Multigateway-only pass (postgres baseline missing) | %d |\n", r.PassedMGOnly)
	fmt.Fprintf(&sb, "| No baseline (postgres could not produce a trace) | %d |\n\n", r.FailedBoth)

	divergences := 0
	for _, tr := range r.Tests {
		if tr.Postgres.Passed && !tr.Gateway.Passed {
			divergences++
		}
	}
	if divergences > 0 {
		fmt.Fprintf(&sb, "### %d new proxy divergence(s) — not covered by a patch\n\n", divergences)
		for _, tr := range r.Tests {
			if tr.Postgres.Passed && !tr.Gateway.Passed {
				fmt.Fprintf(&sb, "#### `%s`\n\n", tr.Name)
				if tr.Note != "" {
					fmt.Fprintf(&sb, "%s\n\n", tr.Note)
				}
				if tr.Gateway.Output != "" {
					fmt.Fprintf(&sb, "```diff\n%s\n```\n\n", tr.Gateway.Output)
				}
			}
		}
	}

	if r.PassedViaPatch > 0 {
		fmt.Fprintf(&sb, "### %d known divergence(s) absorbed by patches\n\n", r.PassedViaPatch)
		for _, tr := range r.Tests {
			if tr.PatchApplied {
				fmt.Fprintf(&sb, "- `%s` — `%s`\n", tr.Name, tr.PatchPath)
			}
		}
		sb.WriteString("\n")
	}

	summary := sb.String()
	path, err := suiteutil.WriteMarkdown(outputDir, "compatibility-report.md", summary)
	if err != nil {
		return summary, err
	}
	t.Logf("Markdown summary written to: %s", path)
	return summary, nil
}

// logSummary dumps a compact summary of the suite to the test log.
func logSummary(t *testing.T, r *suiteutil.SuiteReport) {
	t.Helper()
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("%s summary (%d files, %s):", r.Name, r.TotalFiles, r.Duration)
	if r.TimedOut {
		t.Logf("  status:                       timed out")
	}
	t.Logf("  postgres baseline produced:   %d/%d", r.PGPassed, r.TotalFiles)
	t.Logf("  multigateway matched:         %d/%d", r.GatewayPassed, r.TotalFiles)
	t.Logf("  matched both:                 %d (of which %d via patch)", r.PassedBoth, r.PassedViaPatch)
	t.Logf("  new proxy divergences (PG ok, MG differs, unpatched): %d", r.PassedPGOnly)
	t.Logf("  unexpected MG-only passes:    %d", r.PassedMGOnly)
	t.Logf("  no baseline (PG failed):      %d", r.FailedBoth)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}
