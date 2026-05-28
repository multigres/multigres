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

// fileReport is the per-file record in the serialized report. Its field layout
// mirrors sqllogictest/pgregresstest for familiarity; pgproto itself gates on
// the Go test exit (it fails on unpatched divergences) rather than on a
// downstream baseline comparison.
type fileReport struct {
	Name     string `json:"name"`           // relative path, e.g. "simple_query.pgproto"
	Status   string `json:"status"`         // "pass" | "fail"
	Duration string `json:"duration"`       // combined duration across targets
	Postgres perRun `json:"postgres"`       // baseline (direct PG) — the oracle
	Gateway  perRun `json:"multigateway"`   // candidate (multigres)
	Note     string `json:"note,omitempty"` // human note (divergence, exec err, timeout)
	// PatchApplied is true when the file passes via a recorded known-divergence
	// patch (testdata/patches/<name>.patch) rather than an exact trace match.
	PatchApplied bool `json:"patch_applied,omitempty"`
	// PatchPath is the patch file path (for report links). Empty when no patch.
	PatchPath string `json:"patch_path,omitempty"`
}

// perRun is one target's outcome for a single file.
//
// For pgproto, "passed" is differential, not intrinsic: postgres.passed means
// the baseline trace was produced (pgproto ran cleanly against PG), and
// multigateway.passed means the multigateway ran cleanly AND its normalized
// trace matched PG's — exactly, or after a recorded known-divergence patch.
type perRun struct {
	Passed   bool   `json:"passed"`
	TimedOut bool   `json:"timed_out,omitempty"`
	Duration string `json:"duration"`
	Output   string `json:"output,omitempty"`
	ExecErr  string `json:"exec_err,omitempty"`
}

// suiteReport is what we serialize. Fields line up with sqllogictest /
// pgregresstest for familiarity. pgproto emits a single suite (each data file
// mixes simple and extended protocol however it likes), serialized as a
// one-element array to match the sibling suites' on-disk shape.
type suiteReport struct {
	Name           string       `json:"name"` // "PgProto"
	CorpusDir      string       `json:"corpus_dir"`
	CorpusCommit   string       `json:"corpus_commit"` // pinned pgproto tool commit
	TimedOut       bool         `json:"timed_out"`
	TotalFiles     int          `json:"total_files"`
	PassedBoth     int          `json:"passed_both"`
	PassedViaPatch int          `json:"passed_via_patch"` // subset of passed_both: known divergences absorbed by a patch
	PassedPGOnly   int          `json:"passed_pg_only"`   // proxy divergence (unpatched)
	PassedMGOnly   int          `json:"passed_mg_only"`
	FailedBoth     int          `json:"failed_both"`
	PGPassed       int          `json:"postgres_passed"`
	GatewayPassed  int          `json:"multigateway_passed"`
	StartedAt      time.Time    `json:"started_at"`
	Duration       string       `json:"duration"`
	Tests          []fileReport `json:"tests"`
}

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
func newSuiteReport(name, corpusRoot string, pgResults, mgResults []*runResult, outcomes []patchOutcome, startedAt time.Time, timedOut bool) *suiteReport {
	if len(pgResults) != len(mgResults) || len(pgResults) != len(outcomes) {
		panic(fmt.Sprintf("pgproto: mismatched slices for %s: pg=%d mg=%d outcomes=%d", name, len(pgResults), len(mgResults), len(outcomes)))
	}

	report := &suiteReport{
		Name:         name,
		CorpusDir:    corpusRoot,
		CorpusCommit: PgprotoCommit,
		TimedOut:     timedOut,
		TotalFiles:   len(pgResults),
		StartedAt:    startedAt,
		Duration:     time.Since(startedAt).Round(time.Second).String(),
	}

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

		if pgPassed {
			report.PGPassed++
		}
		if mgPassed {
			report.GatewayPassed++
		}

		status := "fail"
		switch {
		case pgPassed && mgPassed:
			report.PassedBoth++
			status = "pass"
			if oc.PatchApplied {
				report.PassedViaPatch++
			}
		case pgPassed && !mgPassed:
			report.PassedPGOnly++
		case !pgPassed && mgPassed:
			report.PassedMGOnly++
		default:
			report.FailedBoth++
		}

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
			mgRun.Output = truncateOutput(oc.ResidualDiff, maxOutputBytes)
		}

		report.Tests = append(report.Tests, fileReport{
			Name:         fileName,
			Status:       status,
			Duration:     (pg.Duration + mg.Duration).Round(time.Millisecond).String(),
			Postgres:     pgRun,
			Gateway:      mgRun,
			Note:         note,
			PatchApplied: oc.PatchApplied,
			PatchPath:    oc.PatchPath,
		})
	}

	return report
}

// toPerRun converts a runResult into the serialized per-target record. The
// passed verdict is supplied by the caller because it is differential, not a
// property of the run in isolation.
func toPerRun(r *runResult, passed bool) perRun {
	out := perRun{
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
func writeJSON(outputDir string, report *suiteReport) (string, error) {
	return suiteutil.WriteJSON(outputDir, "results.json", []*suiteReport{report})
}

// writeMarkdownSummary emits the suite summary with shields.io badges,
// counters, and the proxy-divergence list. Mirrored to GITHUB_STEP_SUMMARY when
// set so the CI job page shows pass rates without downloading artifacts.
func writeMarkdownSummary(t *testing.T, outputDir string, r *suiteReport) (string, error) {
	t.Helper()

	var sb strings.Builder
	sb.WriteString("## pgproto Wire-Protocol Conformance\n\n")
	fmt.Fprintf(&sb, "**Tool:** [tatsuo-ishii/pgproto@%.12s](%s/tree/%s)\n",
		r.CorpusCommit, PgprotoRepoURL, r.CorpusCommit)
	fmt.Fprintf(&sb, "**Corpus:** `%s`\n", r.CorpusDir)
	fmt.Fprintf(&sb, "**Started:** %s UTC — **Duration:** %s\n\n",
		r.StartedAt.UTC().Format(time.RFC3339), r.Duration)

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
func (r *suiteReport) logSummary(t *testing.T) {
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
