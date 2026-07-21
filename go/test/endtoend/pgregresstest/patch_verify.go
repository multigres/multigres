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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
)

// Patch-based verification compares pg_regress's actual .out against the
// checked-in expected .out, absorbing known, accepted divergences recorded as
// per-test patches. The shared verify/generate mechanics (apply patch, diff,
// regenerate) live in suiteutil; this file holds the pgregress-specific parts:
// reading the .out files, the whitespace normalization those files need, and
// mapping the shared outcome into a VerifyOutcome.

// PatchMode is an alias for the shared mode so existing pgregress call sites
// (e.g. `mode == PatchModeGenerate`) keep working unchanged.
type PatchMode = suiteutil.PatchMode

const (
	PatchModeVerify   = suiteutil.PatchModeVerify
	PatchModeGenerate = suiteutil.PatchModeGenerate
)

// PatchModeEnv is the environment variable that selects the mode. When unset
// or empty, verify mode is used.
const PatchModeEnv = "PGREGRESS_PATCH_MODE"

// GetPatchMode reads the mode from the environment. Defaults to verify.
func GetPatchMode() PatchMode {
	return suiteutil.PatchModeFromEnv(PatchModeEnv)
}

// VerifyOutcome is the result of applying patch-based verification to one test.
type VerifyOutcome struct {
	Name         string `json:"name"`
	Status       string `json:"status"` // "pass" | "fail"
	PatchApplied bool   `json:"patch_applied"`
	// PatchPath is the path to the patch file relative to the repo root, for
	// links in status reports. Empty if no patch was used.
	PatchPath string `json:"patch_path,omitempty"`
	// Reason is a short human-readable description when Status == "fail".
	Reason string `json:"reason,omitempty"`
	// Diff is the residual unified diff on failure. Populated only on fail.
	Diff string `json:"diff,omitempty"`
}

// VerifyInput describes one test's inputs to VerifyTest. All paths are absolute.
type VerifyInput struct {
	// Name is the bare test name (e.g. "boolean").
	Name string
	// ExpectedPath points at postgres's checked-in expected output
	// (e.g. /tmp/multigres_pg_cache/source/postgres/src/test/regress/expected/boolean.out).
	ExpectedPath string
	// ActualPath points at the output produced by this test run
	// (e.g. <buildDir>/src/test/regress/results/boolean.out).
	ActualPath string
	// PatchDir is the directory containing per-test patch files
	// (e.g. <repo>/go/test/endtoend/pgregresstest/testdata/pg17/patches).
	PatchDir string
	// RepoRoot is used only to compute a nice relative PatchPath for reports.
	RepoRoot string
}

// VerifyTest runs the patch-based verification pipeline for one test. It reads
// and whitespace-normalizes the expected and actual .out files, then delegates
// the apply/diff/regenerate mechanics to suiteutil.VerifyPatch and maps the
// shared outcome into a VerifyOutcome.
//
// In verify mode a failed patch apply (likely stale after an upstream change)
// and a residual diff are both reported as Status == "fail", with the residual
// diff attached in the latter case. In generate mode any residual diff is
// absorbed by (re)writing the patch and stale patches are removed, so the test
// ends up passing.
func VerifyTest(ctx context.Context, in VerifyInput, mode PatchMode) (*VerifyOutcome, error) {
	out := &VerifyOutcome{Name: in.Name}

	rawExpected, err := os.ReadFile(in.ExpectedPath)
	if err != nil {
		return nil, fmt.Errorf("read expected %q: %w", in.ExpectedPath, err)
	}
	rawActual, err := os.ReadFile(in.ActualPath)
	if err != nil {
		return nil, fmt.Errorf("read actual %q: %w", in.ActualPath, err)
	}
	// Normalize dynamic output so diff/patch operate on canonical,
	// run-independent bytes. See the normalize* helpers for the rationale.
	patchPath := filepath.Join(in.PatchDir, in.Name+".patch")
	expected := normalizeTestOutput(in.Name, in.PatchDir, normalizeRunPaths(normalizeWhitespace(normalizeNotificationPIDs(rawExpected))))
	actual := normalizeTestOutput(in.Name, in.PatchDir, normalizeRunPaths(normalizeWhitespace(normalizeNotificationPIDs(rawActual))))
	res, err := suiteutil.VerifyPatch(ctx, suiteutil.PatchInput{
		Expected:  expected,
		Actual:    actual,
		PatchPath: patchPath,
	}, mode)
	if err != nil {
		return nil, fmt.Errorf("verify %s: %w", in.Name, err)
	}

	out.PatchApplied = res.PatchApplied
	if res.PatchApplied {
		out.PatchPath = relForReport(in.RepoRoot, patchPath)
	}

	switch {
	case res.ApplyErr != "":
		out.Status = "fail"
		out.Reason = res.ApplyErr
	case res.Matched:
		out.Status = "pass"
	default:
		out.Status = "fail"
		out.Reason = "actual output does not match patched expected"
		out.Diff = res.ResidualDiff
	}
	return out, nil
}

var (
	isolationNotifyPIDRe = regexp.MustCompile(`(: NOTIFY "[^"\n]+" with payload "[^"\n]*" from )PID [0-9]+`)
	psqlNotifyPIDRe      = regexp.MustCompile(`from server process with PID [0-9]+`)
	// runBuildDirRe matches the per-run timestamped build directory that
	// pg_regress substitutes into test scripts via @abs_builddir@ / @abs_srcdir@
	// and that then surfaces in client-side output — e.g. psql's `could not open
	// file "/tmp/multigres_pg_cache/builds/<ts>/build/..."` in largeobject when a
	// preceding lo_export was rejected. The timestamp changes every run, so
	// without masking no committed patch containing such a line could ever
	// verify.
	runBuildDirRe = regexp.MustCompile(`builds/\d{8}-\d{6}\.\d+`)
)

func normalizeTestOutput(name, patchDir string, input []byte) []byte {
	if name == "stats" && filepath.Base(patchDir) == "isolation" {
		return normalizeIsolationStats(input)
	}
	return input
}

// normalizeIsolationStats masks only counters whose exact value depends on
// which pooled backend executes pg_stat_force_next_flush(). Stable booleans,
// write counters, tuple counts, and all other output remain patch-verified.
func normalizeIsolationStats(input []byte) []byte {
	lines := strings.Split(string(input), "\n")
	relationStatsRow := false
	for i, line := range lines {
		fields := strings.Split(line, "|")
		if len(fields) == 4 && strings.HasPrefix(strings.TrimSpace(fields[0]), "test_stat_func") {
			for j := range fields {
				fields[j] = strings.TrimSpace(fields[j])
			}
			fields[1] = "<calls>"
			lines[i] = strings.Join(fields, "|")
			continue
		}
		if line == "seq_scan|seq_tup_read|n_tup_ins|n_tup_upd|n_tup_del|n_live_tup|n_dead_tup|vacuum_count" {
			relationStatsRow = true
			continue
		}
		if relationStatsRow && len(fields) == 8 {
			for j := range fields {
				fields[j] = strings.TrimSpace(fields[j])
			}
			fields[0] = "<seq_scan>"
			fields[1] = "<seq_tup_read>"
			lines[i] = strings.Join(fields, "|")
			relationStatsRow = false
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

// normalizeNotificationPIDs canonicalises PostgreSQL backend PIDs in NOTIFY
// output. Multigres preserves LISTEN/NOTIFY delivery but reports the physical
// PostgreSQL backend PID, not the gateway virtual PID, so raw values vary by run.
func normalizeNotificationPIDs(input []byte) []byte {
	s := isolationNotifyPIDRe.ReplaceAllString(string(input), `${1}PostgreSQL backend PID`)
	return []byte(psqlNotifyPIDRe.ReplaceAllString(s, "from PostgreSQL backend PID"))
}

// normalizeRunPaths masks per-run path segments so diff/patch operate on
// run-independent bytes.
func normalizeRunPaths(input []byte) []byte {
	return runBuildDirRe.ReplaceAll(input, []byte("builds/[RUN]"))
}

// normalizeWhitespace canonicalises whitespace so byte-level comparison is
// stable across platforms. Each line has runs of `[ \t]+` collapsed to a
// single space and leading/trailing whitespace stripped. Newlines are
// preserved.
//
// Why: BSD diff (macOS) and GNU diff (Linux) drift on `-b` for some
// whitespace-only changes. Normalising here lets us drop `-b` and invoke
// plain `diff -U3` against bytes with no whitespace ambiguity left.
func normalizeWhitespace(input []byte) []byte {
	if len(input) == 0 {
		return input
	}
	// Track whether the input ended with a newline so we can preserve that
	// (or lack thereof) on output.
	hasTrailingNewline := input[len(input)-1] == '\n'

	lines := bytes.Split(input, []byte("\n"))
	if hasTrailingNewline {
		// bytes.Split leaves a trailing empty element when the input ends
		// with the separator; drop it so we don't synthesize an extra blank
		// line, then re-add the newline at the very end.
		lines = lines[:len(lines)-1]
	}

	for i, line := range lines {
		var b []byte
		prevWS := false
		for _, c := range line {
			if c == ' ' || c == '\t' {
				if !prevWS {
					b = append(b, ' ')
					prevWS = true
				}
			} else {
				b = append(b, c)
				prevWS = false
			}
		}
		// Strip leading and trailing single-space runs (already collapsed).
		b = bytes.TrimSpace(b)
		lines[i] = b
	}

	out := bytes.Join(lines, []byte("\n"))
	if hasTrailingNewline {
		out = append(out, '\n')
	}
	return out
}

// relForReport returns path relative to repoRoot when possible, otherwise the
// absolute path unchanged. Used purely to prettify PatchPath in JSON output.
func relForReport(repoRoot, path string) string {
	if repoRoot == "" {
		return path
	}
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return path
	}
	return rel
}
