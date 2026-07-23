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
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/tools/executil"
)

// runExternalPgTAP runs one external extension's pgTAP suite against multigateway.
// Unlike the pg_regress path there are no expected-output files: each test .sql is
// fed to psql and the TAP stream its pgTAP assertions emit server-side is parsed
// for ok/not ok. A file passes when psql exits cleanly, no assertion reports
// "not ok", and the number of assertions run matches the declared plan ("1..N").
//
// Each file is one psql process running the file's single BEGIN…ROLLBACK
// transaction. The dependency extensions (PreCreateExtensions) are created up
// front because the test files assume they already exist and never CREATE
// EXTENSION themselves.
//
// # Why only transaction-wrapped tests are runnable
//
// pgTAP keeps its plan and results in SESSION-temp tables (__tcache__,
// __tresults__) that plan() creates with `EXECUTE 'CREATE TEMP TABLE …'` *inside
// the plan() function body*. The gateway pins ("reserves") a backend for session
// affinity by parsing the CLIENT statement — it reserves on a visible BEGIN, or a
// visible top-level CREATE TEMP TABLE (planner.go classifies CreateStmt with
// RELPERSISTENCE_TEMP). It never sees DDL executed inside a function, so it does
// not know plan() created a temp table.
//
// That distinction decides everything:
//
//   - Inside BEGIN…ROLLBACK, the visible BEGIN pins the whole file to one backend,
//     so __tcache__ is consistent for the file, and ROLLBACK discards it. Clean.
//   - In AUTOCOMMIT (pgTAP suites whose procedures COMMIT, so they can't be
//     wrapped), each statement is its own transaction on an unpinned, pooled
//     backend. plan()'s temp table is created on whatever backend served that
//     statement and is NOT discarded on release (only *reserved* temp tables get
//     DISCARD TEMP — and this one was never reserved, being invisible). It leaks
//     onto the pooled backend and the next client that lands there sees a stale
//     plan row → pgTAP raises "You tried to plan twice!".
//
// This is a fundamental limit of pgTAP-via-pooler, not a bug we can fix in the
// harness: you cannot know what a procedure does to a session without executing
// it, so the pooler can neither pin nor discard. It was confirmed empirically —
// running the autocommit suites through the gateway produced many "plan twice"
// failures that all vanished when the same files were run directly against
// PostgreSQL (no pooler). So we run only the suites that wrap each test in
// BEGIN…ROLLBACK (see ExternalExtension.TestGlobs); the rest also need
// infrastructure the pooled path can't provide (background workers, tablespaces,
// non-superuser roles, manual multi-stage commits).
//
// Returns nil when the checkout has no matching test files.
func (pb *PostgresBuilder) runExternalPgTAP(t *testing.T, ctx context.Context, ext ExternalExtension, testDir, pgBinDir string, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	// Run the committed TestGlobs (the union of patterns) minus ExcludeGlobs — for
	// pg_partman the self-contained transaction-wrapped set.
	matches, err := selectPgTAPFiles(testDir, ext.TestGlobs, ext.ExcludeGlobs)
	if err != nil {
		return nil, fmt.Errorf("external/%s: %w", ext.Name, err)
	}
	if len(matches) == 0 {
		t.Logf("external/%s: no test files found, skipping", ext.Name)
		return nil, nil
	}

	// pgTAP suites assume their dependencies already exist; create them in order
	// (pgtap before pg_partman), each in its target schema, through the gateway.
	// Best-effort — a failure here surfaces as the per-file assertions failing,
	// which is more informative than aborting.
	for _, e := range ext.PreCreateExtensions {
		if err := createExtensionWithSchema(multigatewayPort, password, e.Name, e.Schema); err != nil {
			t.Logf("external/%s: warning: CREATE EXTENSION %s failed: %v", ext.Name, e.Name, err)
		}
	}

	outputDir := filepath.Join(pb.OutputDir, "external", ext.Name)
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("external/%s: create output dir: %w", ext.Name, err)
	}

	psqlBin := filepath.Join(pgBinDir, "psql")
	res := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}

	t.Logf("Running external/%s pgTAP suite (%d files) against multigateway...", ext.Name, len(matches))
	for _, f := range matches {
		// Name by path relative to the test dir so subfolder files (e.g. under
		// test_pg17plus/) stay unique and self-describing; for the top-level set
		// this is just the bare file stem.
		rel, relErr := filepath.Rel(testDir, f)
		if relErr != nil {
			rel = filepath.Base(f)
		}
		name := strings.TrimSuffix(rel, ".sql")

		// Drop the throwaway schemas a pg_partman test file creates, on the primary,
		// before each file. The rolled-back tests never leave these behind, so this
		// is normally a no-op; it keeps the run order-independent and re-runnable
		// after a crash that left a half-applied test's schema committed.
		cleanupPgTAPScratch(directPgPort, password)

		// Drive psql the way pg_prove does: unaligned + tuples-only so each pgTAP
		// row is a bare TAP line, no psqlrc, and stop on a genuine SQL error
		// (assertion "not ok" rows are result data, not errors, so they don't trip
		// ON_ERROR_STOP — only real failures abort the file).
		args := []string{
			"--no-psqlrc",
			"--no-align",
			"--quiet",
			"--pset", "pager=off",
			"--tuples-only",
			"--set", "ON_ERROR_STOP=1",
			"--file", f,
		}
		cmd := executil.Command(ctx, psqlBin, args...).WithProcessGroup().SetDir(testDir)
		cmd.AddEnv(
			"PGHOST=localhost",
			fmt.Sprintf("PGPORT=%d", multigatewayPort),
			"PGUSER=postgres",
			"PGPASSWORD="+password,
			"PGDATABASE=postgres",
			"PGCONNECT_TIMEOUT=10",
		)
		cmd.SetWaitDelay(10 * time.Second)

		// Assertion rows and pgTAP diagnostics arrive on stdout; NOTICE/error text
		// goes to stderr. Parse stdout only; tee both to the terminal for visibility.
		var tap bytes.Buffer
		cmd.Stdout = io.MultiWriter(os.Stdout, &tap)
		cmd.Stderr = os.Stderr

		start := time.Now()
		runErr := cmd.Run()
		dur := time.Since(start)

		// Persist the raw TAP for debugging (the pgTAP analog of regression.out).
		// Flatten any subfolder separators in the name so the file lands directly
		// under outputDir (a subfolder name like "test_pg17plus/foo" would otherwise
		// need its parent dir created first).
		tapFile := strings.ReplaceAll(name, "/", "__") + ".tap"
		if werr := os.WriteFile(filepath.Join(outputDir, tapFile), tap.Bytes(), 0o644); werr != nil {
			t.Logf("external/%s/%s: warning: could not save .tap output: %v", ext.Name, name, werr)
		}

		planned, passed, failed := parsePgTAPOutput(tap.String())

		status, reason := "pass", ""
		switch {
		case ctx.Err() == context.DeadlineExceeded:
			status, reason = "fail", "suite timed out"
		case len(failed) > 0:
			status, reason = "fail", summarizePgTAPFailures(failed)
		case planned >= 0 && planned != passed:
			status, reason = "fail", fmt.Sprintf("plan mismatch: declared %d, ran %d", planned, passed)
		case planned < 0:
			// No plan line means the file errored out before SELECT plan(N) — e.g.
			// a setup statement the gateway/pooler couldn't handle.
			reason = "no TAP plan line emitted"
			if runErr != nil {
				reason += fmt.Sprintf("; psql exited: %v", runErr)
			}
			status = "fail"
		}

		res.Tests = append(res.Tests, IndividualTestResult{
			Name:       name,
			Status:     status,
			Duration:   fmt.Sprintf("%dms", dur.Milliseconds()),
			FailReason: reason,
		})
		res.TimedOut = res.TimedOut || ctx.Err() == context.DeadlineExceeded

		emoji := "✅"
		if status == "fail" {
			emoji = "❌"
		}
		t.Logf("  %s external/%s/%s (plan=%d ran=%d fail=%d) %v", emoji, ext.Name, name, planned, passed, len(failed), dur.Round(time.Millisecond))
	}

	// Tear down what we pre-created so the next extension's suite starts from the
	// state its expected output assumes. resetContribState (run before each
	// extension) only drops public, so an extension we installed into its own
	// schema — pg_partman in `partman` — would otherwise persist and change a
	// later suite's output: pgmq DependsOn pg_partman and its base.sql runs
	// `CREATE EXTENSION IF NOT EXISTS pg_partman`, which would then emit an
	// unexpected "already exists" NOTICE and diff against pgmq's base.out. Drop in
	// reverse so dependents go before dependencies; best-effort.
	for _, v := range slices.Backward(ext.PreCreateExtensions) {
		e := v
		_ = execOnPrimary(directPgPort, password, fmt.Sprintf("DROP EXTENSION IF EXISTS %q CASCADE", e.Name))
		if e.Schema != "" {
			_ = execOnPrimary(directPgPort, password, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", e.Schema))
		}
	}

	return res, nil
}

// selectPgTAPFiles returns the absolute paths of the .sql files under testDir to
// run, sorted and deduped: the union of globs (defaulting to ["*.sql"]), each
// matched relative to testDir, minus any file whose path relative to testDir
// matches an entry in excludes. globs and excludes use "/"-separated relative
// patterns (e.g. "test_pg17plus/*.sql"), matched with filepath.Match.
func selectPgTAPFiles(testDir string, globs, excludes []string) ([]string, error) {
	seen := map[string]bool{}
	var matches []string
	add := func(f string) {
		if !seen[f] {
			seen[f] = true
			matches = append(matches, f)
		}
	}

	if len(globs) == 0 {
		globs = []string{"*.sql"}
	}
	for _, g := range globs {
		m, err := filepath.Glob(filepath.Join(testDir, g))
		if err != nil {
			return nil, fmt.Errorf("bad test glob %q: %w", g, err)
		}
		for _, f := range m {
			add(f)
		}
	}

	if len(excludes) > 0 {
		kept := matches[:0]
		for _, f := range matches {
			rel, err := filepath.Rel(testDir, f)
			if err != nil {
				rel = filepath.Base(f)
			}
			drop := false
			for _, ex := range excludes {
				if ok, _ := filepath.Match(ex, rel); ok {
					drop = true
					break
				}
			}
			if !drop {
				kept = append(kept, f)
			}
		}
		matches = kept
	}

	sort.Strings(matches)
	return matches, nil
}

// parsePgTAPOutput parses a TAP stream emitted by a pgTAP test file run through
// psql. pgTAP prints a plan line ("1..N") and one "ok <n> - desc" / "not ok <n> -
// desc" line per assertion (diagnostics are "#"-prefixed and ignored; non-TAP
// command tags and result rows don't match the ok/not-ok prefixes). Returns the
// declared plan count (-1 when absent), the number of passing assertions, and the
// descriptions of the failing ones. "not ok" is checked before "ok" so a failing
// assertion is never miscounted as a pass.
func parsePgTAPOutput(output string) (planned, passed int, failed []string) {
	planned = -1
	for line := range strings.SplitSeq(output, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "1.."):
			// "1..134" → 134. A "1..0 # SKIP" form leaves planned at -1.
			if n, err := strconv.Atoi(strings.TrimSpace(line[len("1.."):])); err == nil {
				planned = n
			}
		case strings.HasPrefix(line, "not ok"):
			failed = append(failed, strings.TrimSpace(strings.TrimPrefix(line, "not ok")))
		case strings.HasPrefix(line, "ok"):
			passed++
		}
	}
	return planned, passed, failed
}

// summarizePgTAPFailures renders failing pgTAP assertion descriptions into a
// short, bounded FailReason (the full TAP is saved alongside as <name>.tap).
func summarizePgTAPFailures(failed []string) string {
	const max = 5
	shown := failed
	if len(shown) > max {
		shown = shown[:max]
	}
	s := fmt.Sprintf("%d assertion(s) failed: %s", len(failed), strings.Join(shown, " | "))
	if len(failed) > max {
		s += fmt.Sprintf(" … (+%d more)", len(failed)-max)
	}
	return s
}

// pgTAPScratchSchemas are the throwaway schemas pg_partman's test files create
// (CREATE SCHEMA partman_test; some also use partman_retention_test). They live
// outside the partman/pgtap/public extension schemas, so dropping them never
// touches installed state.
var pgTAPScratchSchemas = []string{"partman_test", "partman_retention_test"}

// cleanupPgTAPScratch best-effort drops the per-test scratch schemas on the
// primary (directly, bypassing the gateway like resetContribState), so a file
// that committed instead of rolling back can't poison the next one. The supported
// rolled-back suite never leaves these behind — this just keeps the run
// order-independent and re-runnable. It does NOT attempt the fuller cleanup the
// excluded autocommit/procedure suites would need (test roles, leaked pgTAP temp
// tables on pooled backends); those aren't run by default for the reasons in
// runExternalPgTAP's doc comment.
func cleanupPgTAPScratch(directPgPort int, password string) {
	for _, s := range pgTAPScratchSchemas {
		_ = execOnPrimary(directPgPort, password, fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", s))
	}
}
