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
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/tools/executil"

	// PostgreSQL driver for the diagnostic / shim install path.
	_ "github.com/lib/pq"
)

// Re-export pgbuilder constants so existing callers and scripts that reference
// pgregresstest.PostgresVersion / PostgresGitRepo keep working unchanged.
const (
	PostgresGitRepo  = pgbuilder.PostgresGitRepo
	PostgresVersion  = pgbuilder.PostgresVersion
	PostgresCacheDir = pgbuilder.PostgresCacheDir
)

// PostgresBuilder wraps pgbuilder.Builder with regression/isolation-suite
// specific helpers (run tests, parse TAP output, render reports).
//
// The source/build/install/cleanup logic lives in pgbuilder and is also used
// by the sqllogictest differential harness.
type PostgresBuilder struct {
	*pgbuilder.Builder
}

// TestResults contains the results from running PostgreSQL regression tests.
type TestResults struct {
	TotalTests     int
	PassedTests    int
	FailedTests    int
	SkippedTests   int
	TimedOut       bool // true if the suite was killed by context timeout
	Duration       time.Duration
	FailureDetails []TestFailure
	Tests          []IndividualTestResult // Per-test results in order
}

// IndividualTestResult represents a single test's result.
type IndividualTestResult struct {
	Name     string `json:"name"`
	Status   string `json:"status"` // "pass", "fail", "skip"
	Duration string `json:"duration"`
	// PatchApplied is true when a per-test patch (testdata/pg17/patches/<name>.patch)
	// was applied to the upstream expected output before diffing. This signals
	// that multigres's output intentionally differs from stock PostgreSQL for
	// this test (typically error-message wording) and the difference is
	// documented in the patch file.
	PatchApplied bool `json:"patch_applied,omitempty"`
	// PatchPath is the repo-relative path to the patch file (when applied),
	// so status reports can link back to it.
	PatchPath string `json:"patch_path,omitempty"`
	// FailReason is a short description populated when Status == "fail" and
	// the failure comes from the patch-verification step (e.g. "patch did not
	// apply" or "actual output does not match patched expected").
	FailReason string `json:"fail_reason,omitempty"`
}

// TestFailure represents a single test failure.
type TestFailure struct {
	TestName string
	Error    string
}

// NewPostgresBuilder creates a new PostgresBuilder with unique build directories.
func NewPostgresBuilder(t *testing.T) *PostgresBuilder {
	t.Helper()
	return &PostgresBuilder{Builder: pgbuilder.New(t)}
}

// CheckBuildDependencies verifies that required build tools are available.
func CheckBuildDependencies(t *testing.T) error {
	return pgbuilder.CheckBuildDependencies(t)
}

// testSuiteConfig holds configuration for running a PostgreSQL test suite
// via the shared runTestSuite helper.
type testSuiteConfig struct {
	suiteName string // for log messages, e.g. "Regression" or "Isolation"
	outputDir string // where to copy regression.out and regression.diffs
	srcOutDir string // build directory containing regression.out and regression.diffs
}

// runTestSuite executes a pre-built test command and handles result parsing,
// artifact copying, and failure reporting. Both RunRegressionTests and
// RunIsolationTests delegate to this after constructing their command.
func (pb *PostgresBuilder) runTestSuite(t *testing.T, ctx context.Context, cmd *executil.Cmd, cfg testSuiteConfig, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create %s output directory: %w", cfg.suiteName, err)
	}

	t.Logf("%s test results will be saved to: %s", cfg.suiteName, cfg.outputDir)

	// Cap how long Run() waits for I/O after the process exits. Without this,
	// grandchildren (e.g. psql) holding pipes open would cause a hang.
	cmd.SetWaitDelay(10 * time.Second)

	// pg_regress expected .out files were captured against vanilla PostgreSQL
	// defaults, so the planner picks different shapes (e.g. Hash↔Merge,
	// Bitmap↔Index) when pgctld's tuned GUCs are in effect. Override to
	// PostgreSQL defaults per session via PGOPTIONS so the harness matches
	// upstream fixtures without changing pgctld defaults.
	pgOptions := "-c work_mem=4MB" +
		" -c random_page_cost=4.0" +
		" -c effective_cache_size=4GB" +
		" -c max_parallel_workers_per_gather=2"

	cmd.AddEnv(
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
		"PGPASSWORD="+password,
		"PGDATABASE=postgres",
		"PGCONNECT_TIMEOUT=10",
		"PGOPTIONS="+pgOptions,
		// PG_TEST_TIMEOUT_DEFAULT (in seconds) caps how long isolationtester
		// waits per step before cancelling. Upstream default is 180 →
		// max_step_wait = 360 s with hard-exit at 720 s, which lets a
		// single compat-incompatible spec burn ~12 minutes of the suite
		// ctx and starve the rest of the schedule. The cap applies per
		// step and a multi-permutation spec where every permutation
		// hangs scales linearly. 5 s gives 10 s cancel / 20 s hard-exit
		// per step, ~500x headroom over the 10 ms poll interval used to
		// detect legitimate blocking.
		"PG_TEST_TIMEOUT_DEFAULT=5",
	)

	// Capture stdout for result parsing while still printing to the terminal.
	// pg_regress deletes regression.out when all tests pass, so stdout is our
	// reliable source for TAP output. We still try the on-disk file first
	// because it contains partial results if the process is killed mid-run.
	var stdoutBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = os.Stderr

	srcOut := filepath.Join(cfg.srcOutDir, "regression.out")

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)

	t.Logf("%s test execution completed in %v (err=%v)", cfg.suiteName, duration, err)

	outData, readErr := os.ReadFile(srcOut)
	if readErr != nil {
		outData = stdoutBuf.Bytes()
	}
	if len(outData) == 0 {
		return nil, fmt.Errorf("no %s TAP output: regression.out missing and stdout empty", cfg.suiteName)
	}
	results, parseErr := pb.ParseTestResults(string(outData))
	if parseErr != nil {
		return nil, fmt.Errorf("failed to parse %s regression.out: %w", cfg.suiteName, parseErr)
	}
	results.Duration = duration
	results.TimedOut = ctx.Err() == context.DeadlineExceeded

	srcDiffs := filepath.Join(cfg.srcOutDir, "regression.diffs")
	dstDiffs := filepath.Join(cfg.outputDir, "regression.diffs")
	dstOut := filepath.Join(cfg.outputDir, "regression.out")

	if diffsData, err := os.ReadFile(srcDiffs); err == nil {
		if err := os.WriteFile(dstDiffs, diffsData, 0o644); err != nil {
			t.Logf("Warning: Failed to copy %s regression.diffs: %v", cfg.suiteName, err)
		}
	}

	if err := os.WriteFile(dstOut, outData, 0o644); err != nil {
		t.Logf("Warning: Failed to copy %s regression.out: %v", cfg.suiteName, err)
	}

	t.Logf("")
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("%s test results saved to: %s", cfg.suiteName, cfg.outputDir)
	t.Logf("  • Summary:     %s", dstOut)
	t.Logf("  • Differences: %s", dstDiffs)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("")

	if results.FailedTests > 0 {
		if diffsContent, err := os.ReadFile(dstDiffs); err == nil {
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("%s Differences (from %s):", cfg.suiteName, dstDiffs)
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("%s", string(diffsContent))
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		} else {
			t.Logf("Warning: Could not read %s regression.diffs: %v", cfg.suiteName, err)
		}
	}

	if results.TotalTests > 0 {
		return results, err
	}

	if err != nil {
		return nil, fmt.Errorf("%s test harness failed to execute: %w", cfg.suiteName, err)
	}

	return results, nil
}

// RunRegressionTests runs PostgreSQL regression tests against multigateway.
//
// Uses make installcheck-tests with TESTS variable to run specific regression tests
// against the existing PostgreSQL server (multigateway).
//
// The installcheck-tests target runs specific tests against an already-running
// PostgreSQL server, unlike installcheck which runs the entire parallel_schedule.
//
// From PostgreSQL's src/test/regress/GNUmakefile:
//
//	installcheck: runs --schedule=parallel_schedule (all tests)
//	installcheck-tests: runs $(TESTS) (specific tests only)
//
// Environment variables that pg_regress reads:
//
//	PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE - connection params
//
// Reference: https://github.com/postgres/postgres/blob/master/src/test/regress/GNUmakefile
func (pb *PostgresBuilder) RunRegressionTests(t *testing.T, ctx context.Context, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	t.Logf("Running PostgreSQL regression tests against multigateway on port %d...", multigatewayPort)

	regressDir := filepath.Join(pb.BuildDir, "src", "test", "regress")
	makeArgs := []string{"-C", regressDir}

	// --use-existing: skip pg_regress's automatic DROP + CREATE of the
	// "regression" database. Multigateway rejects DROP DATABASE (see the
	// unsafe-statement list in go/services/multigateway/planner/unsafe_stmt.go),
	// so we can't let pg_regress manage the database. Instead we run against
	// the existing "postgres" database for the whole suite.
	//
	// --dbname=postgres: point pg_regress at that existing database. pg_regress
	// will create/drop the expected schema objects per test; cross-test state
	// leakage is still possible but has not surfaced in practice.
	makeArgs = append(makeArgs, "EXTRA_REGRESS_OPTS=--use-existing --dbname=postgres")

	if testsEnv := os.Getenv("PGREGRESS_TESTS"); testsEnv != "" {
		makeArgs = append(makeArgs, "installcheck-tests", "TESTS="+testsEnv)
		t.Logf("Running selective regression tests: %s", testsEnv)
	} else {
		makeArgs = append(makeArgs, "installcheck")
		t.Logf("Running full PostgreSQL regression test suite (installcheck)")
	}

	cmd := executil.Command(ctx, "make", makeArgs...).WithProcessGroup()

	return pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
		suiteName: "Regression",
		outputDir: pb.OutputDir,
		srcOutDir: regressDir,
	}, multigatewayPort, password)
}

// DefaultContribModules is the core-contrib extension test set run through
// multigateway by default. Each entry is a directory under contrib/ whose
// sql/ + expected/ fixtures pg_regress drives via the module's installcheck
// target.
//
// It is derived from ExtensionCatalog (every StatusCovered entry), which is the
// single source of truth for coverage state and the rationale behind each
// extension's status (covered / pending / unsupported / external). uuid-ossp
// and pgcrypto need extra ./configure features enabled by the harness when the
// contrib suite runs (--with-uuid and --with-ssl=openssl; see
// TestPostgreSQLRegression). See extensions.go.
var DefaultContribModules = CoveredContribModules()

// ContribModules returns the contrib module directories to test. PGCONTRIB_TESTS
// (space-separated directory names) overrides the default set when set.
func ContribModules() []string {
	if v := os.Getenv("PGCONTRIB_TESTS"); v != "" {
		return strings.Fields(v)
	}
	return DefaultContribModules
}

// RunContribTests runs each contrib module's installcheck suite against
// multigateway and returns one merged TestResults across all modules.
//
// Each module is a separate pg_regress invocation (make -C contrib/<mod>
// installcheck). As with the core regression suite we pass
// --use-existing --dbname=postgres because multigateway rejects DROP/CREATE
// DATABASE. Per-test names are prefixed with the module directory
// ("citext/citext") so they stay unique in the merged report.
//
// All modules share the single postgres database (multigateway can't isolate
// per-DB), so before each module we reset the public schema directly on the
// primary (directPgPort) to clear objects/extensions a previous module left
// behind — otherwise a module's CREATE TYPE/EXTENSION collides with leftovers
// and its expected output diverges. The reset bypasses multigateway because
// the gateway rejects schema DDL; the standby picks it up via WAL.
//
// A module that produces no parseable output (e.g. a module with no REGRESS
// tests) is logged and skipped — one module must not abort the rest of the set.
func (pb *PostgresBuilder) RunContribTests(t *testing.T, ctx context.Context, modules []string, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	// contrib installcheck targets invoke $(top_builddir)/src/test/regress/pg_regress.
	// Build it up front so a contrib-only run (RUN_PGCONTRIB without RUN_PGREGRESS)
	// does not fail for a missing pg_regress binary.
	regressDir := filepath.Join(pb.BuildDir, "src", "test", "regress")
	if out, err := executil.Command(ctx, "make", "-C", regressDir, "all").CombinedOutput(); err != nil {
		return nil, fmt.Errorf("failed to build pg_regress: %w\n%s", err, truncateForLog(string(out), 2000))
	}

	merged := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}

	// Replace pg_regress's strict text diff with the patch-based pipeline, which
	// whitespace-normalizes both sides (so error-cursor caret-position shifts
	// from multigateway query rewriting are not diffs) and applies per-test
	// patches for genuine multigres-specific output differences.
	mode := GetPatchMode()

	for _, mod := range modules {
		moduleDir := filepath.Join(pb.BuildDir, "contrib", mod)
		if !suiteutil.FileExists(filepath.Join(moduleDir, "Makefile")) {
			t.Logf("contrib/%s: no Makefile in build tree, skipping", mod)
			continue
		}

		// Clean slate for this module: drop anything a prior module left in
		// public. Best-effort — log and proceed if it fails, since the run is
		// still useful (the failure will show up as a diff).
		if err := resetContribState(directPgPort, password); err != nil {
			t.Logf("contrib/%s: warning: public schema reset failed: %v", mod, err)
		}

		t.Logf("Running contrib/%s installcheck against multigateway...", mod)
		cmd := executil.Command(ctx, "make", "-C", moduleDir, "installcheck",
			"EXTRA_REGRESS_OPTS=--use-existing --dbname=postgres").WithProcessGroup()

		res, err := pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
			suiteName: "Contrib/" + mod,
			outputDir: filepath.Join(pb.OutputDir, "contrib", mod),
			srcOutDir: moduleDir,
		}, multigatewayPort, password)
		if res == nil {
			// No TAP output: module has no REGRESS tests or the harness failed
			// to execute. Log and continue so one module can't abort the set.
			t.Logf("contrib/%s: no test results (%v)", mod, err)
			continue
		}

		// Re-evaluate each test's verdict via the patch pipeline (bare names,
		// before the module prefix is applied below).
		pb.verifyContribModule(ctx, mod, res, mode)

		for _, tr := range res.Tests {
			tr.Name = mod + "/" + tr.Name
			merged.Tests = append(merged.Tests, tr)
			switch tr.Status {
			case "fail":
				merged.FailedTests++
				detail := tr.FailReason
				if detail == "" {
					detail = "see contrib/" + mod + "/regression.diffs"
				}
				merged.FailureDetails = append(merged.FailureDetails, TestFailure{
					TestName: tr.Name,
					Error:    detail,
				})
			case "skip":
				merged.SkippedTests++
			default:
				merged.PassedTests++
			}
		}
		merged.TimedOut = merged.TimedOut || res.TimedOut
	}

	merged.TotalTests = merged.PassedTests + merged.FailedTests + merged.SkippedTests
	return merged, nil
}

// ExternalModules returns the external extensions to test, defaulting to the
// covered set (CoveredExternalExtensions). PGEXTERNAL_TESTS (space-separated
// catalog names, e.g. "vector") selects a subset for local iteration.
func ExternalModules() []ExternalExtension {
	all := CoveredExternalExtensions()
	sel := strings.Fields(os.Getenv("PGEXTERNAL_TESTS"))
	if len(sel) == 0 {
		return all
	}
	want := make(map[string]bool, len(sel))
	for _, s := range sel {
		want[s] = true
	}
	var out []ExternalExtension
	for _, e := range all {
		if want[e.Name] {
			out = append(out, e)
		}
	}
	return out
}

// listRegressTests returns the regression test names for an external extension,
// derived from the .sql files under <testDir>/sql. This mirrors the PGXS
// convention REGRESS = $(patsubst test/sql/%.sql,%,$(wildcard test/sql/*.sql)).
// Names are sorted so the run order is deterministic and matches make's sorted
// $(wildcard).
func listRegressTests(testDir string) []string {
	matches, err := filepath.Glob(filepath.Join(testDir, "sql", "*.sql"))
	if err != nil {
		return nil
	}
	sort.Strings(matches)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, strings.TrimSuffix(filepath.Base(m), ".sql"))
	}
	return names
}

// RunExternalTests runs each external extension's shipped pg_regress suite
// against multigateway and returns one merged TestResults across all of them.
//
// External (KindExternal) extensions are PGXS modules that live outside the
// PostgreSQL source tree (see Builder.InstallExternalExtension, which has
// already cloned and installed each one before this is called). They ship the
// same sql/ + expected/ fixture layout as contrib, so verification reuses the
// shared pipeline (verifyModuleResults). Per-test names are prefixed with the
// extension name ("vector/btree") to stay unique in the merged report.
//
// Unlike contrib we cannot use `make installcheck`: under PGXS that target runs
// $(top_builddir)/src/test/regress/pg_regress, and PGXS resolves top_builddir
// into the install tree, where pg_regress is not installed. We instead invoke
// the pg_regress we built directly, passing the same flags the contrib suite
// relies on (--use-existing --dbname=postgres, because multigateway rejects
// DROP/CREATE DATABASE) plus the extension's --inputdir/--load-extension.
//
// As with contrib, all extensions share the single postgres database, so before
// each one we reset the public schema directly on the primary (directPgPort,
// bypassing the gateway's DDL block) to clear objects a prior extension left
// behind; pg_regress's --load-extension re-creates the extension per test.
func (pb *PostgresBuilder) RunExternalTests(t *testing.T, ctx context.Context, exts []ExternalExtension, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	// Ensure the pg_regress we drive directly is built (a contrib/regression run
	// would have built it already; an external-only run must build it here).
	regressDir := filepath.Join(pb.BuildDir, "src", "test", "regress")
	pgRegress := filepath.Join(regressDir, "pg_regress")
	if !suiteutil.FileExists(pgRegress) {
		if out, err := executil.Command(ctx, "make", "-C", regressDir, "all").CombinedOutput(); err != nil {
			return nil, fmt.Errorf("failed to build pg_regress: %w\n%s", err, truncateForLog(string(out), 2000))
		}
	}

	merged := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}
	mode := GetPatchMode()
	pgBinDir := pb.BinDir()

	for _, ext := range exts {
		cloneDir := filepath.Join(pb.ExternalDir, ext.Name)
		// Fixtures live under ext.TestSubdir within the checkout (pgvector: test/;
		// pg_cron: the repo root, i.e. "."). filepath.Join collapses "." back to
		// the clone root.
		testDir := filepath.Join(cloneDir, ext.TestSubdir)
		if !suiteutil.FileExists(filepath.Join(testDir, "sql")) {
			t.Logf("external/%s: no %s/sql in checkout, skipping", ext.Name, ext.TestSubdir)
			continue
		}

		tests := listRegressTests(testDir)
		if len(tests) == 0 {
			t.Logf("external/%s: no .sql tests found, skipping", ext.Name)
			continue
		}

		// Clean slate for this extension: drop anything a prior extension left in
		// public. Best-effort — log and proceed if it fails.
		if err := resetContribState(directPgPort, password); err != nil {
			t.Logf("external/%s: warning: public schema reset failed: %v", ext.Name, err)
		}

		// Front-load the extension's scratch databases directly on the primary.
		// multigateway blocks CREATE DATABASE by design (one-DB-per-instance; see
		// ExternalExtension.ScratchDatabases), but the suite only needs them to
		// exist in the catalog, not to be connectable, so create them here.
		// Best-effort. They are dropped after the suite runs (below).
		for _, db := range ext.ScratchDatabases {
			if err := execOnPrimary(directPgPort, password, fmt.Sprintf("CREATE DATABASE %q", db)); err != nil {
				t.Logf("external/%s: warning: create scratch db %q failed: %v", ext.Name, db, err)
			}
		}

		// Load the extension's fixtures through multigateway before the suite, the
		// way its own runner does (pg_graphql's bin/installcheck runs
		// `psql -f test/fixtures.sql` first; those fixtures CREATE the extension and
		// set its schema config). Same pooled path the test queries take.
		if ext.FixturesFile != "" {
			fixturesPath := filepath.Join(testDir, ext.FixturesFile)
			if err := loadFixturesViaGateway(ctx, filepath.Join(pgBinDir, "psql"), fixturesPath, multigatewayPort, password); err != nil {
				t.Logf("external/%s: warning: load fixtures %q failed: %v", ext.Name, ext.FixturesFile, err)
			}
		}

		// Preload the extension when its fixtures assume it already exists:
		// --use-existing skips pg_regress's own --load-extension step, and
		// pgvector's fixtures open with a bare CREATE TABLE ... vector(3) (see
		// createExtensionViaGateway). Extensions whose fixtures manage the
		// extension themselves (pg_cron CREATEs it as their first statement) set
		// CreateExtension=false so we don't collide with that.
		if ext.CreateExtension {
			if err := createExtensionViaGateway(multigatewayPort, password, ext.Name); err != nil {
				t.Logf("external/%s: warning: CREATE EXTENSION failed: %v", ext.Name, err)
			}
		}

		t.Logf("Running external/%s pg_regress (%d tests) against multigateway...", ext.Name, len(tests))
		args := []string{
			"--inputdir=" + testDir,
			"--outputdir=" + cloneDir,
			"--bindir=" + pgBinDir,
			"--use-existing",
			"--dbname=postgres",
		}
		// --load-extension only fires inside create_database(), which pg_regress
		// skips under --use-existing, so it is a no-op here; pass it only for the
		// preloaded extensions to keep intent clear.
		if ext.CreateExtension {
			args = append(args, "--load-extension="+ext.Name)
		}
		args = append(args, tests...)
		// Run from the clone root so psql's client-side \copy resolves the
		// relative paths the fixtures use (e.g. pgvector's copy test does
		// \copy t TO 'results/vector.bin'); pg_regress writes its results/ dir
		// under --outputdir=cloneDir, which is this same directory.
		cmd := executil.Command(ctx, pgRegress, args...).WithProcessGroup().SetDir(cloneDir)

		res, err := pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
			suiteName: "External/" + ext.Name,
			outputDir: filepath.Join(pb.OutputDir, "external", ext.Name),
			srcOutDir: cloneDir,
		}, multigatewayPort, password)

		// Drop the scratch databases now the suite has run (verification reads
		// result files, not the live DB). WITH (FORCE) in case pg_cron's launcher
		// opened a connection. Best-effort; the cluster is torn down after anyway.
		for _, db := range ext.ScratchDatabases {
			if err := execOnPrimary(directPgPort, password, fmt.Sprintf("DROP DATABASE IF EXISTS %q WITH (FORCE)", db)); err != nil {
				t.Logf("external/%s: warning: drop scratch db %q failed: %v", ext.Name, db, err)
			}
		}

		if res == nil {
			t.Logf("external/%s: no test results (%v)", ext.Name, err)
			continue
		}

		// Re-evaluate each test via the patch pipeline. pg_regress wrote results
		// to <cloneDir>/results (--outputdir); expected lives in <cloneDir>/test
		// (--inputdir), and patches are per-extension under patches/external/<ext>.
		patchDir := filepath.Join(PatchesDir(), "external", ext.Name)
		pb.verifyModuleResults(ctx, testDir, filepath.Join(cloneDir, "results"), patchDir, res, mode)

		for _, tr := range res.Tests {
			tr.Name = ext.Name + "/" + tr.Name
			merged.Tests = append(merged.Tests, tr)
			switch tr.Status {
			case "fail":
				merged.FailedTests++
				detail := tr.FailReason
				if detail == "" {
					detail = "see external/" + ext.Name + "/regression.diffs"
				}
				merged.FailureDetails = append(merged.FailureDetails, TestFailure{
					TestName: tr.Name,
					Error:    detail,
				})
			case "skip":
				merged.SkippedTests++
			default:
				merged.PassedTests++
			}
		}
		merged.TimedOut = merged.TimedOut || res.TimedOut
	}

	merged.TotalTests = merged.PassedTests + merged.FailedTests + merged.SkippedTests
	return merged, nil
}

// verifyContribModule re-evaluates each test in a contrib module's results
// using the patch-based pipeline (see patch_verify.go), replacing pg_regress's
// strict text verdict. Expected output lives in the source tree
// (contrib/<mod>/expected); actual output is written by pg_regress into the
// build tree (contrib/<mod>/results). Patches are per-module
// (testdata/pg<major>/patches/contrib/<mod>/<test>.patch) so test names that
// collide across modules (e.g. "init") do not clash.
//
// res.Tests carry bare test names here (the module prefix is applied by the
// caller afterwards). Statuses are updated in place.
func (pb *PostgresBuilder) verifyContribModule(ctx context.Context, mod string, res *TestResults, mode PatchMode) {
	moduleSrcDir := filepath.Join(pb.SourceDir, "contrib", mod)
	moduleResultsDir := filepath.Join(pb.BuildDir, "contrib", mod, "results")
	patchDir := filepath.Join(PatchesDir(), "contrib", mod)
	pb.verifyModuleResults(ctx, moduleSrcDir, moduleResultsDir, patchDir, res, mode)
}

// verifyModuleResults is the shared per-test verification loop behind both the
// contrib and external suites. expectedDir is the directory whose expected/
// subdirectory holds the upstream .out files (and numbered variants); resultsDir
// holds the .out files pg_regress wrote for this run; patchDir holds the
// per-test patches for genuine multigres-specific differences. Statuses in
// res.Tests are updated in place; skipped tests are left untouched.
func (pb *PostgresBuilder) verifyModuleResults(ctx context.Context, expectedDir, resultsDir, patchDir string, res *TestResults, mode PatchMode) {
	repoRoot := findRepoRoot()

	for i := range res.Tests {
		test := &res.Tests[i]
		if test.Status == "skip" {
			continue
		}

		// PostgreSQL ships alternate expected files (name_1.out, name_2.out, …)
		// for output that legitimately varies by platform/build — e.g.
		// citext_utf8_1.out for a C-locale run, or pgcrypto blowfish_1.out for an
		// OpenSSL build without the legacy cipher provider. pg_regress passes if
		// actual matches ANY variant; mirror that here before falling back to
		// patch-based verification, otherwise a canonical-only comparison
		// manufactures diffs that aren't multigres behavior at all.
		variants := expectedVariants(expectedDir, test.Name)
		actPath := filepath.Join(resultsDir, test.Name+".out")
		if len(variants) == 0 || !suiteutil.FileExists(actPath) {
			// Test didn't run or expected missing; keep the TAP verdict.
			test.FailReason = "expected or actual output missing; kept TAP verdict"
			continue
		}

		if actualMatchesAnyVariant(actPath, variants) {
			test.Status = "pass"
			test.PatchApplied = false
			test.PatchPath = ""
			test.FailReason = ""
			// A patch is unnecessary once a stock variant matches; drop a stale one.
			if mode == PatchModeGenerate {
				_ = os.Remove(filepath.Join(patchDir, test.Name+".patch"))
			}
			continue
		}

		// No variant matches: apply (verify) or write (generate) a patch against
		// the canonical expected file for genuine multigres-specific differences.
		outcome, err := VerifyTest(ctx, VerifyInput{
			Name:         test.Name,
			ExpectedPath: variants[0],
			ActualPath:   actPath,
			PatchDir:     patchDir,
			RepoRoot:     repoRoot,
		}, mode)
		if err != nil {
			test.Status = "fail"
			test.FailReason = err.Error()
			continue
		}

		test.Status = outcome.Status
		test.PatchApplied = outcome.PatchApplied
		test.PatchPath = outcome.PatchPath
		test.FailReason = outcome.Reason
	}
}

// expectedVariants returns the expected-output files pg_regress would accept for
// a test: the canonical <name>.out first, then numbered variants
// <name>_1.out … <name>_9.out that exist. Mirrors findExpectedFile's search but
// returns every match rather than only the first.
func expectedVariants(regressDir, name string) []string {
	var out []string
	canonical := filepath.Join(regressDir, "expected", name+".out")
	if suiteutil.FileExists(canonical) {
		out = append(out, canonical)
	}
	for i := 1; i < 10; i++ {
		p := filepath.Join(regressDir, "expected", fmt.Sprintf("%s_%d.out", name, i))
		if suiteutil.FileExists(p) {
			out = append(out, p)
		}
	}
	return out
}

// actualMatchesAnyVariant reports whether the actual output equals any expected
// variant after whitespace normalization (the same canonicalization the patch
// pipeline uses, so error-cursor caret shifts are not treated as differences).
func actualMatchesAnyVariant(actPath string, variants []string) bool {
	actRaw, err := os.ReadFile(actPath)
	if err != nil {
		return false
	}
	act := normalizeWhitespace(actRaw)
	for _, v := range variants {
		expRaw, err := os.ReadFile(v)
		if err != nil {
			continue
		}
		if bytes.Equal(normalizeWhitespace(expRaw), act) {
			return true
		}
	}
	return false
}

// resetContribState drops and recreates the public schema on the primary's
// PostgreSQL directly (bypassing multigateway, which rejects schema DDL),
// clearing every object and extension a previous contrib module installed
// there. Contrib test fixtures assume a clean database; sharing one postgres
// DB across modules otherwise leaks types/extensions between them.
func resetContribState(directPgPort int, password string) error {
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		directPgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	stmts := []string{
		"DROP SCHEMA IF EXISTS public CASCADE",
		"CREATE SCHEMA public",
		"GRANT ALL ON SCHEMA public TO public",
		"GRANT ALL ON SCHEMA public TO postgres",
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("exec [%s]: %w", stmt, err)
		}
	}
	return nil
}

// execOnPrimary runs a single statement directly on the primary's PostgreSQL
// (bypassing multigateway, which rejects some Tier 2 DDL like CREATE DATABASE).
// Used to front-load fixture setup an extension's suite needs but the gateway
// blocks; the standby picks the change up via WAL. Each call is its own
// autocommit connection so statements that can't run in a transaction block
// (CREATE DATABASE) work.
func execOnPrimary(directPgPort int, password, stmt string) error {
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		directPgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("exec [%s]: %w", stmt, err)
	}
	return nil
}

// createExtensionViaGateway runs CREATE EXTENSION IF NOT EXISTS through
// multigateway — the same path the test queries take, so there's no
// create-on-primary-then-read-from-standby replication race, and it doubles as
// real coverage that CREATE EXTENSION works over the pooled path (CREATE
// EXTENSION is not on the gateway's blocked-DDL list; contrib tests create their
// own extensions through the gateway too).
//
// This is needed because pg_regress applies its --load-extension flag only
// inside create_database(), which it calls solely when !use_existing (see
// pg_regress.c). The external suite must pass --use-existing (multigateway
// rejects CREATE/DROP DATABASE), so that load step never fires. Extensions whose
// test fixtures assume the extension is preloaded (e.g. pgvector — its tests
// open with a bare CREATE TABLE t (val vector(3)) and never CREATE EXTENSION
// themselves) would otherwise fail every statement with "type ... does not
// exist". Creating it here reproduces what create_database would have done.
func createExtensionViaGateway(multigatewayPort int, password, ext string) error {
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		multigatewayPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	// ext comes from the controlled extension catalog, not user input; quote it
	// as an identifier defensively all the same.
	stmt := fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS "%s"`, ext)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("exec [%s]: %w", stmt, err)
	}
	return nil
}

// loadFixturesViaGateway runs an extension's fixtures SQL file through
// multigateway with psql before its suite, mirroring how the extension's own
// runner seeds the database (pg_graphql's bin/installcheck runs
// `psql -v ON_ERROR_STOP=1 -f test/fixtures.sql` first). Routing it through the
// gateway — not the primary — keeps setup on the same pooled path as the tests
// and exercises the fixtures' own DDL (pg_graphql's fixtures CREATE the
// extension and set the graphql schema comment). psql replays the multi-statement
// file faithfully, including any backslash commands.
func loadFixturesViaGateway(ctx context.Context, psqlPath, fixturesPath string, multigatewayPort int, password string) error {
	cmd := executil.Command(ctx, psqlPath,
		"-v", "ON_ERROR_STOP=1",
		"-f", fixturesPath,
		"-d", "postgres")
	cmd.AddEnv(
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
		"PGPASSWORD="+password,
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("psql -f %s: %w\n%s", fixturesPath, err, truncateForLog(string(out), 2000))
	}
	return nil
}

// ParseTestResults parses pg_regress TAP output to extract test results.
// Returns an error if no TAP-formatted lines are found in the output.
func (pb *PostgresBuilder) ParseTestResults(output string) (*TestResults, error) {
	results := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}

	// Parse TAP format output from pg_regress
	// Example lines:
	//   ok 1         - test_setup                                178 ms  (serial)
	//   ok 2         + boolean                                    61 ms  (parallel)
	//   not ok 3     + char                                       39 ms  (parallel)
	tapLine := regexp.MustCompile(`(?m)^(ok|not ok)\s+(\d+)\s+[-+]\s+(\S+)\s+(\d+)\s+ms`)
	for _, match := range tapLine.FindAllStringSubmatch(output, -1) {
		status := "pass"
		if match[1] == "not ok" {
			status = "fail"
		}
		results.Tests = append(results.Tests, IndividualTestResult{
			Name:     match[3],
			Status:   status,
			Duration: match[4] + "ms",
		})
		if status == "pass" {
			results.PassedTests++
		} else {
			results.FailedTests++
			results.FailureDetails = append(results.FailureDetails, TestFailure{
				TestName: match[3],
				Error:    "Test failed (see regression.diffs for details)",
			})
		}
	}

	if len(results.Tests) == 0 {
		return nil, errors.New("no TAP-formatted test output found in pg_regress output")
	}

	summaryPassPattern := regexp.MustCompile(`All (\d+) tests? passed`)
	summaryFailPattern := regexp.MustCompile(`(\d+) of (\d+) tests? failed`)

	if matches := summaryPassPattern.FindStringSubmatch(output); len(matches) > 1 {
		total, _ := strconv.Atoi(matches[1])
		results.TotalTests = total
		results.PassedTests = total
		results.FailedTests = 0
	} else if matches := summaryFailPattern.FindStringSubmatch(output); len(matches) > 2 {
		failed, _ := strconv.Atoi(matches[1])
		total, _ := strconv.Atoi(matches[2])
		results.TotalTests = total
		results.FailedTests = failed
		results.PassedTests = total - failed
	}

	if results.TotalTests == 0 && (results.PassedTests > 0 || results.FailedTests > 0) {
		results.TotalTests = results.PassedTests + results.FailedTests + results.SkippedTests
	}

	return results, nil
}

// PatchesDir returns the absolute path to the per-test patch directory for
// the PostgreSQL major version this builder is pinned to. Patches live under
// testdata/pg<major>/patches/ next to this file.
func PatchesDir() string {
	_, file, _, _ := runtime.Caller(0)
	pkgDir := filepath.Dir(file)
	return filepath.Join(pkgDir, "testdata", pgMajorDir(), "patches")
}

// pgMajorDir returns the directory name under testdata/ that holds patches for
// the PostgreSQL version this test targets. Pinned to pg17 today; add a case
// when PostgresVersion advances.
func pgMajorDir() string {
	// "REL_17_6" → "pg17"
	if strings.HasPrefix(PostgresVersion, "REL_17") {
		return "pg17"
	}
	// Unknown version: fall back to pg17 and let the test fail if patches
	// are missing.
	return "pg17"
}

// findRepoRoot walks up from this source file until it finds a directory
// containing go.mod. Returns empty string if not found — callers degrade to
// absolute paths in that case.
func findRepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// findExpectedFile locates the expected-output file pg_regress would use for
// the given test name. PostgreSQL ships variant files (name_1.out, name_2.out,
// …) for platform-dependent output. We try the canonical name first, then
// numbered variants in order. Returns the empty string if none exist.
func findExpectedFile(regressDir, name string) string {
	canonical := filepath.Join(regressDir, "expected", name+".out")
	if suiteutil.FileExists(canonical) {
		return canonical
	}
	for i := 1; i < 10; i++ {
		p := filepath.Join(regressDir, "expected", fmt.Sprintf("%s_%d.out", name, i))
		if suiteutil.FileExists(p) {
			return p
		}
	}
	return ""
}

// VerifyWithPatches re-evaluates each test's pass/fail status using the
// patch-based pipeline (see patch_verify.go). After pg_regress runs, this
// ignores pg_regress's own pass/fail verdict (which is a strict text diff)
// and replaces it with: does the actual output match the (patched) expected
// output? Results are updated in-place, including aggregate counters.
//
// In generate mode, any residual diffs are absorbed by (re)writing patches.
//
// Expected output lives in the source tree (prep_buildtree does not symlink
// .out files into the build tree). Actual output is written by pg_regress
// into the build tree's results/ directory.
func (pb *PostgresBuilder) VerifyWithPatches(t *testing.T, ctx context.Context, results *TestResults, buildRegressDir, outputDir string) error {
	t.Helper()
	mode := GetPatchMode()
	patchDir := PatchesDir()
	repoRoot := findRepoRoot()
	sourceRegressDir := filepath.Join(pb.SourceDir, "src", "test", "regress")

	// Ensure patch dir exists in generate mode so writes don't fail.
	if mode == PatchModeGenerate {
		if err := os.MkdirAll(patchDir, 0o755); err != nil {
			return fmt.Errorf("mkdir patches: %w", err)
		}
	}

	t.Logf("Patch-based verification: mode=%s patches=%s", mode, patchDir)
	t.Logf("  expected source: %s", sourceRegressDir)
	t.Logf("  actual source:   %s", buildRegressDir)

	// Per-test residual diffs are written under outputDir/diffs/ for inclusion
	// in the CI artifact. Concatenated failures.diffs is written at the end.
	diffsDir := ""
	if outputDir != "" {
		diffsDir = filepath.Join(outputDir, "diffs")
	}
	var aggregated bytes.Buffer

	// Recompute aggregates from the per-test results after verification.
	// We intentionally discard pg_regress's TAP-derived aggregates because
	// patch-based verification is authoritative.
	var passed, failed int
	failures := results.FailureDetails[:0]

	for i := range results.Tests {
		test := &results.Tests[i]
		if test.Status == "skip" {
			// Leave skipped tests alone.
			continue
		}

		expPath := findExpectedFile(sourceRegressDir, test.Name)
		actPath := filepath.Join(buildRegressDir, "results", test.Name+".out")
		if expPath == "" || !suiteutil.FileExists(actPath) {
			// Infrastructure problem (test didn't run, expected missing).
			// Preserve TAP verdict, count accordingly.
			test.FailReason = "expected or actual output missing; kept TAP verdict"
			if test.Status == "pass" {
				passed++
			} else {
				failed++
				failures = append(failures, TestFailure{
					TestName: test.Name,
					Error:    test.FailReason,
				})
			}
			continue
		}

		outcome, err := VerifyTest(ctx, VerifyInput{
			Name:         test.Name,
			ExpectedPath: expPath,
			ActualPath:   actPath,
			PatchDir:     patchDir,
			RepoRoot:     repoRoot,
		}, mode)
		if err != nil {
			return fmt.Errorf("verify %s: %w", test.Name, err)
		}

		test.Status = outcome.Status
		test.PatchApplied = outcome.PatchApplied
		test.PatchPath = outcome.PatchPath
		test.FailReason = outcome.Reason

		if outcome.Status == "pass" {
			passed++
			continue
		}
		failed++
		failures = append(failures, TestFailure{
			TestName: test.Name,
			Error:    outcome.Reason,
		})

		if outcome.Diff == "" || diffsDir == "" {
			continue
		}
		if err := os.MkdirAll(diffsDir, 0o755); err != nil {
			t.Logf("Warning: mkdir %s: %v", diffsDir, err)
			continue
		}
		diffPath := filepath.Join(diffsDir, test.Name+".diff")
		if err := os.WriteFile(diffPath, []byte(outcome.Diff), 0o644); err != nil {
			t.Logf("Warning: write %s: %v", diffPath, err)
			continue
		}
		fmt.Fprintf(&aggregated, "=== %s ===\n%s\n", test.Name, outcome.Diff)
	}

	if outputDir != "" && aggregated.Len() > 0 {
		aggPath := filepath.Join(outputDir, "failures.diffs")
		if err := os.WriteFile(aggPath, aggregated.Bytes(), 0o644); err != nil {
			t.Logf("Warning: write %s: %v", aggPath, err)
		} else {
			t.Logf("Residual failure diffs: %s (per-test files in %s)", aggPath, diffsDir)
		}
	}

	results.PassedTests = passed
	results.FailedTests = failed
	// Preserve SkippedTests + any pre-existing total if it exceeds ran.
	if results.TotalTests < passed+failed+results.SkippedTests {
		results.TotalTests = passed + failed + results.SkippedTests
	}
	results.FailureDetails = failures
	return nil
}

// CountScheduleTests parses a PostgreSQL schedule file and returns the number
// of tests listed. Each line starting with "test:" contains space-separated
// test names (parallel groups have multiple tests per line).
func CountScheduleTests(scheduleFile string) (int, error) {
	data, err := os.ReadFile(scheduleFile)
	if err != nil {
		return 0, err
	}
	count := 0
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "test:"); ok {
			count += len(strings.Fields(rest))
		}
	}
	return count, nil
}

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
			status := "✅ ok"
			switch test.Status {
			case "fail":
				status = "❌ FAIL"
			case "skip":
				status = "⏭️ skip"
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

// patchIsolationtester rewrites two pieces of
// src/test/isolation/isolationtester.c so the harness works against
// multigateway:
//
//  1. Per-session application_name setup. Upstream sends
//     PQexecParams("SELECT set_config('application_name',
//     current_setting('application_name') || '/' || $1, false)", ...),
//     which the multigateway planner rejects (the value must be a literal
//     constant for is_local=false set_config so the pooler can track it).
//     The replacement reads PGAPPNAME (set by isolation_main.c per test),
//     concatenates the session name client-side, escapes via
//     PQescapeLiteral, and sends a simple-protocol PQexec.
//
//  2. Lock-wait probe function name. Upstream prepares
//     `pg_catalog.pg_isolation_test_session_is_blocked(...)`. Replacing
//     that builtin C function with a PL/pgSQL shim via CREATE OR REPLACE
//     proved unreliable (fresh backends were observed to still bind the
//     C entry, returning false for every probe and hanging every
//     blocking spec at max_step_wait). We point the harness at our own
//     function `public.multigres_test_session_is_blocked` (installed by
//     installPIDMappingFunction) with explicit arg casts so PG resolves
//     it under extended-protocol Parse with paramTypes=NULL.
//
// Idempotent: source is reset via `git checkout` before patching, so
// repeat invocations against the cached checkout produce the same
// result.
func (pb *PostgresBuilder) patchIsolationtester(t *testing.T, ctx context.Context) error {
	t.Helper()
	rel := filepath.Join("src", "test", "isolation", "isolationtester.c")
	abs := filepath.Join(pb.SourceDir, rel)

	reset := executil.Command(ctx, "git", "-C", pb.SourceDir, "checkout", "--", rel)
	if out, err := reset.CombinedOutput(); err != nil {
		return fmt.Errorf("reset %s: %w\n%s", rel, err, out)
	}

	src, err := os.ReadFile(abs)
	if err != nil {
		return fmt.Errorf("read %s: %w", abs, err)
	}

	appNameOrig := "\t\tres = PQexecParams(conns[i].conn,\n" +
		"\t\t\t\t\t\t   \"SELECT set_config('application_name',\\n\"\n" +
		"\t\t\t\t\t\t   \"  current_setting('application_name') || '/' || $1,\\n\"\n" +
		"\t\t\t\t\t\t   \"  false)\",\n" +
		"\t\t\t\t\t\t   1, NULL,\n" +
		"\t\t\t\t\t\t   &sessionname,\n" +
		"\t\t\t\t\t\t   NULL, NULL, 0);"

	appNameReplacement := "\t\t/*\n" +
		"\t\t * multigres patch: build the application_name literal client-side\n" +
		"\t\t * and send it via the simple protocol. The multigateway planner\n" +
		"\t\t * rejects set_config() when the value is a non-literal expression\n" +
		"\t\t * or bound parameter (the pooler tracks the literal value), so we\n" +
		"\t\t * cannot use PQexecParams + current_setting() here.\n" +
		"\t\t */\n" +
		"\t\t{\n" +
		"\t\t\tconst char *appname_prefix = getenv(\"PGAPPNAME\");\n" +
		"\t\t\tchar\t   *combined;\n" +
		"\t\t\tchar\t   *escaped;\n" +
		"\t\t\tchar\t   *appname_query;\n" +
		"\n" +
		"\t\t\tif (appname_prefix == NULL)\n" +
		"\t\t\t\tappname_prefix = \"\";\n" +
		"\t\t\tcombined = psprintf(\"%s/%s\", appname_prefix, sessionname);\n" +
		"\t\t\tescaped = PQescapeLiteral(conns[i].conn, combined, strlen(combined));\n" +
		"\t\t\tfree(combined);\n" +
		"\t\t\tif (escaped == NULL)\n" +
		"\t\t\t{\n" +
		"\t\t\t\tfprintf(stderr, \"PQescapeLiteral failed: %s\",\n" +
		"\t\t\t\t\t\tPQerrorMessage(conns[i].conn));\n" +
		"\t\t\t\texit(1);\n" +
		"\t\t\t}\n" +
		"\t\t\tappname_query = psprintf(\n" +
		"\t\t\t\t\"SELECT set_config('application_name', %s, false)\", escaped);\n" +
		"\t\t\tPQfreemem(escaped);\n" +
		"\t\t\tres = PQexec(conns[i].conn, appname_query);\n" +
		"\t\t\tfree(appname_query);\n" +
		"\t\t}"

	if !bytes.Contains(src, []byte(appNameOrig)) {
		return fmt.Errorf("%s: original set_config block not found (PG version drift?)", rel)
	}
	patched := bytes.Replace(src, []byte(appNameOrig), []byte(appNameReplacement), 1)

	// Add explicit type casts on both args. isolationtester PQprepares the
	// wait-query with paramTypes=NULL so $1 enters parse as UNKNOWN, and the
	// '{...}' literal is also UNKNOWN. PG resolves this for the pg_catalog
	// C builtin via implicit catalog priority but fails for a public
	// PL/pgSQL function with "function public.X(unknown, unknown) does not
	// exist". Casting to int4 / int4[] removes the ambiguity.
	waitFnOrig := "\"SELECT pg_catalog.pg_isolation_test_session_is_blocked($1, '{\""
	waitFnReplacement := "\"SELECT public.multigres_test_session_is_blocked($1::int4, '{\""
	if !bytes.Contains(patched, []byte(waitFnOrig)) {
		return fmt.Errorf("%s: wait-query function reference not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(waitFnOrig), []byte(waitFnReplacement), 1)

	waitFnSuffixOrig := "appendPQExpBufferStr(&wait_query, \"}')\");"
	waitFnSuffixReplacement := "appendPQExpBufferStr(&wait_query, \"}'::int4[])\");"
	if !bytes.Contains(patched, []byte(waitFnSuffixOrig)) {
		return fmt.Errorf("%s: wait-query suffix not found (PG version drift?)", rel)
	}
	patched = bytes.Replace(patched, []byte(waitFnSuffixOrig), []byte(waitFnSuffixReplacement), 1)

	if err := os.WriteFile(abs, patched, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", abs, err)
	}
	t.Logf("Patched %s: literal application_name + public.multigres_test_session_is_blocked", rel)
	return nil
}

// BuildIsolation builds the PostgreSQL isolation test tools (isolationtester and
// pg_isolation_regress). Must be called after Build().
func (pb *PostgresBuilder) BuildIsolation(t *testing.T, ctx context.Context) error {
	t.Helper()

	if err := pb.patchIsolationtester(t, ctx); err != nil {
		return fmt.Errorf("patch isolationtester: %w", err)
	}

	isolationDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")

	t.Logf("Building isolation test tools in %s...", isolationDir)
	cmd := executil.Command(ctx, "make", "-C", isolationDir, "all")
	cmd.Cmd.Stdout = os.Stdout
	cmd.Cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("make isolation tools failed: %w", err)
	}

	t.Logf("Isolation test tools built successfully")
	return nil
}

// truncateForLog clips s to at most n characters (with an ellipsis suffix when
// truncation occurs). Used for compact log/error messages.
func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// installPIDMappingFunction creates public.multigres_test_session_is_blocked
// in the target database so the patched isolationtester (see
// patchIsolationtester) can probe lock waits through the multigateway →
// multipooler → PostgreSQL hop.
//
// Multipooler is configured with --database=postgres and routes every
// query to a pooled connection against the postgres DB regardless of the
// dbname in the client startup packet, so the shim must live in postgres.
// Both isolation invocation paths (selective via PGISOLATION_TESTS and
// full-suite via the make installcheck target) force --dbname=postgres on
// pg_isolation_regress, so postgres is also the dbname the harness opens.
//
// The shim mirrors the upstream builtin: returns true if check_pid is
// waiting on any pid in blocked_by, considering both heavyweight lock
// waits (pg_blocking_pids) and SSI safe-snapshot waits
// (pg_safe_snapshot_blocking_pids — required for SERIALIZABLE READ ONLY
// DEFERRABLE specs such as read-only-anomaly-3). Both inputs are
// multigateway virtual pids; we map them to real PostgreSQL backend pids
// via pg_stat_activity.application_name (the multipooler stamps each
// backend with `multigres_vpid:<id>` per query). A given vpid can map to
// multiple PG backends in flight (a leftover stamp on a pool conn after
// a regular query, plus the live reserved conn) so the wait-check
// aggregates over every matching backend rather than picking one
// non-deterministically.
func (pb *PostgresBuilder) installPIDMappingFunction(t *testing.T, pgPort int, password string) error {
	t.Helper()
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		pgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	stmts := []string{
		// Debug table: every shim invocation logs its inputs/outputs and a
		// snapshot of every backend in this DB so failures can be diagnosed
		// post-hoc by querying isolation_debug_log (see
		// dumpIsolationDebugLog).
		`CREATE TABLE IF NOT EXISTS public.isolation_debug_log (
			id serial PRIMARY KEY,
			ts timestamptz DEFAULT now(),
			check_pid int4,
			blocked_by int4[],
			real_check_pid int4,
			real_blocked_by int4[],
			blocking_pids int4[],
			vpid_entries text[],
			all_pg_backends text[],
			result boolean
		)`,
		// Non-destructive add for runs against a pre-existing table from an
		// earlier shim version that lacked all_pg_backends.
		`ALTER TABLE public.isolation_debug_log
		   ADD COLUMN IF NOT EXISTS all_pg_backends text[]`,
		`TRUNCATE public.isolation_debug_log`,
		`DROP FUNCTION IF EXISTS public.multigres_test_session_is_blocked(int4, int4[])`,
		`CREATE FUNCTION public.multigres_test_session_is_blocked(check_pid int4, blocked_by int4[])
RETURNS boolean
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
<<fn>>
DECLARE
    v_log_id int4;
    v_real_check_pid int4;
    v_real_blocked_by int4[];
    v_blocking_pids int4[];
    v_vpid_entries text[];
    v_all_backends text[];
    v_stamp_found boolean;
    v_result boolean;
BEGIN
    -- Capture the inserted id directly so the later UPDATE targets THIS
    -- invocation's row even under concurrent shim calls (parallel groups
    -- in the isolation schedule, or multiple sessions polling at once).
    -- max(id) would race against any concurrent INSERT in between.
    INSERT INTO public.isolation_debug_log
        (check_pid, blocked_by, result)
    VALUES
        (check_pid, blocked_by, NULL)
    RETURNING id INTO v_log_id;

    SELECT array_agg(sa.application_name || '=' || sa.pid)
    INTO v_vpid_entries
    FROM pg_stat_activity sa
    WHERE sa.application_name LIKE 'multigres_vpid:%';

    SELECT array_agg(sa.pid || ':' || COALESCE(sa.application_name,'<null>') || ':' || COALESCE(sa.state,'<null>'))
    INTO v_all_backends
    FROM pg_stat_activity sa
    WHERE sa.datname = current_database();

    -- A client vpid can map to multiple PG backends (a leftover stamp on
    -- a pool conn after the client ran a regular query, plus the live
    -- reserved conn). Picking one non-deterministically risks probing the
    -- idle one and missing the wait. real_check_pid is kept for
    -- diagnostic display only; the actual block check below aggregates
    -- over every matching backend.
    SELECT sa.pid INTO v_real_check_pid
    FROM pg_stat_activity sa
    WHERE sa.application_name = 'multigres_vpid:' || check_pid
    LIMIT 1;

    SELECT array_agg(sa.pid) INTO v_real_blocked_by
    FROM pg_stat_activity sa
    WHERE sa.application_name = ANY(
        SELECT 'multigres_vpid:' || unnest(blocked_by)
    );

    -- Direct connections (no multigateway) hand us real pids; preserve them.
    v_stamp_found := v_real_check_pid IS NOT NULL;
    v_real_check_pid := COALESCE(v_real_check_pid, check_pid);
    v_real_blocked_by := COALESCE(v_real_blocked_by, blocked_by);

    -- Aggregate heavyweight lock blockers and SSI safe-snapshot blockers
    -- across every PG backend currently stamped for this vpid. SSI
    -- safe-snapshot wait is required for SERIALIZABLE READ ONLY
    -- DEFERRABLE specs (e.g. read-only-anomaly-3). Aggregation handles
    -- the duplicate-stamp case where one backend is the live reserved
    -- conn (potentially blocked) and another is a leaked pool conn
    -- (idle).
    --
    -- The direct-pid fallback only fires when no stamp was found for
    -- check_pid. vpids occupy the full 31-bit signed int32 space, so a
    -- vpid value can coincidentally equal an unrelated real PG backend
    -- PID; probing check_pid as a real pid unconditionally would surface
    -- that unrelated backend's blockers and risk a false positive.
    SELECT COALESCE(array_agg(DISTINCT b), '{}'::int4[]) INTO v_blocking_pids
    FROM (
        SELECT unnest(pg_blocking_pids(sa.pid)) AS b
        FROM pg_stat_activity sa
        WHERE sa.application_name = 'multigres_vpid:' || check_pid
        UNION ALL
        SELECT unnest(pg_safe_snapshot_blocking_pids(sa.pid)) AS b
        FROM pg_stat_activity sa
        WHERE sa.application_name = 'multigres_vpid:' || check_pid
        UNION ALL
        SELECT unnest(pg_blocking_pids(check_pid)) AS b WHERE NOT v_stamp_found
        UNION ALL
        SELECT unnest(pg_safe_snapshot_blocking_pids(check_pid)) AS b WHERE NOT v_stamp_found
    ) sub
    WHERE b IS NOT NULL;

    v_result := v_blocking_pids && v_real_blocked_by;

    UPDATE public.isolation_debug_log
    SET real_check_pid = v_real_check_pid,
        real_blocked_by = v_real_blocked_by,
        blocking_pids = v_blocking_pids,
        vpid_entries = v_vpid_entries,
        all_pg_backends = v_all_backends,
        result = v_result
    WHERE id = v_log_id;

    RETURN v_result;
END fn;
$$`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute statement [%s]: %w", truncateForLog(stmt, 80), err)
		}
	}

	// Sanity check: the function exists and is plpgsql.
	var lang string
	if err := db.QueryRow(`
		SELECT l.lanname
		FROM pg_proc p JOIN pg_language l ON p.prolang = l.oid
		WHERE p.proname = 'multigres_test_session_is_blocked'
		  AND p.pronamespace = 'public'::regnamespace`).Scan(&lang); err != nil {
		return fmt.Errorf("verify multigres_test_session_is_blocked: %w", err)
	}
	if lang != "plpgsql" {
		return fmt.Errorf("multigres_test_session_is_blocked installed with lanname=%q (expected plpgsql)", lang)
	}

	t.Logf("Installed public.multigres_test_session_is_blocked on database \"postgres\"")
	return nil
}

// dumpIsolationDebugLog prints recent entries from
// public.isolation_debug_log so investigators can see the inputs/outputs
// of every shim invocation during the isolation run. Best-effort;
// failures are logged and ignored.
func (pb *PostgresBuilder) dumpIsolationDebugLog(t *testing.T, pgPort int, password string) {
	t.Helper()
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		pgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Logf("isolation_debug_log dump: connect failed: %v", err)
		return
	}
	defer db.Close()

	var total int
	if err := db.QueryRow(`SELECT count(*) FROM public.isolation_debug_log WHERE check_pid > 0`).Scan(&total); err != nil {
		t.Logf("isolation_debug_log dump: count failed: %v", err)
		return
	}
	t.Logf("isolation_debug_log: %d shim invocations recorded during run", total)
	if total == 0 {
		t.Logf("isolation_debug_log: shim never executed — wait-query is not reaching public.multigres_test_session_is_blocked")
		return
	}

	rows, err := db.Query(`
		SELECT id, check_pid, blocked_by, real_check_pid, real_blocked_by,
		       blocking_pids, vpid_entries, all_pg_backends, result
		FROM public.isolation_debug_log
		WHERE check_pid > 0
		ORDER BY id DESC
		LIMIT 30`)
	if err != nil {
		t.Logf("isolation_debug_log dump: select failed: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, checkPid int
		var blockedBy, realBlockedBy, blockingPids, vpidEntries, allBackends sql.NullString
		var realCheckPid sql.NullInt32
		var result sql.NullBool
		if err := rows.Scan(&id, &checkPid, &blockedBy, &realCheckPid, &realBlockedBy, &blockingPids, &vpidEntries, &allBackends, &result); err != nil {
			t.Logf("isolation_debug_log dump: scan failed: %v", err)
			continue
		}
		t.Logf("isolation_debug_log id=%d check_pid=%d blocked_by=%s real_check=%v real_blocked=%s blocking=%s vpids=%s all=%s result=%v",
			id, checkPid, blockedBy.String, realCheckPid, realBlockedBy.String, blockingPids.String, vpidEntries.String, allBackends.String, result)
	}
}

// RunIsolationTests runs PostgreSQL isolation tests against multigateway.
// Isolation tests exercise multi-connection concurrency (deadlocks, serialization
// anomalies, lock contention, concurrent DDL) using isolationtester.
//
// directPgPort is the primary's direct PostgreSQL port; it's used to install
// the public.multigres_test_session_is_blocked shim that the patched
// isolationtester binary calls (see patchIsolationtester for the source-side
// rewrite that retargets the wait query at the public function).
//
// The isolation Makefile has no installcheck-tests target, so for selective tests
// we invoke pg_isolation_regress directly with test names as positional args.
func (pb *PostgresBuilder) RunIsolationTests(t *testing.T, ctx context.Context, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	t.Logf("Running PostgreSQL isolation tests against multigateway on port %d (harness db=postgres)...", multigatewayPort)

	// Install the lock-detection shim on PostgreSQL directly (bypassing
	// multigateway). Both the selective (PGISOLATION_TESTS) and full-suite
	// paths force --dbname=postgres on pg_isolation_regress (see the cmd
	// construction below), and multipooler routes every query to the
	// postgres DB anyway, so the shim only needs to live there.
	if err := pb.installPIDMappingFunction(t, directPgPort, password); err != nil {
		t.Logf("Warning: Failed to install PID mapping function: %v", err)
		t.Logf("Isolation tests that rely on lock detection (deadlock, etc.) may fail")
	}

	isolationBuildDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")
	isolationSourceDir := filepath.Join(pb.SourceDir, "src", "test", "isolation")
	outputIsoDir := filepath.Join(isolationBuildDir, "output_iso")

	if err := os.MkdirAll(outputIsoDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output_iso directory: %w", err)
	}

	pgIsoRegress := filepath.Join(isolationBuildDir, "pg_isolation_regress")
	if _, err := os.Stat(pgIsoRegress); os.IsNotExist(err) {
		t.Logf("Building pg_isolation_regress...")
		buildCmd := executil.Command(ctx, "make", "-C", isolationBuildDir, "all")
		if out, err := buildCmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("failed to build pg_isolation_regress: %w\n%s", err, out)
		}
	}

	var cmd *executil.Cmd
	if testsEnv := os.Getenv("PGISOLATION_TESTS"); testsEnv != "" {
		args := []string{
			"--inputdir=" + isolationSourceDir,
			"--outputdir=" + outputIsoDir,
			"--host=localhost",
			fmt.Sprintf("--port=%d", multigatewayPort),
			"--user=postgres",
			"--dbname=postgres",
			"--use-existing",
			"--dlpath=" + isolationBuildDir,
		}
		args = append(args, strings.Fields(testsEnv)...)
		cmd = executil.Command(ctx, pgIsoRegress, args...).WithProcessGroup()
		t.Logf("Running selective isolation tests: %s", testsEnv)
	} else {
		cmd = executil.Command(ctx, "make", "-C", isolationBuildDir, "installcheck",
			"EXTRA_REGRESS_OPTS=--use-existing --dbname=postgres").WithProcessGroup()
		t.Logf("Running full PostgreSQL isolation test suite (installcheck)")
	}

	results, runErr := pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
		suiteName: "Isolation",
		outputDir: filepath.Join(pb.OutputDir, "isolation"),
		srcOutDir: outputIsoDir,
	}, multigatewayPort, password)

	// Post-suite diagnostic: dump the last entries of isolation_debug_log
	// so investigators can see what the shim observed (or didn't) for
	// hung specs. The table lives in the postgres DB on the primary;
	// query it directly to bypass any multigateway routing that a
	// failing wait-query would have used.
	pb.dumpIsolationDebugLog(t, directPgPort, password)

	return results, runErr
}
