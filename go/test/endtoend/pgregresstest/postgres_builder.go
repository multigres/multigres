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

// RunExternalTests runs each external extension's shipped test suite against
// multigateway and returns one merged TestResults across all of them.
//
// External (KindExternal) extensions are PGXS modules that live outside the
// PostgreSQL source tree (see Builder.InstallExternalExtension, which has already
// cloned and installed each one — and any DependsOn build dependencies — before
// this is called). Two test harnesses are supported, selected per extension by
// ExternalExtension.Harness:
//
//   - pg_regress (default, e.g. pgvector/pg_cron): drives the pg_regress binary
//     and diffs against expected/*.out via the patch pipeline. See
//     runExternalRegress.
//   - pgTAP (HarnessPgTAP, e.g. pg_partman): feeds each test .sql to psql and
//     parses the TAP stream the assertions emit server-side; no expected-output
//     files, no patch pipeline. See runExternalPgTAP.
//
// Per-test names are prefixed with the extension name ("vector/btree",
// "pg_partman/test-id-10") to stay unique in the merged report.
//
// All extensions share the single postgres database (multigateway can't isolate
// per-DB), so before each one we reset the public schema directly on the primary
// (directPgPort, bypassing the gateway's DDL block) to clear objects a prior
// extension left behind, and front-load/drop any ScratchDatabases. Those shared
// setup steps live here; only the run-and-verify step differs per harness.
func (pb *PostgresBuilder) RunExternalTests(t *testing.T, ctx context.Context, exts []ExternalExtension, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	pgBinDir := pb.BinDir()

	// Ensure the pg_regress we drive directly is built — but only if a
	// regress-harness extension will actually use it. A pgTAP-only run (e.g.
	// PGEXTERNAL_TESTS=pg_partman) drives psql directly and needs no pg_regress.
	needRegress := false
	for _, ext := range exts {
		if ext.Harness == HarnessPgRegress {
			needRegress = true
			break
		}
	}
	if needRegress {
		regressDir := filepath.Join(pb.BuildDir, "src", "test", "regress")
		if !suiteutil.FileExists(filepath.Join(regressDir, "pg_regress")) {
			if out, err := executil.Command(ctx, "make", "-C", regressDir, "all").CombinedOutput(); err != nil {
				return nil, fmt.Errorf("failed to build pg_regress: %w\n%s", err, truncateForLog(string(out), 2000))
			}
		}
	}

	merged := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}

	for _, ext := range exts {
		cloneDir := filepath.Join(pb.ExternalDir, ext.Name)
		// Fixtures live under ext.TestSubdir within the checkout (pgvector: test/;
		// pg_cron: the repo root, i.e. "."; pg_partman: test/). filepath.Join
		// collapses "." back to the clone root.
		testDir := filepath.Join(cloneDir, ext.TestSubdir)

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

		var (
			res *TestResults
			err error
		)
		switch ext.Harness {
		case HarnessPgTAP:
			res, err = pb.runExternalPgTAP(t, ctx, ext, testDir, pgBinDir, multigatewayPort, directPgPort, password)
		case HarnessPgRegress:
			res, err = pb.runExternalRegress(t, ctx, ext, cloneDir, testDir, pgBinDir, multigatewayPort, password)
		default:
			err = fmt.Errorf("external/%s: unknown test harness %q", ext.Name, ext.Harness)
		}

		// Drop the scratch databases now the suite has run (verification reads
		// result files, not the live DB). WITH (FORCE) in case a launcher opened a
		// connection. Best-effort; the cluster is torn down after anyway.
		for _, db := range ext.ScratchDatabases {
			if err := execOnPrimary(directPgPort, password, fmt.Sprintf("DROP DATABASE IF EXISTS %q WITH (FORCE)", db)); err != nil {
				t.Logf("external/%s: warning: drop scratch db %q failed: %v", ext.Name, db, err)
			}
		}

		if res == nil {
			t.Logf("external/%s: no test results (%v)", ext.Name, err)
			continue
		}

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

// createExtensionWithSchema creates an extension on the given port, optionally
// into a target schema (which it creates first). With an empty schema it emits a
// plain CREATE EXTENSION IF NOT EXISTS; with a schema it adds SCHEMA <schema> —
// needed for extensions whose control file pins no schema (relocatable=false, no
// `schema=`) but whose tests expect a specific one: pg_partman must live in
// `partman` or its tests' schema-qualified partman.* references fail with "schema
// partman does not exist". Names come from the controlled extension catalog, not
// user input, but are quoted as identifiers defensively all the same.
//
// Both the pg_regress and pgTAP preload paths (ExternalExtension.PreCreateExtensions)
// use this. Routing CREATE EXTENSION through multigateway (rather than the primary)
// keeps setup on the same pooled path the test queries take — no
// create-on-primary-then-read-from-standby replication race — and doubles as real
// coverage that CREATE EXTENSION works over the gateway. It is the preload the
// external suite needs because pg_regress's own --load-extension only fires inside
// create_database(), which it skips under --use-existing (multigateway rejects
// CREATE/DROP DATABASE); fixtures that assume the extension exists (pgvector opens
// with a bare CREATE TABLE t (val vector(3))) would otherwise fail.
func createExtensionWithSchema(port int, password, ext, schema string) error {
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		port, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	stmt := fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %q`, ext)
	if schema != "" {
		if _, err := db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %q`, schema)); err != nil {
			return fmt.Errorf("create schema %q: %w", schema, err)
		}
		stmt = fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS %q SCHEMA %q`, ext, schema)
	}
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

// truncateForLog clips s to at most n characters (with an ellipsis suffix when
// truncation occurs). Used for compact log/error messages.
func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
