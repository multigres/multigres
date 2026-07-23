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
	"slices"
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
		"PGPASSWORD="+password,
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
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

// ExternalModules returns the external extensions to verify, defaulting to the
// runnable set (covered/partial upstream suites plus build-only smoke checks).
// PGEXTERNAL_TESTS (space-separated catalog names, e.g. "vector") selects a
// subset for local iteration.
func ExternalModules() []ExternalExtension {
	all := RunnableExternalExtensions()
	sel := strings.Fields(os.Getenv("PGEXTERNAL_TESTS"))
	if len(sel) == 0 {
		return all
	}
	want := make(map[string]bool, len(sel))
	for _, s := range sel {
		want[s] = true
	}
	var out []ExternalExtension
	seen := map[string]bool{}
	add := func(e ExternalExtension) {
		if seen[e.Name] {
			return
		}
		seen[e.Name] = true
		out = append(out, e)
	}
	for _, e := range all {
		if want[e.Name] {
			add(e)
		}
	}
	for name := range want {
		spec, ok := externalSpecs[name]
		if !ok || spec.TestRunner != "postgis-alias" {
			continue
		}
		if postgis, ok := externalSpecs["postgis"]; ok {
			add(postgis)
		}
	}
	return out
}

// RunExternalTests runs each external extension verification against
// multigateway and returns one merged TestResults across all of them.
//
// External (KindExternal) extensions are PGXS modules that live outside the
// PostgreSQL source tree (see Builder.InstallExternalExtension, which has already
// cloned and installed each one — and any DependsOn build dependencies — before
// this is called). Test harnesses are selected per extension by
// ExternalExtension.Harness:
//
//   - pg_regress (default, e.g. pgvector/pg_cron): drives the pg_regress binary
//     and diffs against expected/*.out via the patch pipeline. Partial
//     extensions use this same path, with known compatibility gaps documented
//     by narrow patches. See runExternalRegress.
//   - pgTAP (HarnessPgTAP, e.g. pg_partman): feeds each test .sql to psql and
//     parses the TAP stream the assertions emit server-side; no expected-output
//     files, no patch pipeline. See runExternalPgTAP.
//   - smoke (HarnessSmoke, e.g. pgaudit): CREATE EXTENSION only, used when the
//     upstream regression suite is not a valid multigateway compatibility
//     signal.
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
		if ext.Harness == HarnessPgRegress && ext.TestRunner == "" {
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
		if ext.TestRunner == "postgis" {
			res, err := pb.runPostGISTests(t, ctx, ext, multigatewayPort, directPgPort, password)
			if res == nil {
				merged.TotalTests = merged.PassedTests + merged.FailedTests + merged.SkippedTests
				if err != nil {
					return merged, fmt.Errorf("external/%s: %w", ext.Name, err)
				}
				return merged, fmt.Errorf("external/%s: no test results", ext.Name)
			}
			if res.TotalTests == 0 {
				merged.TotalTests = merged.PassedTests + merged.FailedTests + merged.SkippedTests
				if err != nil {
					return merged, fmt.Errorf("external/%s: no tests executed: %w", ext.Name, err)
				}
				return merged, fmt.Errorf("external/%s: no tests executed", ext.Name)
			}
			if err != nil && !hasPostGISRunnerResults(res) {
				merged.TotalTests = merged.PassedTests + merged.FailedTests + merged.SkippedTests
				return merged, fmt.Errorf("external/%s: no PostGIS regress tests executed: %w", ext.Name, err)
			}
			mergeExternalResults(merged, res)
			if err != nil {
				t.Logf("external/%s: runner exited with error after producing %d result(s): %v", ext.Name, res.TotalTests, err)
			}
			continue
		}
		if ext.TestRunner == "postgis-alias" {
			continue
		}

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
		case HarnessSmoke:
			res, err = pb.runExternalSmoke(t, ext, multigatewayPort, password)
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

// runExternalSmoke verifies a build-only extension can be loaded through
// multigateway. It intentionally does not run upstream regression fixtures.
func (pb *PostgresBuilder) runExternalSmoke(t *testing.T, ext ExternalExtension, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	start := time.Now()
	installs := ext.PreCreateExtensions
	if len(installs) == 0 {
		installs = []ExtensionInstall{{Name: ext.Name}}
	}

	for _, install := range installs {
		if loadErr := createExtensionWithSchema(multigatewayPort, password, install.Name, install.Schema); loadErr != nil {
			return &TestResults{
				TotalTests:  1,
				FailedTests: 1,
				Duration:    time.Since(start),
				Tests: []IndividualTestResult{{
					Name:       "load",
					Status:     "fail",
					FailReason: loadErr.Error(),
				}},
			}, loadErr
		}
	}

	return &TestResults{
		TotalTests:  1,
		PassedTests: 1,
		Duration:    time.Since(start),
		Tests: []IndividualTestResult{{
			Name:   "load",
			Status: "pass",
		}},
	}, nil
}

func hasPostGISRunnerResults(res *TestResults) bool {
	if res == nil {
		return false
	}
	for _, tr := range res.Tests {
		if !strings.HasSuffix(tr.Name, "/create_extension") {
			return true
		}
	}
	return false
}

func mergeExternalResults(dst, src *TestResults) {
	for _, tr := range src.Tests {
		dst.Tests = append(dst.Tests, tr)
		switch tr.Status {
		case "fail":
			dst.FailedTests++
			detail := tr.FailReason
			if detail == "" {
				detail = "see external test artifacts"
			}
			dst.FailureDetails = append(dst.FailureDetails, TestFailure{
				TestName: tr.Name,
				Error:    detail,
			})
		case "skip":
			dst.SkippedTests++
		default:
			dst.PassedTests++
		}
	}
	dst.TimedOut = dst.TimedOut || src.TimedOut
}

type postGISComponent struct {
	Extension string
	Module    string
	Flag      string
	Required  bool
}

var postGISComponents = []postGISComponent{
	{Extension: "postgis", Module: "postgis", Required: true},
	{Extension: "postgis_topology", Module: "postgis_topology", Flag: "--topology"},
	{Extension: "postgis_raster", Module: "postgis_raster", Flag: "--raster"},
	{Extension: "postgis_sfcgal", Module: "postgis_sfcgal", Flag: "--sfcgal"},
}

// postGISTestSources maps each component module to the directory (relative to
// the checkout root) that holds its regress Makefile + tests.mk. Used both for
// test discovery (make -n check) and for generating the Make-prerequisite
// fixtures before the runner.
var postGISTestSources = []struct {
	module string
	dir    string
}{
	{module: "postgis", dir: "regress"},
	{module: "postgis_topology", dir: filepath.Join("topology", "test")},
	{module: "postgis_raster", dir: filepath.Join("raster", "test", "regress")},
	{module: "postgis_sfcgal", dir: filepath.Join("sfcgal", "regress")},
}

// generatePostGISFixtures builds the regress fixtures PostGIS generates as Make
// prerequisites of `make check` but NOT as part of `make all`/`make install`.
// Topology is the notable case: its tests open with
//
//	\i :top_builddir/topology/test/load_topology.sql
//
// and load_topology.sql / load_topology-4326.sql / load_large_topology.sql /
// topo_predicates.sql are produced by cpp from *.in templates in the Makefile's
// check-regress-deps target. Because the harness drives regress/run_test.pl
// directly (it cannot use `make check`; see runPostGISTests), those prerequisites
// are never built, so the \i fails with "No such file or directory", the
// city_data topology never loads, and every downstream city_data.* reference
// errors out. Running check-regress-deps here closes that gap. Components whose
// Makefile has no such target (the build already generated their fixtures, e.g.
// raster's rtpostgis.sql) are skipped.
func (pb *PostgresBuilder) generatePostGISFixtures(ctx context.Context, t *testing.T, cloneDir string, enabled map[string]bool) error {
	for _, src := range postGISTestSources {
		if !enabled[src.module] {
			continue
		}
		dir := filepath.Join(cloneDir, src.dir)
		raw, err := os.ReadFile(filepath.Join(dir, "Makefile"))
		if err != nil {
			continue
		}
		if !strings.Contains(string(raw), "check-regress-deps:") {
			continue
		}
		t.Logf("Generating PostGIS %s regress fixtures (make check-regress-deps)...", src.module)
		if out, err := executil.Command(ctx, "make", "-C", dir, "check-regress-deps").CombinedOutput(); err != nil {
			return fmt.Errorf("postgis %s fixture generation failed: %w\n%s", src.module, err, truncateForLog(string(out), 2000))
		}
	}
	return nil
}

func (pb *PostgresBuilder) runPostGISTests(t *testing.T, ctx context.Context, ext ExternalExtension, multigatewayPort, directPgPort int, password string) (*TestResults, error) {
	t.Helper()

	cloneDir := filepath.Join(pb.ExternalDir, ext.Name)
	outputDir := filepath.Join(pb.OutputDir, "external", ext.Name)
	tmpDir := filepath.Join(outputDir, "tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir postgis tmp: %w", err)
	}

	if err := resetContribState(directPgPort, password); err != nil {
		t.Logf("external/%s: warning: public schema reset failed: %v", ext.Name, err)
	}
	if err := patchPostGISRunnerDatabase(filepath.Join(cloneDir, "regress", "run_test.pl")); err != nil {
		return nil, err
	}

	// Pre-install the PostGIS components directly on the primary (directPgPort),
	// NOT through the gateway. CREATE EXTENSION postgis_topology runs
	// `ALTER DATABASE <db> SET search_path = ..., topology` as part of its install
	// script; routed through the gateway that change would land on one pooled
	// backend while the others keep the old default, so later statements scattered
	// across the pool wouldn't see the `topology` schema. Executing it on the
	// primary bakes the new per-database search_path default into
	// pg_db_role_setting; after the install we terminate any already-open client
	// backends so multipooler reconnects and every backend used by run_test.pl is
	// born with the topology-aware default. This keeps the primary-preinstall
	// approach reliable even in the full external suite, where earlier extensions
	// or setup readiness probes may have warmed the pool before PostGIS runs.
	// (This replaces the connection-defaults pool-refresh mechanism for the
	// PostGIS path.)
	enabled := map[string]bool{}
	synthetic := []IndividualTestResult{}
	for _, comp := range postGISComponents {
		if !extensionControlExists(pb.InstallDir, comp.Extension) {
			if comp.Required {
				return nil, fmt.Errorf("%s control file not installed", comp.Extension)
			}
			continue
		}
		if err := createExtensionWithSchema(directPgPort, password, comp.Extension, ""); err != nil {
			tr := IndividualTestResult{
				Name:       comp.Module + "/create_extension",
				Status:     "fail",
				FailReason: err.Error(),
			}
			synthetic = append(synthetic, tr)
			if comp.Required {
				return testResultsFromSynthetic(synthetic), err
			}
			continue
		}
		enabled[comp.Module] = true
	}
	if terminated, err := terminateDatabaseClientBackends(directPgPort, password); err != nil {
		return testResultsFromSynthetic(synthetic), fmt.Errorf("refresh PostGIS pooled backends: %w", err)
	} else if terminated > 0 {
		t.Logf("external/%s: terminated %d existing postgres client backend(s) after PostGIS pre-install", ext.Name, terminated)
	}

	// Build the fixtures PostGIS generates as `make check` prerequisites (topology
	// load_*.sql, topo_predicates.sql) before the runner \i's them.
	if err := pb.generatePostGISFixtures(ctx, t, cloneDir, enabled); err != nil {
		return testResultsFromSynthetic(synthetic), err
	}

	tests, err := listPostGISTests(ctx, cloneDir, enabled)
	if err != nil {
		return nil, err
	}
	if len(tests) == 0 {
		return testResultsFromSynthetic(synthetic), errors.New("no PostGIS regress tests found")
	}

	args := []string{
		filepath.Join("regress", "run_test.pl"),
		"--nocreate",
		"--nodrop",
		"--extensions",
	}
	for _, comp := range postGISComponents {
		if comp.Flag != "" && enabled[comp.Module] {
			args = append(args, comp.Flag)
		}
	}
	args = append(args, tests...)

	t.Logf("Running external/%s run_test.pl (%d tests) against multigateway...", ext.Name, len(tests))
	cmd := executil.Command(ctx, "perl", args...).WithProcessGroup().SetDir(cloneDir)
	cmd.SetWaitDelay(10 * time.Second)
	cmd.AddEnv(
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
		"PGPASSWORD="+password,
		"PGDATABASE=postgres",
		"PGCONNECT_TIMEOUT=10",
		"POSTGIS_REGRESS_DB=postgres",
		"POSTGIS_TOP_BUILD_DIR="+cloneDir,
		"PGIS_REG_TMPDIR="+tmpDir,
	)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	startTime := time.Now()
	runErr := cmd.Run()
	duration := time.Since(startTime)

	outData := stdoutBuf.Bytes()
	if err := os.WriteFile(filepath.Join(outputDir, "regression.out"), outData, 0o644); err != nil {
		t.Logf("external/%s: warning: write regression.out failed: %v", ext.Name, err)
	}
	if stderrBuf.Len() > 0 {
		if err := os.WriteFile(filepath.Join(outputDir, "regression.stderr"), stderrBuf.Bytes(), 0o644); err != nil {
			t.Logf("external/%s: warning: write regression.stderr failed: %v", ext.Name, err)
		}
	}

	res, parseErr := parsePostGISResults(string(outData))
	if parseErr != nil {
		if runErr != nil {
			return testResultsFromSynthetic(synthetic), fmt.Errorf("postgis runner failed: %w; parse output: %w", runErr, parseErr)
		}
		return testResultsFromSynthetic(synthetic), parseErr
	}
	res.Duration = duration
	res.TimedOut = ctx.Err() == context.DeadlineExceeded

	verifyPostGISResults(ctx, cloneDir, tmpDir, res, GetPatchMode())
	res.Tests = append(synthetic, res.Tests...)
	recountResults(res)
	if runErr != nil {
		return res, runErr
	}
	return res, nil
}

func extensionControlExists(installDir, name string) bool {
	for _, dir := range []string{
		filepath.Join(installDir, "share", "postgresql", "extension"),
		filepath.Join(installDir, "share", "extension"),
	} {
		if suiteutil.FileExists(filepath.Join(dir, name+".control")) {
			return true
		}
	}
	return false
}

func patchPostGISRunnerDatabase(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read postgis runner %q: %w", path, err)
	}
	patched := strings.ReplaceAll(string(raw), " template1", " postgres")
	patched = strings.ReplaceAll(patched,
		`    sql("ALTER DATABASE \"$DB\" SET test.executor_slow_factor = $test_executor_slow_factor");`,
		`    # Multigres runs PostGIS against an existing database through the gateway; ALTER DATABASE is intentionally blocked.`,
	)
	if patched == string(raw) {
		return nil
	}
	if err := os.WriteFile(path, []byte(patched), 0o755); err != nil {
		return fmt.Errorf("patch postgis runner %q: %w", path, err)
	}
	return nil
}

func listPostGISTests(ctx context.Context, cloneDir string, enabled map[string]bool) ([]string, error) {
	// Let PostGIS's generated Makefiles decide the regress list. The manifests are
	// make programs, not data files: they contain feature conditionals, ordering
	// constraints (for example raster cleanup tests that must run last), slow-test
	// exclusions, and runner hook paths in RUNTESTFLAGS_INTERNAL. A dry-run of the
	// component check targets gives us the fully-expanded run_test.pl command lines
	// without reimplementing Make in Go.
	var tests []string
	seen := map[string]bool{}
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		tests = append(tests, name)
	}

	for _, src := range postGISTestSources {
		if !enabled[src.module] {
			continue
		}
		dir := filepath.Join(cloneDir, src.dir)
		if !suiteutil.FileExists(filepath.Join(dir, "Makefile")) {
			return nil, fmt.Errorf("PostGIS %s tests enabled but %s/Makefile is missing", src.module, src.dir)
		}
		cmd := executil.Command(ctx, "make", "-C", dir, "-n", "check")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("PostGIS %s test discovery failed: %w\n%s", src.module, err, truncateForLog(string(out), 2000))
		}
		names := parsePostGISMakeDryRunTests(cloneDir, src.dir, string(out))
		if len(names) == 0 {
			return nil, fmt.Errorf("PostGIS %s tests enabled but make dry-run produced no regress tests", src.module)
		}
		for _, name := range names {
			add(name)
		}
	}
	return tests, nil
}

func parsePostGISMakeDryRunTests(cloneDir, makeDir, output string) []string {
	var tests []string
	seen := map[string]bool{}
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		tests = append(tests, name)
	}

	// Make may print long run_test.pl invocations with shell continuations.
	output = strings.ReplaceAll(output, "\\\n", " ")
	for line := range strings.SplitSeq(output, "\n") {
		if !strings.Contains(line, "run_test.pl") {
			continue
		}
		words := splitShellWords(line)
		start := -1
		for i, word := range words {
			if strings.Contains(word, "run_test.pl") {
				start = i + 1
				break
			}
		}
		if start < 0 {
			continue
		}
		for _, word := range words[start:] {
			add(normalizePostGISTestArg(cloneDir, makeDir, word))
		}
	}
	return tests
}

func normalizePostGISTestArg(cloneDir, makeDir, arg string) string {
	arg = strings.TrimSpace(arg)
	arg = strings.TrimRight(arg, ";")
	if arg == "" || strings.HasPrefix(arg, "-") || arg == "&&" || arg == "||" || arg == "|" {
		return ""
	}
	if strings.Contains(arg, "=") && !strings.ContainsAny(arg, `/\`) {
		return ""
	}
	arg = strings.TrimSuffix(arg, ".sql")

	candidates := []string{arg}
	if filepath.IsAbs(arg) {
		if rel, err := filepath.Rel(cloneDir, arg); err == nil && !strings.HasPrefix(rel, "..") {
			candidates = append([]string{rel}, candidates...)
		}
	} else if makeDir != "" {
		candidates = append(candidates, filepath.Join(makeDir, arg))
	}
	for _, candidate := range candidates {
		candidate = filepath.ToSlash(filepath.Clean(candidate))
		if candidate == "." || strings.HasPrefix(candidate, "../") || strings.HasPrefix(candidate, "/") {
			continue
		}
		if postGISTestExpectedExists(cloneDir, candidate) {
			return candidate
		}
	}
	return ""
}

func splitShellWords(line string) []string {
	var words []string
	var buf strings.Builder
	quote := rune(0)
	escaped := false
	flush := func() {
		if buf.Len() == 0 {
			return
		}
		words = append(words, buf.String())
		buf.Reset()
	}
	for _, r := range line {
		if escaped {
			buf.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				buf.WriteRune(r)
			}
			continue
		}
		switch r {
		case '\'', '"':
			quote = r
		case ' ', '\t', '\r', '\n':
			flush()
		default:
			buf.WriteRune(r)
		}
	}
	if escaped {
		buf.WriteRune('\\')
	}
	flush()
	return words
}

func parsePostGISResults(output string) (*TestResults, error) {
	res := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}
	lineRe := regexp.MustCompile(`^\s+(.+?)\s+\.+\s+(ok|failed|skipped)(?:\s*(.*))?$`)
	durationRe := regexp.MustCompile(`in\s+(\d+)\s+ms`)
	for line := range strings.SplitSeq(output, "\n") {
		m := lineRe.FindStringSubmatch(line)
		if len(m) == 0 {
			continue
		}
		rawName := strings.TrimSpace(m[1])
		statusText := m[2]
		detail := strings.TrimSpace(m[3])
		status := "pass"
		switch statusText {
		case "failed":
			status = "fail"
		case "skipped":
			status = "skip"
		}
		duration := ""
		if dm := durationRe.FindStringSubmatch(detail); len(dm) == 2 {
			duration = dm[1] + "ms"
		}
		tr := IndividualTestResult{
			Name:     postGISResultName(rawName),
			Status:   status,
			Duration: duration,
		}
		if status == "fail" {
			if detail == "" {
				detail = "PostGIS runner reported failure"
			}
			tr.FailReason = detail
			res.FailureDetails = append(res.FailureDetails, TestFailure{
				TestName: tr.Name,
				Error:    detail,
			})
		}
		res.Tests = append(res.Tests, tr)
	}
	if len(res.Tests) == 0 {
		return nil, errors.New("no PostGIS regress result lines found")
	}
	recountResults(res)
	return res, nil
}

func postGISResultName(rawName string) string {
	module := "postgis"
	switch {
	case strings.HasPrefix(rawName, "topology/"):
		module = "postgis_topology"
	case strings.HasPrefix(rawName, "raster/"):
		module = "postgis_raster"
	case strings.HasPrefix(rawName, "sfcgal/"):
		module = "postgis_sfcgal"
	}
	return module + "/" + rawName
}

func verifyPostGISResults(ctx context.Context, cloneDir, tmpDir string, res *TestResults, mode PatchMode) {
	repoRoot := findRepoRoot()
	for i := range res.Tests {
		test := &res.Tests[i]
		module, rawName, ok := strings.Cut(test.Name, "/")
		if !ok || rawName == "create_extension" || test.Status == "skip" {
			continue
		}
		expectedPath := postGISExpectedPath(cloneDir, rawName)
		actualPath := filepath.Join(tmpDir, fmt.Sprintf("test_%d_out", i+1))
		if expectedPath == "" || !suiteutil.FileExists(actualPath) {
			continue
		}
		outcome, err := VerifyTest(ctx, VerifyInput{
			Name:         rawName,
			ExpectedPath: expectedPath,
			ActualPath:   actualPath,
			PatchDir:     filepath.Join(PatchesDir(), "external", module),
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

func postGISTestExpectedExists(cloneDir, rawName string) bool {
	if postGISExpectedPath(cloneDir, rawName) != "" {
		return true
	}
	// Some PostGIS expected files are generated by configure from .in templates.
	// The built tree should normally contain the generated file by the time tests
	// run, but accepting templates during dry-run parsing keeps discovery tied to
	// real expected-output fixtures without mistaking runner hook SQL files for
	// tests.
	return slices.ContainsFunc([]string{
		filepath.Join(cloneDir, rawName+"_expected.in"),
		filepath.Join(cloneDir, rawName+".expected.in"),
	}, suiteutil.FileExists)
}

func postGISExpectedPath(cloneDir, rawName string) string {
	candidates := []string{
		filepath.Join(cloneDir, rawName+"_expected"),
		filepath.Join(cloneDir, rawName+".expected"),
	}
	for _, c := range candidates {
		if suiteutil.FileExists(c) {
			return c
		}
	}
	return ""
}

func testResultsFromSynthetic(tests []IndividualTestResult) *TestResults {
	res := &TestResults{Tests: tests}
	recountResults(res)
	return res
}

func recountResults(res *TestResults) {
	res.PassedTests = 0
	res.FailedTests = 0
	res.SkippedTests = 0
	res.FailureDetails = res.FailureDetails[:0]
	for _, tr := range res.Tests {
		switch tr.Status {
		case "fail":
			res.FailedTests++
			detail := tr.FailReason
			if detail == "" {
				detail = "test failed"
			}
			res.FailureDetails = append(res.FailureDetails, TestFailure{TestName: tr.Name, Error: detail})
		case "skip":
			res.SkippedTests++
		default:
			res.PassedTests++
		}
	}
	res.TotalTests = len(res.Tests)
}

// resetContribState resets the shared postgres database to a clean baseline on
// the primary's PostgreSQL directly (bypassing multigateway, which rejects
// schema DDL), clearing everything a previous module's suite installed:
//
//  1. every extension except the built-in plpgsql — extensions whose control
//     file pins a non-public schema (pgmq → pgmq, pgsodium → pgsodium,
//     pg_graphql → graphql) survive a public-only reset, and leftovers poison
//     later suites' catalog-introspection output (pgtap's extensions_are sees
//     them as "extra"; its aretap helper even breaks on any schema matching
//     LIKE 'pg_%' with the unescaped underscore wildcard, e.g. pgmq);
//  2. every user schema except public — preserving the system schemas and
//     `multigres`, which holds multipooler's internal heartbeat/leader state
//     (see multipooler's pg_multischema.go) and must outlive the reset;
//  3. the public schema itself, dropped and recreated with stock grants.
//
// Test fixtures assume a clean database; sharing one postgres DB across
// modules otherwise leaks types/extensions/schemas between them.
func resetContribState(directPgPort int, password string) error {
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		directPgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	// Drop leftover extensions first: extension-created schemas (pg_cron's cron)
	// are owned by the extension and go away with it; the schema sweep below
	// catches the rest.
	rows, err := db.Query(`SELECT extname FROM pg_extension WHERE extname <> 'plpgsql'`)
	if err != nil {
		return fmt.Errorf("list extensions: %w", err)
	}
	exts, err := collectStrings(rows)
	if err != nil {
		return fmt.Errorf("list extensions: %w", err)
	}
	for _, ext := range exts {
		if _, err := db.Exec(fmt.Sprintf("DROP EXTENSION IF EXISTS %q CASCADE", ext)); err != nil {
			return fmt.Errorf("drop extension %q: %w", ext, err)
		}
	}

	rows, err = db.Query(`SELECT nspname FROM pg_namespace
		WHERE nspname NOT LIKE 'pg\_%'
		  AND nspname NOT IN ('information_schema', 'public', 'multigres')`)
	if err != nil {
		return fmt.Errorf("list schemas: %w", err)
	}
	schemas, err := collectStrings(rows)
	if err != nil {
		return fmt.Errorf("list schemas: %w", err)
	}
	for _, schema := range schemas {
		if _, err := db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %q CASCADE", schema)); err != nil {
			return fmt.Errorf("drop schema %q: %w", schema, err)
		}
	}

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

// terminateDatabaseClientBackends closes existing client backends for the shared
// postgres database on the primary, excluding this direct maintenance
// connection. The PostGIS harness uses it after installing postgis_topology
// directly on the primary: that install updates ALTER DATABASE search_path, but
// already-open multipooler backends keep the old startup default until they
// reconnect. Terminating them here forces subsequent gateway traffic to open
// fresh backends that inherit the new database default.
func terminateDatabaseClientBackends(directPgPort int, password string) (int, error) {
	connStr := fmt.Sprintf("host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
		directPgPort, password)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND backend_type = 'client backend'`)
	if err != nil {
		return 0, fmt.Errorf("terminate client backends: %w", err)
	}
	defer rows.Close()

	terminated := 0
	for rows.Next() {
		var ok sql.NullBool
		if err := rows.Scan(&ok); err != nil {
			return terminated, err
		}
		if ok.Valid && ok.Bool {
			terminated++
		}
	}
	if err := rows.Err(); err != nil {
		return terminated, err
	}
	return terminated, nil
}

// collectStrings drains a single-column query result into a slice, closing the
// rows. Used by resetContribState's catalog sweeps.
func collectStrings(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
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
