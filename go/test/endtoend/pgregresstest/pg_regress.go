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
	"sort"
	"strings"
	"testing"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/tools/executil"
)

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

// runExternalRegress runs one external extension's shipped pg_regress suite
// against multigateway (the pgvector/pg_cron model) and re-evaluates each test
// through the patch pipeline. Returns nil when the checkout has no sql/ fixtures.
//
// Unlike contrib we cannot use `make installcheck`: under PGXS that target runs
// $(top_builddir)/src/test/regress/pg_regress, and PGXS resolves top_builddir
// into the install tree, where pg_regress is not installed. We instead invoke
// the pg_regress we built directly, passing the same flags the contrib suite
// relies on (--use-existing --dbname=postgres, because multigateway rejects
// DROP/CREATE DATABASE) plus the extension's --inputdir. Fixtures that assume the
// extension exists are preloaded via PreCreateExtensions, since --use-existing
// skips pg_regress's own --load-extension step.
func (pb *PostgresBuilder) runExternalRegress(t *testing.T, ctx context.Context, ext ExternalExtension, cloneDir, testDir, pgBinDir string, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	if !suiteutil.FileExists(filepath.Join(testDir, "sql")) {
		t.Logf("external/%s: no %s/sql in checkout, skipping", ext.Name, ext.TestSubdir)
		return nil, nil
	}
	tests := listRegressTests(testDir)
	if len(tests) == 0 {
		t.Logf("external/%s: no .sql tests found, skipping", ext.Name)
		return nil, nil
	}

	// Load the extension's fixtures through multigateway before the suite, the way
	// its own runner does (pg_graphql's bin/installcheck runs `psql -f
	// test/fixtures.sql` first; those fixtures CREATE the extension and set its
	// schema config). Same pooled path the test queries take.
	if ext.FixturesFile != "" {
		fixturesPath := filepath.Join(testDir, ext.FixturesFile)
		if err := loadFixturesViaGateway(ctx, filepath.Join(pgBinDir, "psql"), fixturesPath, multigatewayPort, password); err != nil {
			t.Logf("external/%s: warning: load fixtures %q failed: %v", ext.Name, ext.FixturesFile, err)
		}
	}

	// Preload extensions whose fixtures assume they already exist (pgvector's
	// fixtures open with a bare CREATE TABLE ... vector(3) and never CREATE
	// EXTENSION). Extensions whose fixtures manage the extension themselves (pg_cron
	// CREATEs it as their first statement) leave PreCreateExtensions empty so we
	// don't collide with that. resetContribState clears these (all in public) before
	// the next extension, so no teardown is needed here.
	for _, e := range ext.PreCreateExtensions {
		if err := createExtensionWithSchema(multigatewayPort, password, e.Name, e.Schema); err != nil {
			t.Logf("external/%s: warning: CREATE EXTENSION %s failed: %v", ext.Name, e.Name, err)
		}
	}

	t.Logf("Running external/%s pg_regress (%d tests) against multigateway...", ext.Name, len(tests))
	pgRegress := filepath.Join(pb.BuildDir, "src", "test", "regress", "pg_regress")
	args := []string{
		"--inputdir=" + testDir,
		"--outputdir=" + cloneDir,
		"--bindir=" + pgBinDir,
		"--use-existing",
		"--dbname=postgres",
	}
	args = append(args, tests...)
	// Run from the clone root so psql's client-side \copy resolves the relative
	// paths the fixtures use (e.g. pgvector's copy test does \copy t TO
	// 'results/vector.bin'); pg_regress writes its results/ dir under
	// --outputdir=cloneDir, which is this same directory.
	cmd := executil.Command(ctx, pgRegress, args...).WithProcessGroup().SetDir(cloneDir)

	res, err := pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
		suiteName: "External/" + ext.Name,
		outputDir: filepath.Join(pb.OutputDir, "external", ext.Name),
		srcOutDir: cloneDir,
	}, multigatewayPort, password)
	if res == nil {
		return nil, err
	}

	// Re-evaluate each test via the patch pipeline. pg_regress wrote results to
	// <cloneDir>/results (--outputdir); expected lives in <testDir>/expected, and
	// patches are per-extension under patches/external/<ext>.
	patchDir := filepath.Join(PatchesDir(), "external", ext.Name)
	pb.verifyModuleResults(ctx, testDir, filepath.Join(cloneDir, "results"), patchDir, res, GetPatchMode())
	return res, err
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
