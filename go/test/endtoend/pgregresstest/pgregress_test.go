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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPostgreSQLRegression tests PostgreSQL compatibility by running the official
// PostgreSQL regression and isolation test suites against one or more frontends.
//
// Steps:
//  1. Checks out PostgreSQL source code (REL_17_6) from GitHub.
//  2. Builds PostgreSQL using ./configure and make.
//  3. Builds isolation test tools (only when the isolation suite will run).
//  4. Prepends the built PostgreSQL bin directory to PATH so pgctld + the
//     standalone pgbouncer-target PostgreSQL instances share the same binary
//     as the regression test library (regress.so).
//  5. For each target in PGREGRESS_TARGETS (default: multigateway), spins up
//     that target's backing cluster and runs the enabled suites against it.
//  6. Generates a unified compatibility report covering every (suite, target).
//
// The test is skipped by default. Enable via one of:
//   - RUN_EXTENDED_QUERY_SERVING_TESTS=1 — runs both suites (what CI uses)
//   - RUN_PGREGRESS=1 — runs the regression suite only (local iteration)
//   - RUN_PGISOLATION=1 — runs the isolation suite only (local iteration)
//
// Setting more than one is fine; the union is run.
//
// Environment variables:
//   - RUN_EXTENDED_QUERY_SERVING_TESTS=1 — enable both regression and isolation
//   - RUN_PGREGRESS=1 — enable regression only
//   - RUN_PGISOLATION=1 — enable isolation only
//   - PGREGRESS_TARGETS="multigateway,pgbouncer-session,pgbouncer-tx" — target
//     frontends to run against; defaults to multigateway only. Triggered by the
//     workflow_dispatch input for ad-hoc comparison runs.
//   - PGREGRESS_TESTS="boolean char" — run only specific regression tests
//   - PGISOLATION_TESTS="deadlock-simple tuplelock-update" — run only specific isolation tests
func TestPostgreSQLRegression(t *testing.T) {
	extendedGate := os.Getenv(suiteutil.EnvRunExtendedQueryServingTests) == "1"
	runRegress := extendedGate || os.Getenv("RUN_PGREGRESS") == "1"
	runIsolation := extendedGate || os.Getenv("RUN_PGISOLATION") == "1"
	if !runRegress && !runIsolation {
		t.Skipf("skipping: set %s=1, or RUN_PGREGRESS=1 and/or RUN_PGISOLATION=1, to run",
			suiteutil.EnvRunExtendedQueryServingTests)
	}

	// PGREGRESS_TARGETS selects which frontends the suites run against. The
	// extended-query-serving CI label and the nightly cron both unset this and
	// therefore run only multigateway; the all-targets comparison run is
	// triggered manually via the workflow_dispatch input.
	targets, err := ParseTargets(os.Getenv("PGREGRESS_TARGETS"))
	if err != nil {
		t.Fatalf("PGREGRESS_TARGETS: %v", err)
	}

	// Check build dependencies first (before doing anything expensive)
	if err := CheckBuildDependencies(t); err != nil {
		t.Skipf("Build dependencies not available: %v", err)
	}

	// Each target gets its own suite-timeout budget so a slow target cannot
	// starve later ones. The build phase needs room for a cold clone +
	// configure + make + install on slower developer machines (macOS); CI is
	// faster so 20 minutes is overkill there but harmless.
	const (
		buildTimeout = 20 * time.Minute
		suiteTimeout = 20 * time.Minute
		setupTimeout = 5 * time.Minute
	)

	buildCtx := utils.WithTimeout(t, buildTimeout)
	builder := NewPostgresBuilder(t)
	t.Cleanup(func() {
		builder.Cleanup()
	})

	// Phase 1: Setup PostgreSQL source
	t.Logf("Phase 1: Setting up PostgreSQL source...")
	if err := builder.EnsureSource(t, buildCtx); err != nil {
		t.Fatalf("Failed to setup PostgreSQL source: %v", err)
	}

	// Phase 2: Build PostgreSQL
	t.Logf("Phase 2: Building PostgreSQL...")
	if err := builder.Build(t, buildCtx); err != nil {
		t.Fatalf("Failed to build PostgreSQL: %v", err)
	}

	// Phase 2b: Build isolation test tools (only when that suite will run)
	if runIsolation {
		t.Logf("Phase 2b: Building isolation test tools...")
		if err := builder.BuildIsolation(t, buildCtx); err != nil {
			t.Fatalf("Failed to build isolation test tools: %v", err)
		}
	}

	// Phase 3: Prepend built PostgreSQL bin directory to PATH so that pgctld
	// and the per-target standalone PostgreSQL instances use the same
	// PostgreSQL version as the regression test library (regress.so).
	t.Logf("Phase 3: Configuring PATH to use built PostgreSQL...")
	pgBinDir := filepath.Join(builder.InstallDir, "bin")
	currentPath := os.Getenv("PATH")
	newPath := pgBinDir + string(os.PathListSeparator) + currentPath
	if err := os.Setenv("PATH", newPath); err != nil {
		t.Fatalf("Failed to set PATH: %v", err)
	}
	t.Logf("Using built PostgreSQL from %s", pgBinDir)

	// Phase 4: Loop over targets, spinning up each target's cluster and
	// running the enabled suites against it.
	t.Logf("Phase 4: Running suites against targets: %s", joinTargets(targets))

	suitesByName := map[string]*SuiteResult{}

	for _, tgt := range targets {
		t.Run(string(tgt), func(t *testing.T) {
			runTargetSuites(t, runTargetSuitesArgs{
				target:       tgt,
				builder:      builder,
				runRegress:   runRegress,
				runIsolation: runIsolation,
				setupTimeout: setupTimeout,
				suiteTimeout: suiteTimeout,
				suitesByName: suitesByName,
			})
		})
	}

	// Phase 5: Generate unified report covering every (suite, target).
	if len(suitesByName) > 0 {
		suites := orderedSuites(suitesByName)
		if _, err := builder.WriteMarkdownSummary(t, suites); err != nil {
			t.Logf("Warning: Failed to write markdown summary: %v", err)
		}
		if _, err := builder.WriteJSONResults(t, suites); err != nil {
			t.Logf("Warning: Failed to write JSON results: %v", err)
		}
	}
}

// runTargetSuitesArgs groups arguments for runTargetSuites — easier to grow
// than a long positional signature when later commits add per-target options.
type runTargetSuitesArgs struct {
	target       Target
	builder      *PostgresBuilder
	runRegress   bool
	runIsolation bool
	setupTimeout time.Duration
	suiteTimeout time.Duration
	suitesByName map[string]*SuiteResult
}

// runTargetSuites brings up the cluster for a single target, runs the enabled
// suites against it, and aggregates the per-(suite, target) results into
// args.suitesByName. The cluster is torn down via t.Cleanup so it dies even
// if a sub-suite t.Fatal's.
//
// Isolation is skipped for pgbouncer targets even when args.runIsolation is
// set. The isolation harness uses a multigres-specific shim that maps
// virtual PIDs to real backend PIDs via application_name; pgbouncer does not
// propagate session-scoped state, so the shim has nothing to map against and
// most specs hang waiting for lock-release detection that can never fire.
// Surfacing "(timed out)" rows for every pgbouncer isolation run would be
// noise without signal — we skip the run cleanly and leave the pgbouncer
// columns blank in the isolation table.
func runTargetSuites(t *testing.T, args runTargetSuitesArgs) {
	t.Helper()

	cluster, err := newTargetCluster(t, args.target, args.builder)
	if err != nil {
		t.Fatalf("construct cluster for %s: %v", args.target, err)
	}
	setupCtx, setupCancel := context.WithTimeout(context.Background(), args.setupTimeout)
	defer setupCancel()
	if err := cluster.Setup(t, setupCtx); err != nil {
		t.Fatalf("setup cluster for %s: %v", args.target, err)
	}
	t.Cleanup(cluster.Teardown)

	runIsolationForTarget := args.runIsolation && args.target == TargetMultigateway
	if args.runIsolation && !runIsolationForTarget {
		t.Logf("Skipping isolation suite for %s: harness shim is multigres-specific and pgbouncer does not propagate the vpid stamp the shim looks for.", args.target)
	}

	if args.runRegress {
		t.Run("regression", func(t *testing.T) {
			runRegressionSuite(t, args.suiteTimeout, args.builder, cluster, args.suitesByName)
		})
	}

	// Between suites: full cluster reinit. The regression suite can leave
	// PostgreSQL in a degraded state (crashed backends, stale pools,
	// modified databases). A clean cluster gives isolation a fair shake.
	if args.runRegress && runIsolationForTarget {
		t.Logf("Reinitializing %s cluster between suites...", args.target)
		reinitCtx, reinitCancel := context.WithTimeout(context.Background(), args.setupTimeout)
		if err := cluster.ReinitializeBetweenSuites(t, reinitCtx); err != nil {
			reinitCancel()
			t.Fatalf("reinit cluster for %s: %v", args.target, err)
		}
		reinitCancel()
	}

	if runIsolationForTarget {
		t.Run("isolation", func(t *testing.T) {
			runIsolationSuite(t, args.suiteTimeout, args.builder, cluster, args.suitesByName)
		})
	}
}

// runRegressionSuite executes pg_regress against cluster and records its
// per-target results into suitesByName under the "Regression Tests" key.
// Patch-based verification runs only on the multigateway target — pgbouncer
// targets are a control baseline and explicitly skip patches.
func runRegressionSuite(t *testing.T, suiteTimeout time.Duration, builder *PostgresBuilder, cluster targetCluster, suitesByName map[string]*SuiteResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
	defer cancel()

	// Multigateway cannot run pg_regress's default CREATE-then-DROP DATABASE
	// flow (it rejects DROP DATABASE), so it pins the suite to the
	// pre-existing "postgres" DB. The pgbouncer comparison targets sit on a
	// standalone PG that accepts the full lifecycle — let pg_regress own
	// "regression" so tests that hard-code that DB name run cleanly.
	useExistingDB := cluster.Target() == TargetMultigateway
	results, err := builder.RunRegressionTests(t, ctx, cluster.FrontendPort(), cluster.Password(), useExistingDB)
	if results == nil {
		if err != nil {
			t.Fatalf("Regression harness failed for %s: %v", cluster.Target(), err)
		}
		t.Fatalf("Regression harness returned nil results for %s", cluster.Target())
	}

	if cluster.Target() == TargetMultigateway {
		regressDir := filepath.Join(builder.BuildDir, "src", "test", "regress")
		if verr := builder.VerifyWithPatches(t, ctx, results, regressDir, builder.OutputDir); verr != nil {
			t.Logf("Warning: patch verification failed: %v", verr)
		}
	}

	logSuiteResults(t, fmt.Sprintf("Regression (%s)", cluster.Target()), results)

	expectedRegress := 0
	if os.Getenv("PGREGRESS_TESTS") == "" {
		schedule := filepath.Join(builder.SourceDir, "src", "test", "regress", "parallel_schedule")
		if n, e := CountScheduleTests(schedule); e == nil {
			expectedRegress = n
			t.Logf("Regression schedule has %d tests; %d executed (%s)", n, results.TotalTests, cluster.Target())
		}
	}

	appendTargetResults(suitesByName, "Regression Tests", cluster.Target(), results, expectedRegress)

	if err != nil && results.TotalTests == 0 {
		t.Fatalf("Regression harness failed for %s: %v", cluster.Target(), err)
	}
}

// runIsolationSuite executes pg_isolation_regress against cluster and records
// its per-target results into suitesByName under the "Isolation Tests" key.
// The lock-detection shim is installed via the cluster's DirectPgPort so it
// bypasses the frontend pooler — this is the same trick the multigateway
// path uses, repurposed for pgbouncer comparison targets.
func runIsolationSuite(t *testing.T, suiteTimeout time.Duration, builder *PostgresBuilder, cluster targetCluster, suitesByName map[string]*SuiteResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
	defer cancel()

	directPgPort := cluster.DirectPgPort(t)
	results, err := builder.RunIsolationTests(t, ctx, cluster.FrontendPort(), directPgPort, cluster.Password())
	if results == nil {
		if err != nil {
			t.Fatalf("Isolation harness failed for %s: %v", cluster.Target(), err)
		}
		t.Fatalf("Isolation harness returned nil results for %s", cluster.Target())
	}

	logSuiteResults(t, fmt.Sprintf("Isolation (%s)", cluster.Target()), results)

	expectedIsolation := 0
	if os.Getenv("PGISOLATION_TESTS") == "" {
		schedule := filepath.Join(builder.SourceDir, "src", "test", "isolation", "isolation_schedule")
		if n, e := CountScheduleTests(schedule); e == nil {
			expectedIsolation = n
			t.Logf("Isolation schedule has %d tests; %d executed (%s)", n, results.TotalTests, cluster.Target())
		}
	}

	appendTargetResults(suitesByName, "Isolation Tests", cluster.Target(), results, expectedIsolation)

	if err != nil && results.TotalTests == 0 {
		t.Fatalf("Isolation harness failed for %s: %v", cluster.Target(), err)
	}
}

// appendTargetResults stores per-target results for a suite, creating the
// SuiteResult on first write. ExpectedTests is the per-suite schedule count
// and is target-independent; the larger of the existing and incoming values
// wins so timeouts on one target do not undercount the schedule.
func appendTargetResults(m map[string]*SuiteResult, name string, tgt Target, r *TestResults, expected int) {
	s, ok := m[name]
	if !ok {
		s = &SuiteResult{Name: name, PerTarget: map[Target]*TestResults{}}
		m[name] = s
	}
	s.PerTarget[tgt] = r
	if expected > s.ExpectedTests {
		s.ExpectedTests = expected
	}
}

// orderedSuites returns the suites in a stable order (regression first,
// isolation second) so reports look identical across runs that include the
// same set of suites.
func orderedSuites(m map[string]*SuiteResult) []SuiteResult {
	var out []SuiteResult
	for _, name := range []string{"Regression Tests", "Isolation Tests"} {
		if s, ok := m[name]; ok {
			out = append(out, *s)
		}
	}
	return out
}

// logSuiteResults logs structured results for a test suite to the test output.
func logSuiteResults(t *testing.T, suiteName string, results *TestResults) {
	t.Helper()

	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("PostgreSQL %s Test Results:", suiteName)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("  Total:   %d", results.TotalTests)
	t.Logf("  Passed:  %d", results.PassedTests)
	t.Logf("  Failed:  %d", results.FailedTests)
	t.Logf("  Skipped: %d", results.SkippedTests)
	t.Logf("  Duration: %v", results.Duration)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	if len(results.Tests) > 0 {
		t.Logf("")
		t.Logf("  %-4s %-30s %-8s %s", "#", "Test", "Status", "Duration")
		t.Logf("  %-4s %-30s %-8s %s", "---", "----", "------", "--------")
		for i, test := range results.Tests {
			emoji := "✅"
			if test.Status == "fail" {
				emoji = "❌"
			}
			dur := test.Duration
			if dur == "" {
				dur = "-"
			}
			t.Logf("  %-4d %-30s %s      %s", i+1, test.Name, emoji, dur)
		}
	}

	if results.FailedTests > 0 {
		t.Logf("")
		t.Logf("Failed Tests:")
		for _, failure := range results.FailureDetails {
			t.Logf("  ❌ %s - %s", failure.TestName, failure.Error)
		}
		t.Logf("")
		t.Logf("⚠️  WARNING: %d %s test(s) failed.", results.FailedTests, suiteName)
		t.Logf("   This is logged for investigation but won't fail the Go test.")
		t.Logf("   Review the test output above for details.")
	} else if results.PassedTests > 0 {
		t.Logf("")
		t.Logf("✅ All %d %s tests passed!", results.PassedTests, suiteName)
	}
}
