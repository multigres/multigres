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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPostgreSQLRegression tests PostgreSQL compatibility by running the official
// PostgreSQL regression and isolation test suites against a multigres cluster.
//
// This test performs the following steps:
// 1. Checks out PostgreSQL source code (REL_17_6) from GitHub
// 2. Builds PostgreSQL using ./configure and make
// 3. Builds isolation test tools (only when the isolation suite will run)
// 4. Prepends the built PostgreSQL bin directory to PATH
// 5. Spins up a multigres cluster (2 nodes + multigateway) using the built PostgreSQL
// 6. Runs regression tests through multigateway (if enabled)
// 7. Runs isolation tests through multigateway (if enabled)
// 8. Generates a unified compatibility report
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

	// Check build dependencies first (before doing anything expensive)
	if err := CheckBuildDependencies(t); err != nil {
		t.Skipf("Build dependencies not available: %v", err)
	}

	// Each test suite gets its own sub-context so one suite hanging cannot
	// starve the other. The build phase needs room for a cold clone + configure
	// + make + install on slower developer machines (macOS); CI is faster so
	// 20 minutes is overkill there but harmless.
	const (
		buildTimeout = 20 * time.Minute
		suiteTimeout = 20 * time.Minute
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

	// Phase 3: Prepend built PostgreSQL bin directory to PATH so that pgctld uses
	// the same PostgreSQL version as the regression test library (regress.so)
	t.Logf("Phase 3: Configuring PATH to use built PostgreSQL...")
	pgBinDir := filepath.Join(builder.InstallDir, "bin")
	currentPath := os.Getenv("PATH")
	newPath := pgBinDir + string(os.PathListSeparator) + currentPath
	if err := os.Setenv("PATH", newPath); err != nil {
		t.Fatalf("Failed to set PATH: %v", err)
	}
	t.Logf("Using built PostgreSQL from %s", pgBinDir)

	// Phase 4: Set up cluster using the built PostgreSQL
	// This MUST happen after we modify PATH so pgctld uses the correct binaries
	t.Logf("Phase 4: Setting up multigres cluster...")
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Collect suite results for the unified report
	var suites []SuiteResult

	// Phase 5: Run regression tests
	if runRegress {
		t.Run("regression", func(t *testing.T) {
			suiteCtx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
			defer cancel()
			results, err := builder.RunRegressionTests(t, suiteCtx, setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
			if results == nil {
				if err != nil {
					t.Fatalf("Test harness failed to execute: %v", err)
				}
				t.Fatal("Test harness returned nil results")
				return
			}

			// Replace pg_regress's strict diff verdict with patch-based
			// verification. Operates on the regress build directory, where
			// pg_regress wrote expected/ and results/ files.
			regressDir := filepath.Join(builder.BuildDir, "src", "test", "regress")
			if verr := builder.VerifyWithPatches(t, suiteCtx, results, regressDir, builder.OutputDir); verr != nil {
				t.Logf("Warning: patch verification failed: %v", verr)
			}

			logSuiteResults(t, "Regression", results)

			// Count expected tests from schedule (only for full suite runs)
			var expectedRegress int
			if os.Getenv("PGREGRESS_TESTS") == "" {
				schedule := filepath.Join(builder.SourceDir, "src", "test", "regress", "parallel_schedule")
				if n, err := CountScheduleTests(schedule); err == nil {
					expectedRegress = n
					t.Logf("Regression schedule has %d tests; %d executed", n, results.TotalTests)
				}
			}

			suites = append(suites, SuiteResult{
				Name:          "Regression Tests",
				Results:       results,
				ExpectedTests: expectedRegress,
			})

			if err != nil && results.TotalTests == 0 {
				t.Fatalf("Test harness failed to execute: %v", err)
			}
		})
	}

	// Between suites: fully reinitialize the cluster. The regression suite
	// can leave PostgreSQL in a degraded state (crashed backends, stale
	// connection pools, modified databases). A full teardown + re-bootstrap
	// gives isolation a completely fresh cluster.
	if runRegress && runIsolation {
		t.Logf("Reinitializing cluster between suites...")
		setup.ReinitializeCluster(t)
	}

	// Phase 6: Run isolation tests
	if runIsolation {
		t.Run("isolation", func(t *testing.T) {
			suiteCtx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
			defer cancel()
			results, err := builder.RunIsolationTests(t, suiteCtx, setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
			if results == nil {
				if err != nil {
					t.Fatalf("Isolation test harness failed to execute: %v", err)
				}
				t.Fatal("Isolation test harness returned nil results")
				return
			}

			logSuiteResults(t, "Isolation", results)

			// Count expected tests from schedule (only for full suite runs)
			var expectedIsolation int
			if os.Getenv("PGISOLATION_TESTS") == "" {
				schedule := filepath.Join(builder.SourceDir, "src", "test", "isolation", "isolation_schedule")
				if n, err := CountScheduleTests(schedule); err == nil {
					expectedIsolation = n
					t.Logf("Isolation schedule has %d tests; %d executed", n, results.TotalTests)
				}
			}

			suites = append(suites, SuiteResult{
				Name:          "Isolation Tests",
				Results:       results,
				ExpectedTests: expectedIsolation,
			})

			if err != nil && results.TotalTests == 0 {
				t.Fatalf("Isolation test harness failed to execute: %v", err)
			}
		})
	}

	// Phase 7: Generate unified report
	if len(suites) > 0 {
		if _, err := builder.WriteMarkdownSummary(t, suites); err != nil {
			t.Logf("Warning: Failed to write markdown summary: %v", err)
		}
		if _, err := builder.WriteJSONResults(t, suites); err != nil {
			t.Logf("Warning: Failed to write JSON results: %v", err)
		}
	}
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
