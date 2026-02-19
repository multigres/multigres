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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPostgreSQLRegression tests PostgreSQL compatibility by running the official
// PostgreSQL regression and isolation test suites against a multigres cluster.
//
// This test performs the following steps:
// 1. Checks out PostgreSQL source code (REL_17_6) from GitHub
// 2. Builds PostgreSQL using ./configure and make
// 3. Builds isolation test tools if RUN_PGISOLATION=1
// 4. Prepends the built PostgreSQL bin directory to PATH
// 5. Spins up a multigres cluster (2 nodes + multigateway) using the built PostgreSQL
// 6. Runs regression tests (if RUN_PGREGRESS=1) through multigateway
// 7. Runs isolation tests (if RUN_PGISOLATION=1) through multigateway
// 8. Generates a unified compatibility report
//
// The test is skipped by default. Set RUN_PGREGRESS=1 and/or RUN_PGISOLATION=1 to run.
//
// Environment variables:
//   - RUN_PGREGRESS=1: enable regression tests
//   - PGREGRESS_TESTS="boolean char": run only specific regression tests
//   - RUN_PGISOLATION=1: enable isolation tests
//   - PGISOLATION_TESTS="deadlock-simple tuplelock-update": run only specific isolation tests
func TestPostgreSQLRegression(t *testing.T) {
	runRegress := os.Getenv("RUN_PGREGRESS") == "1"
	runIsolation := os.Getenv("RUN_PGISOLATION") == "1"

	// Skip unless at least one suite is enabled
	if !runRegress && !runIsolation {
		t.Skip("skipping pg_regress/isolation tests (set RUN_PGREGRESS=1 and/or RUN_PGISOLATION=1 to run)")
	}

	// Check build dependencies first (before doing anything expensive)
	if err := CheckBuildDependencies(t); err != nil {
		t.Skipf("Build dependencies not available: %v", err)
	}

	// Create PostgresBuilder for managing source and build.
	// Use a longer timeout when isolation tests are enabled (lock waits).
	timeout := 20 * time.Minute
	if runIsolation {
		timeout = 45 * time.Minute
	}
	ctx := utils.WithTimeout(t, timeout)
	builder := NewPostgresBuilder(t)
	t.Cleanup(func() {
		builder.Cleanup()
	})

	// Phase 1: Setup PostgreSQL source
	t.Logf("Phase 1: Setting up PostgreSQL source...")
	if err := builder.EnsureSource(t, ctx); err != nil {
		t.Fatalf("Failed to setup PostgreSQL source: %v", err)
	}

	// Phase 2: Build PostgreSQL
	t.Logf("Phase 2: Building PostgreSQL...")
	if err := builder.Build(t, ctx); err != nil {
		t.Fatalf("Failed to build PostgreSQL: %v", err)
	}

	// Phase 2b: Build isolation test tools (only when needed)
	if runIsolation {
		t.Logf("Phase 2b: Building isolation test tools...")
		if err := builder.BuildIsolation(t, ctx); err != nil {
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
			results, err := builder.RunRegressionTests(t, ctx, setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
			if results == nil {
				if err != nil {
					t.Fatalf("Test harness failed to execute: %v", err)
				}
				t.Fatal("Test harness returned nil results")
				return
			}

			logSuiteResults(t, "Regression", results)

			suites = append(suites, SuiteResult{
				Name:     "Regression Tests",
				Results:  results,
				DiffsDir: builder.OutputDir,
			})

			if err != nil && results.TotalTests == 0 {
				t.Fatalf("Test harness failed to execute: %v", err)
			}
		})
	}

	// Phase 6: Run isolation tests
	if runIsolation {
		t.Run("isolation", func(t *testing.T) {
			results, err := builder.RunIsolationTests(t, ctx, setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
			if results == nil {
				if err != nil {
					t.Fatalf("Isolation test harness failed to execute: %v", err)
				}
				t.Fatal("Isolation test harness returned nil results")
				return
			}

			logSuiteResults(t, "Isolation", results)

			suites = append(suites, SuiteResult{
				Name:     "Isolation Tests",
				Results:  results,
				DiffsDir: filepath.Join(builder.OutputDir, "isolation"),
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
