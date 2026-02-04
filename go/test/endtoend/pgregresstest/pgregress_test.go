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
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPostgreSQLRegression tests PostgreSQL compatibility by running the official
// PostgreSQL regression test suite against a multigres cluster.
//
// This test performs the following steps:
// 1. Checks out PostgreSQL source code (REL_17_6) from GitHub
// 2. Builds PostgreSQL using ./configure and make
// 3. Spins up a multigres cluster (2 nodes + multigateway)
// 4. Runs PostgreSQL regression tests (boolean, char) through multigateway using make installcheck-tests
// 5. Reports results (logs failures but doesn't fail the test)
//
// The test is skipped by default. Set RUN_PGREGRESS=1 to run it.
func TestPostgreSQLRegression(t *testing.T) {
	// Skip unless explicitly enabled via environment variable
	if os.Getenv("RUN_PGREGRESS") != "1" {
		t.Skip("skipping pg_regress tests (set RUN_PGREGRESS=1 to run)")
	}

	// Skip if PostgreSQL binaries not available
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping regression tests")
	}

	// Get shared cluster setup (2 nodes + multigateway)
	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Create PostgresBuilder for managing source and build
	ctx := utils.WithTimeout(t, 60*time.Minute)
	builder := NewPostgresBuilder(t, setup.TempDir)
	t.Cleanup(func() {
		builder.Cleanup()
	})

	// Track if build succeeded
	buildSucceeded := false

	// Phase 1: Setup PostgreSQL source
	t.Run("setup_postgres_source", func(t *testing.T) {
		// Check build dependencies first
		if err := CheckBuildDependencies(t); err != nil {
			t.Skipf("Build dependencies not available: %v", err)
		}

		// Ensure PostgreSQL source is available (clone if needed)
		if err := builder.EnsureSource(t, ctx); err != nil {
			t.Fatalf("Failed to setup PostgreSQL source: %v", err)
		}
	})

	// Phase 2: Build PostgreSQL
	t.Run("build_postgres", func(t *testing.T) {
		if err := builder.Build(t, ctx); err != nil {
			t.Fatalf("Failed to build PostgreSQL: %v", err)
		}
		buildSucceeded = true
	})

	// Phase 3: Run regression tests - ONLY if build succeeded
	t.Run("run_regression_tests", func(t *testing.T) {
		if !buildSucceeded {
			t.Skip("Skipping regression tests because PostgreSQL build failed")
		}

		// Run tests against multigateway
		results, err := builder.RunRegressionTests(t, ctx, setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)

		// Handle nil results gracefully
		if results == nil {
			if err != nil {
				t.Fatalf("Test harness failed to execute: %v", err)
			}
			t.Fatal("Test harness returned nil results")
			return
		}

		// Always log results (even on success)
		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		t.Logf("PostgreSQL Regression Test Results:")
		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		t.Logf("  Total:   %d", results.TotalTests)
		t.Logf("  Passed:  %d", results.PassedTests)
		t.Logf("  Failed:  %d", results.FailedTests)
		t.Logf("  Skipped: %d", results.SkippedTests)
		t.Logf("  Duration: %v", results.Duration)
		t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

		// Log failure details if any
		if results.FailedTests > 0 {
			t.Logf("")
			t.Logf("Failed Tests:")
			for _, failure := range results.FailureDetails {
				t.Logf("  ❌ %s - %s", failure.TestName, failure.Error)
			}
			t.Logf("")
			t.Logf("⚠️  WARNING: %d regression test(s) failed.", results.FailedTests)
			t.Logf("   This is logged for investigation but won't fail the Go test.")
			t.Logf("   Review the test output above for details.")
		} else if results.PassedTests > 0 {
			t.Logf("")
			t.Logf("✅ All %d regression tests passed!", results.PassedTests)
		}

		// Only fail if test harness crashed (no tests ran at all)
		if err != nil && results.TotalTests == 0 {
			t.Fatalf("Test harness failed to execute: %v", err)
		}
	})
}
