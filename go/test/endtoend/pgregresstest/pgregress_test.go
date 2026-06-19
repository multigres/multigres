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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
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
// 8. Runs contrib extension tests through multigateway (if enabled)
// 9. Runs external extension tests (e.g. pgvector) through multigateway (if enabled)
// 10. Generates a unified compatibility report
//
// The test is skipped by default. Enable via one of:
//   - RUN_EXTENDED_QUERY_SERVING_TESTS=1 — runs all suites (what CI uses)
//   - RUN_PGREGRESS=1 — runs the regression suite only (local iteration)
//   - RUN_PGISOLATION=1 — runs the isolation suite only (local iteration)
//   - RUN_PGCONTRIB=1 — runs the contrib extension suite only (local iteration)
//   - RUN_PGEXTERNAL=1 — runs the external extension suite only (local iteration)
//
// Setting more than one is fine; the union is run.
//
// Environment variables:
//   - RUN_EXTENDED_QUERY_SERVING_TESTS=1 — enable regression, isolation, contrib, and external
//   - RUN_PGREGRESS=1 — enable regression only
//   - RUN_PGISOLATION=1 — enable isolation only
//   - RUN_PGCONTRIB=1 — enable contrib extension suite only
//   - RUN_PGEXTERNAL=1 — enable external extension suite only
//   - PGREGRESS_TESTS="boolean char" — run only specific regression tests
//   - PGISOLATION_TESTS="deadlock-simple tuplelock-update" — run only specific isolation tests
//   - PGCONTRIB_TESTS="citext hstore" — run only specific contrib modules (directory names)
//   - PGEXTERNAL_TESTS="vector" — run only specific external extensions (catalog names)
func TestPostgreSQLRegression(t *testing.T) {
	extendedGate := os.Getenv(suiteutil.EnvRunExtendedQueryServingTests) == "1"
	runRegress := extendedGate || os.Getenv("RUN_PGREGRESS") == "1"
	runIsolation := extendedGate || os.Getenv("RUN_PGISOLATION") == "1"
	runContrib := extendedGate || os.Getenv("RUN_PGCONTRIB") == "1"
	runExternal := extendedGate || os.Getenv("RUN_PGEXTERNAL") == "1"
	if !runRegress && !runIsolation && !runContrib && !runExternal {
		t.Skipf("skipping: set %s=1, or RUN_PGREGRESS=1 / RUN_PGISOLATION=1 / RUN_PGCONTRIB=1 / RUN_PGEXTERNAL=1, to run",
			suiteutil.EnvRunExtendedQueryServingTests)
	}

	// External extensions (e.g. pgvector) are built as PGXS modules; fail loudly
	// if one is marked covered in the catalog but lacks build coordinates rather
	// than silently testing nothing.
	if runExternal {
		if missing := CheckExternalSpecs(); len(missing) > 0 {
			t.Fatalf("external extensions marked covered but missing a build spec: %v", missing)
		}
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
		buildTimeout = 60 * time.Minute
		suiteTimeout = 60 * time.Minute
	)

	buildCtx := utils.WithTimeout(t, buildTimeout)
	builder := NewPostgresBuilder(t)
	t.Cleanup(func() {
		builder.Cleanup()
	})

	// The core compression regression test expects lz4 support in PostgreSQL;
	// without --with-lz4, a stock run fails before exercising Multigres behavior.
	if runRegress {
		builder.ConfigureArgs = append(builder.ConfigureArgs, "--with-lz4")
	}

	// Two contrib modules need optional build features enabled at ./configure.
	// Enable them only when the contrib suite runs so regression/isolation-only
	// builds (and other pgbuilder callers) are unaffected and can't be broken
	// by a missing libuuid / libssl:
	//   - uuid-ossp needs --with-uuid. PG_UUID_LIB overrides the implementation
	//     if the default (e2fs) is unavailable on the host (e.g. bsd / ossp).
	//   - pgcrypto (PG16+) requires OpenSSL; without --with-ssl=openssl the
	//     contrib Makefile drops pgcrypto into ALWAYS_SUBDIRS and never installs
	//     it, so CREATE EXTENSION pgcrypto fails.
	if runContrib {
		uuidLib := os.Getenv("PG_UUID_LIB")
		if uuidLib == "" {
			uuidLib = "e2fs"
		}
		builder.ConfigureArgs = append(builder.ConfigureArgs,
			"--with-uuid="+uuidLib,
			"--with-ssl=openssl",
		)
	}
	// An external-only run still needs OpenSSL when a selected extension's suite
	// depends on contrib pgcrypto (pgjwt): without --with-ssl=openssl the contrib
	// Makefile never builds pgcrypto, so the targeted InstallContribModules below
	// would fail. Skipped when runContrib already added the flag.
	if runExternal && !runContrib && slices.Contains(ExternalContribDeps(), "pgcrypto") {
		builder.ConfigureArgs = append(builder.ConfigureArgs, "--with-ssl=openssl")
	}
	// PG_CONFIGURE_EXTRA_ARGS appends host-specific ./configure flags. Needed for
	// local macOS runs of suites that require OpenSSL (contrib, or external with
	// a pgcrypto dependency): Homebrew's keg lives outside the default search
	// path, so e.g.
	//   PG_CONFIGURE_EXTRA_ARGS="--with-includes=/opt/homebrew/include --with-libraries=/opt/homebrew/lib"
	// CI (Linux) installs the dev packages into default paths and leaves this
	// unset.
	if extra := os.Getenv("PG_CONFIGURE_EXTRA_ARGS"); extra != "" {
		builder.ConfigureArgs = append(builder.ConfigureArgs, strings.Fields(extra)...)
	}

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

	// Phase 2c: Install contrib modules (only when that suite will run).
	// Top-level `make install` skips contrib, so the extension .so/.control/.sql
	// must be installed before CREATE EXTENSION can load them.
	if runContrib {
		t.Logf("Phase 2c: Installing contrib modules...")
		if err := builder.InstallContrib(t, buildCtx); err != nil {
			t.Fatalf("Failed to install contrib modules: %v", err)
		}
	}

	// Phase 2d: Clone, build, and install external extensions (only when that
	// suite will run). Each is a PGXS module living outside the PostgreSQL source
	// tree (e.g. pgvector); it is built against the just-installed PostgreSQL so
	// its .so matches the running server's ABI. ExternalBuildList includes the
	// runnable extensions plus their dependency-only modules (e.g. pgtap for
	// pg_partman, and pg_partman for pgmq), ordered so dependencies install first.
	if runExternal {
		t.Logf("Phase 2d: Installing external extensions...")
		// Install contrib modules the external extensions depend on (e.g.
		// pg_graphql's tests `create extension citext`). In a full run contrib is
		// already installed; this makes external-only runs self-sufficient.
		if deps := ExternalContribDeps(); len(deps) > 0 {
			if err := builder.InstallContribModules(t, buildCtx, deps); err != nil {
				t.Fatalf("Failed to install external contrib deps %v: %v", deps, err)
			}
		}
		for _, ext := range ExternalBuildList() {
			spec := pgbuilder.ExtensionBuildSpec{
				Name:          ext.Name,
				Repo:          ext.Repo,
				Tag:           ext.Tag,
				Commit:        ext.Commit,
				BuildSubdir:   ext.BuildSubdir,
				BuildSystem:   ext.BuildSystem,
				PgrxVersion:   ext.PgrxVersion,
				PkgConfigDeps: ext.PkgConfigDeps,
				ConfigureArgs: ext.ConfigureArgs,
			}
			if _, err := builder.InstallExternalExtension(t, buildCtx, spec); err != nil {
				t.Fatalf("Failed to install external extension %s: %v", ext.Name, err)
			}
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

	// INVARIANT: the external suite must remain the LAST suite in this
	// sequence. Its server config (shared_preload_libraries and friends, see
	// externalServerConfPaths) is applied to the cluster only at the
	// reinitialization that precedes it, precisely so the suites before it run
	// on a stock cluster — the preloaded libraries are not inert (plpgsql_check
	// emits cursor-leak WARNINGs the core plpgsql test does not expect). A
	// suite added or reordered AFTER the external phase would silently run with
	// those preloads active.

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
			// The isolation harness installs a lock-detection shim on the
			// primary's PostgreSQL directly, bypassing multigateway, so it
			// needs the primary's direct PG port too.
			directPgPort := setup.GetPrimary(t).Pgctld.PgPort
			results, err := builder.RunIsolationTests(t, suiteCtx, setup.MultigatewayPgPort, directPgPort, shardsetup.TestPostgresPassword)
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

	// Reinitialize before the contrib suite if a prior suite ran — the
	// regression/isolation suites can leave PostgreSQL in a degraded state.
	if runContrib && (runRegress || runIsolation) {
		t.Logf("Reinitializing cluster before contrib suite...")
		setup.ReinitializeCluster(t)
	}

	// Phase 6b: Run contrib extension tests
	if runContrib {
		t.Run("contrib", func(t *testing.T) {
			suiteCtx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
			defer cancel()

			modules := ContribModules()
			// directPgPort lets the harness reset the public schema on the
			// primary between modules (bypassing the gateway's DDL block).
			directPgPort := setup.GetPrimary(t).Pgctld.PgPort
			results, err := builder.RunContribTests(t, suiteCtx, modules, setup.MultigatewayPgPort, directPgPort, shardsetup.TestPostgresPassword)
			if results == nil {
				if err != nil {
					t.Fatalf("Contrib test harness failed to execute: %v", err)
				}
				t.Fatal("Contrib test harness returned nil results")
				return
			}

			logSuiteResults(t, "Contrib", results)

			suites = append(suites, SuiteResult{
				Name:    "Contrib Extension Tests",
				Results: results,
			})

			if err != nil && results.TotalTests == 0 {
				t.Fatalf("Contrib test harness failed to execute: %v", err)
			}
		})
	}

	// Reinitialize before the external suite if any prior suite ran — earlier
	// suites can leave PostgreSQL in a degraded state. This reinit also applies
	// the external extensions' server config (shared_preload_libraries and
	// friends) to the fresh cluster: the snippets are deliberately NOT applied
	// at initial setup in combined runs, so the regression/isolation/contrib
	// suites above ran on a stock cluster (the preloads are not always inert —
	// see externalServerConfPaths). The external suite is last, so the config
	// stays in effect only for it.
	if runExternal && (runRegress || runIsolation || runContrib) {
		t.Logf("Reinitializing cluster before external suite (applying external server config)...")
		if confs := externalServerConfPaths(); len(confs) > 0 {
			setup.AddPgInitdbExtraConfFiles(confs...)
		}
		setup.ReinitializeCluster(t)
	}

	// Phase 6c: Run external extension tests
	if runExternal {
		t.Run("external", func(t *testing.T) {
			suiteCtx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
			defer cancel()

			exts := ExternalModules()
			// directPgPort lets the harness reset the public schema on the
			// primary between extensions (bypassing the gateway's DDL block).
			directPgPort := setup.GetPrimary(t).Pgctld.PgPort
			results, err := builder.RunExternalTests(t, suiteCtx, exts, setup.MultigatewayPgPort, directPgPort, shardsetup.TestPostgresPassword)
			if results == nil {
				if err != nil {
					t.Fatalf("External test harness failed to execute: %v", err)
				}
				t.Fatal("External test harness returned nil results")
				return
			}

			logSuiteResults(t, "External", results)

			suites = append(suites, SuiteResult{
				Name:    "External Extension Tests",
				Results: results,
			})

			if err != nil && results.TotalTests == 0 {
				t.Fatalf("External test harness failed to execute: %v", err)
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
		if err := builder.WriteBadgeEndpoints(t, suites); err != nil {
			t.Logf("Warning: Failed to write badge endpoints: %v", err)
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
