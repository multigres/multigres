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

package benchmarking

import (
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPgBench runs pgbench benchmarks against direct PostgreSQL, multigateway,
// and optionally pgbouncer, then generates a comparison report.
//
// The test is skipped by default. Set RUN_PGBENCH=1 to run.
//
// Environment variables:
//   - RUN_PGBENCH=1: enable benchmark tests
//   - PGBENCH_DURATION: seconds per scenario (default: 30)
//   - PGBENCH_CLIENTS: comma-separated client counts (default: "1,10,50")
func TestPgBench(t *testing.T) {
	if os.Getenv("RUN_PGBENCH") != "1" {
		t.Skip("skipping pgbench tests (set RUN_PGBENCH=1 to run)")
	}

	if _, err := exec.LookPath("pgbench"); err != nil {
		t.Skipf("pgbench binary not found: %v", err)
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 30*time.Minute)

	primary := setup.GetPrimary(t)
	pgTarget := suiteutil.Target{
		Name: "postgres",
		Host: "localhost",
		Port: primary.Pgctld.PgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}
	mgwTarget := suiteutil.Target{
		Name: "multigateway",
		Host: "localhost",
		Port: setup.MultigatewayPgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}

	targets := []suiteutil.Target{pgTarget, mgwTarget}

	// Optional pgbouncer target
	pgb, err := NewPgBouncerInstance(t, "localhost", primary.Pgctld.PgPort,
		shardsetup.DefaultTestUser, shardsetup.TestPostgresPassword)
	if err == nil && pgb != nil {
		t.Cleanup(func() { pgb.Stop(t) })
		targets = append(targets, suiteutil.Target{
			Name: "pgbouncer",
			Host: "localhost",
			Port: pgb.Port(),
			User: shardsetup.DefaultTestUser,
			Pass: shardsetup.TestPostgresPassword,
			DB:   "postgres",
		})
		t.Logf("pgbouncer available at port %d", pgb.Port())
	} else {
		t.Logf("pgbouncer not available, skipping comparison")
	}

	runner := NewPgBenchRunner(t)
	duration := ParseDuration(t)
	clients := ParseClients(t)
	scenarios := DefaultScenarios(duration, clients)

	// Initialize pgbench tables once through direct postgres.
	// All targets route to the same backend so tables are shared.
	t.Run("init", func(t *testing.T) {
		if err := runner.Initialize(ctx, t, pgTarget, 10); err != nil {
			t.Fatalf("pgbench initialization failed: %v", err)
		}
	})

	// Run all scenarios against all targets.
	// Scenario failures are logged but do not fail the test — benchmarks are
	// measurements, and some scenario/target combinations may legitimately not
	// work yet (e.g. extended protocol edge cases through the proxy).
	var allResults []ScenarioResult
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			for _, target := range targets {
				t.Run(target.Name, func(t *testing.T) {
					result, err := runner.RunScenario(ctx, t, target, scenario)
					if err != nil {
						t.Logf("SKIP: pgbench failed for %s/%s: %v", target.Name, scenario.Name, err)
						return
					}
					allResults = append(allResults, *result)
					t.Logf("TPS=%.1f  AvgLatency=%.2fms  P99=%.2fms  Txns=%d",
						result.TPS, result.LatencyAvg, result.LatencyP99, result.Transactions)
				})
			}
		})
	}

	// Generate reports.
	report := &BenchmarkReport{
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Results:     allResults,
		Environment: CaptureEnvironment(t, runner.pgbenchBinary),
	}

	if jsonPath, err := WriteJSONReport(t, runner.OutputDir, report); err == nil {
		t.Logf("JSON report: %s", jsonPath)
	} else {
		t.Logf("Warning: failed to write JSON report: %v", err)
	}
	if mdPath, err := WriteMarkdownReport(t, runner.OutputDir, report); err == nil {
		t.Logf("Markdown report: %s", mdPath)
	} else {
		t.Logf("Warning: failed to write markdown report: %v", err)
	}
}
