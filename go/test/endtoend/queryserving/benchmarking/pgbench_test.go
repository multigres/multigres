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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPgBench runs pgbench against a configurable set of targets and
// generates a comparison report.
//
// The test is skipped by default. Set RUN_BENCHMARKS=1 to run.
//
// Environment variables
//
//	RUN_BENCHMARKS=1               enable the test
//	PGBENCH_DURATION               seconds per scenario (default: 30)
//	PGBENCH_CLIENTS                comma-separated client counts
//	                               (default: "1,10,50")
//	PGBENCH_PROTOCOLS              comma-separated subset of
//	                               "simple,extended" (default: both)
//	PGBENCH_NO_CHURN=1             skip the churn (-C) scenarios
//	PGBENCH_TARGETS                comma-separated subset of
//	                               "multigateway,postgres,pgbouncer"
//	                               (default: "multigateway")
//	PGBENCH_PG_MAX_CONNECTIONS=N   bump postgres max_connections to N and
//	                               restart pgctld (needed when running
//	                               postgres or pgbouncer targets at
//	                               client counts above the default 60).
//	CAPTURE_PPROF=1                capture a CPU profile from
//	                               multigateway and primary multipooler
//	                               during each scenario
//	CAPTURE_HEAP=1                 capture heap, allocs, and goroutine
//	                               profiles before+after each scenario
//
// Outputs land under /tmp/multigres_pgbench_results/<timestamp>/:
//
//	results.json, benchmark-report.md   — pgbench TPS / latency
//	cpu/<scenario>/<target>.json        — per-target CPU usage samples
//	pprof/<scenario>/<target>/cpu.pb.gz — CPU profiles (when enabled)
//	pprof/<scenario>/<target>/{heap,allocs,goroutine}-{before,after}.pb.gz
func TestPgBench(t *testing.T) {
	suiteutil.SkipUnlessEnabled(t, suiteutil.EnvRunBenchmarks)

	if _, err := exec.LookPath("pgbench"); err != nil {
		t.Skipf("pgbench binary not found: %v", err)
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	// Generous timeout — long sweeps with 5-min runs across many client
	// counts and several targets can take >2 hours.
	ctx := utils.WithTimeout(t, 4*time.Hour)

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

	wantPostgres, wantMultigateway, wantPgbouncer := parseTargets(t)

	// Optionally bump postgres max_connections so high-client-count direct
	// pgbench / pgbouncer runs don't exhaust the default of 60. This
	// restarts every pgctld in the cluster, so do it before init.
	if raw := os.Getenv("PGBENCH_PG_MAX_CONNECTIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid PGBENCH_PG_MAX_CONNECTIONS=%q: %v", raw, err)
		}
		bumpPostgresMaxConnections(ctx, t, setup, n)
		// Wait briefly for multipooler to reconnect after pgctld restart.
		time.Sleep(2 * time.Second)
	}

	var targets []suiteutil.Target
	if wantPostgres {
		targets = append(targets, pgTarget)
	}
	if wantMultigateway {
		targets = append(targets, mgwTarget)
	}
	if wantPgbouncer {
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
			t.Logf("pgbouncer requested but not available: %v", err)
		}
	}
	if len(targets) == 0 {
		t.Fatalf("no benchmark targets selected (PGBENCH_TARGETS=%q)", os.Getenv("PGBENCH_TARGETS"))
	}
	t.Logf("Benchmarking targets: %s", targetNames(targets))

	runner := NewPgBenchRunner(t)
	duration := ParseDuration(t)
	clients := ParseClients(t)
	scenarios := DefaultScenarios(duration, clients)

	// Initialize pgbench tables once via direct postgres. All routes share
	// the same backend, so tables are visible from every target.
	t.Run("init", func(t *testing.T) {
		if err := runner.Initialize(ctx, t, pgTarget, 10); err != nil {
			t.Fatalf("pgbench initialization failed: %v", err)
		}
	})

	capturePprof := os.Getenv("CAPTURE_PPROF") == "1"
	captureHeap := os.Getenv("CAPTURE_HEAP") == "1"
	if capturePprof {
		t.Logf("CAPTURE_PPROF=1: CPU profiles will be captured during each scenario")
	}
	if captureHeap {
		t.Logf("CAPTURE_HEAP=1: heap/allocs/goroutine profiles will be captured before+after each scenario")
	}

	// CPU sampler PID enumerators for each long-lived service. Multigateway
	// and multipooler PIDs are stable across the run; the postgres process
	// tree changes as backends come and go, so we re-read postmaster.pid
	// each tick.
	mgwPid := setup.Multigateway.Process.Process.Pid
	pgPidEnum := postgresPidEnumerator(setup.GetPrimary(t).Pgctld.PoolerDir + "/pg_data")

	var allResults []ScenarioResult
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			for _, target := range targets {
				t.Run(target.Name, func(t *testing.T) {
					runScenarioOnce(ctx, t, setup, runner, scenario, target,
						mgwPid, pgPidEnum, capturePprof, captureHeap, &allResults)
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

// runScenarioOnce executes one (scenario, target) tuple with optional
// CPU sampling, CPU profiling and heap/allocs/goroutine snapshots.
func runScenarioOnce(
	ctx context.Context,
	t *testing.T,
	setup *shardsetup.ShardSetup,
	runner *PgBenchRunner,
	scenario ScenarioConfig,
	target suiteutil.Target,
	mgwPid int,
	pgPidEnum pidEnumerator,
	capturePprof bool,
	captureHeap bool,
	allResults *[]ScenarioResult,
) {
	t.Helper()

	// Per-process CPU sampling runs every (scenario, target) regardless of
	// pprof flags — it's cheap and useful even on its own.
	mpPid := setup.GetPrimary(t).Multipooler.Process.Process.Pid
	samplers := []*cpuSampler{
		newCPUSampler("postgres", pgPidEnum, 500*time.Millisecond),
		newCPUSampler("multigateway", staticPidEnumerator(mgwPid), 500*time.Millisecond),
		newCPUSampler("multipooler_primary", staticPidEnumerator(mpPid), 500*time.Millisecond),
	}
	for _, s := range samplers {
		s.Start(ctx)
	}

	mgwAddr := fmt.Sprintf("localhost:%d", setup.Multigateway.HttpPort)
	mpAddr := fmt.Sprintf("localhost:%d", setup.GetPrimary(t).Multipooler.HttpPort)
	profileDir := filepath.Join(runner.OutputDir, "pprof", scenario.Name, target.Name)
	services := map[string]string{
		"multigateway":        mgwAddr,
		"multipooler_primary": mpAddr,
	}

	// Heap/allocs/goroutine snapshot before the run (point-in-time samples).
	if captureHeap {
		captureSnapshots(ctx, t, services, profileDir, "before")
	}

	var wg sync.WaitGroup
	if capturePprof {
		// pprof CPU profile must be shorter than the pgbench run. Leave a
		// 2-second margin on each side so the profile lands inside the
		// steady-state window.
		profileDur := scenario.Duration - 4
		if profileDur < 5 {
			profileDur = scenario.Duration
		}
		wg.Add(len(services))
		for label, addr := range services {
			go func(label, addr string) {
				defer wg.Done()
				out := filepath.Join(profileDir, "cpu-"+shortServiceName(label)+".pb.gz")
				if err := captureCPUProfile(ctx, addr, profileDur, out); err != nil {
					t.Logf("%s CPU profile failed: %v", label, err)
					return
				}
				t.Logf("%s CPU profile: %s", label, out)
			}(label, addr)
		}
	}

	result, err := runner.RunScenario(ctx, t, target, scenario)
	wg.Wait()

	if captureHeap {
		captureSnapshots(ctx, t, services, profileDir, "after")
	}

	// Stop samplers and persist CPU stats per (scenario, target).
	stats := make(map[string]CPUStats, len(samplers))
	for _, s := range samplers {
		st := s.Stop()
		stats[st.Label] = st
	}
	writeCPUStatsFile(t, runner.OutputDir, scenario.Name, target.Name, stats)
	for label, st := range stats {
		t.Logf("CPU %s/%s/%s: mean=%.1f%% max=%.1f%% (n=%d)",
			scenario.Name, target.Name, label,
			st.MeanPercent, st.MaxPercent, st.Samples)
	}

	if err != nil {
		t.Logf("SKIP: pgbench failed for %s/%s: %v", target.Name, scenario.Name, err)
		return
	}
	*allResults = append(*allResults, *result)
	t.Logf("TPS=%.1f  AvgLatency=%.2fms  P99=%.2fms  Txns=%d",
		result.TPS, result.LatencyAvg, result.LatencyP99, result.Transactions)
}

// captureSnapshots fetches heap, allocs, and goroutine profiles from each
// service and writes them under profileDir with the given suffix
// ("before" or "after"). Failures are logged, not fatal.
func captureSnapshots(ctx context.Context, t *testing.T, services map[string]string, profileDir, suffix string) {
	t.Helper()
	for label, addr := range services {
		short := shortServiceName(label)
		for kind, fn := range map[string]func(context.Context, string, string) error{
			"heap":      captureHeapProfile,
			"allocs":    captureAllocsProfile,
			"goroutine": captureGoroutineProfile,
		} {
			out := filepath.Join(profileDir, fmt.Sprintf("%s-%s-%s.pb.gz", short, kind, suffix))
			if err := fn(ctx, addr, out); err != nil {
				t.Logf("%s %s %s capture failed: %v", label, kind, suffix, err)
			}
		}
	}
}

func shortServiceName(label string) string {
	switch label {
	case "multipooler_primary":
		return "multipooler"
	default:
		return label
	}
}

// parseTargets reads PGBENCH_TARGETS and returns flags for each known
// target. Default is "multigateway" only. Unknown names are rejected.
func parseTargets(t *testing.T) (postgres, multigateway, pgbouncer bool) {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv("PGBENCH_TARGETS"))
	if raw == "" {
		return false, true, false
	}
	for name := range strings.SplitSeq(raw, ",") {
		switch strings.TrimSpace(name) {
		case "postgres":
			postgres = true
		case "multigateway":
			multigateway = true
		case "pgbouncer":
			pgbouncer = true
		case "":
			// allow trailing commas
		default:
			t.Fatalf("PGBENCH_TARGETS contains unknown target %q (valid: postgres, multigateway, pgbouncer)", name)
		}
	}
	return postgres, multigateway, pgbouncer
}

func targetNames(ts []suiteutil.Target) string {
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = t.Name
	}
	return strings.Join(names, ",")
}
