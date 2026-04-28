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
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/test/utils"
)

// TestSysBench drives sysbench oltp_point_select against multigateway (and
// optionally postgres / pgbouncer) under both prepared-statements-on
// (--db-ps-mode=auto) and prepared-statements-off (--db-ps-mode=disable)
// modes, capturing TPS / latency / per-process CPU / CPU pprofs.
//
// Skipped by default. Enable with:
//
//	RUN_BENCHMARKS=1 CAPTURE_PPROF=1 SYSBENCH_CLIENTS=1,8,32 SYSBENCH_DURATION=60 \
//	  go test -v -timeout 30m -run TestSysBench ./go/test/endtoend/queryserving/benchmarking/
//
// See SYSBENCH.md in this directory for the env-var reference and output layout.
func TestSysBench(t *testing.T) {
	suiteutil.SkipUnlessEnabled(t, suiteutil.EnvRunBenchmarks)

	runner := NewSysBenchRunner(t) // skips test if sysbench/pgsql missing

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 2*time.Hour)

	primary := setup.GetPrimary(t)
	pgTarget := suiteutil.Target{
		Name: "postgres",
		Host: "127.0.0.1",
		Port: primary.Pgctld.PgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}
	mgwTarget := suiteutil.Target{
		Name: "multigateway",
		Host: "127.0.0.1",
		Port: setup.MultigatewayPgPort,
		User: shardsetup.DefaultTestUser,
		Pass: shardsetup.TestPostgresPassword,
		DB:   "postgres",
	}

	wantPostgres, wantMultigateway, wantPgbouncer := ParseSysBenchTargets(t)

	// Bumping postgres max_connections is the same recipe pgbench uses; we
	// reuse the same env var (PGBENCH_PG_MAX_CONNECTIONS) so a single sweep
	// can flip both harnesses at once.
	if raw := os.Getenv("PGBENCH_PG_MAX_CONNECTIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("invalid PGBENCH_PG_MAX_CONNECTIONS=%q: %v", raw, err)
		}
		bumpPostgresMaxConnections(ctx, t, setup, n)
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
		pgb, err := NewPgBouncerInstance(t, "127.0.0.1", primary.Pgctld.PgPort,
			shardsetup.DefaultTestUser, shardsetup.TestPostgresPassword)
		if err == nil && pgb != nil {
			t.Cleanup(func() { pgb.Stop(t) })
			targets = append(targets, suiteutil.Target{
				Name: "pgbouncer",
				Host: "127.0.0.1",
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
		t.Fatalf("no benchmark targets selected (SYSBENCH_TARGETS=%q)", os.Getenv("SYSBENCH_TARGETS"))
	}
	t.Logf("Sysbench targets: %s", targetNames(targets))

	duration := ParseSysBenchDuration(t)
	clients := ParseSysBenchClients(t)
	psModes := ParseSysBenchPSModes(t)
	quiesce := ParseSysBenchQuiesce(t)
	scenarios := DefaultSysBenchScenarios(duration, clients, psModes)

	// Prepare runs once against postgres directly — sbtestN tables are
	// shared across all targets because the same backend serves them all.
	t.Run("prepare", func(t *testing.T) {
		if err := runner.Prepare(ctx, t, pgTarget); err != nil {
			t.Fatalf("sysbench prepare failed: %v", err)
		}
	})

	capturePprof := os.Getenv("CAPTURE_PPROF") == "1"
	captureHeap := os.Getenv("CAPTURE_HEAP") == "1"
	if capturePprof {
		t.Logf("CAPTURE_PPROF=1: CPU profiles will be captured during each scenario")
	}

	mgwPid := setup.Multigateway.Process.Process.Pid
	pgPidEnum := postgresPidEnumerator(setup.GetPrimary(t).Pgctld.PoolerDir + "/pg_data")

	var allResults []ScenarioResult
	for i, scenario := range scenarios {
		if i > 0 && quiesce > 0 {
			t.Logf("Quiesce %ds before next scenario", quiesce)
			time.Sleep(time.Duration(quiesce) * time.Second)
		}
		t.Run(scenario.Name, func(t *testing.T) {
			for _, target := range targets {
				t.Run(target.Name, func(t *testing.T) {
					runSysBenchScenarioOnce(ctx, t, setup, runner, scenario, target,
						mgwPid, pgPidEnum, capturePprof, captureHeap, &allResults)
				})
			}
		})
	}

	// Fail loudly if any scenario landed zero TPS — the parser explicitly
	// guards against this, but defence in depth.
	for _, r := range allResults {
		if r.TPS <= 0 {
			t.Fatalf("scenario %s/%s recorded zero TPS", r.Target, r.Scenario)
		}
	}

	report := &BenchmarkReport{
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		LoadTool:    "sysbench",
		Results:     allResults,
		Environment: CaptureSysBenchEnvironment(t, "sysbench"),
	}

	if jsonPath, err := WriteJSONReport(t, runner.OutputDir, report); err == nil {
		t.Logf("JSON report: %s", jsonPath)
	} else {
		t.Logf("Warning: failed to write JSON report: %v", err)
	}
	if mdPath, err := WriteSysBenchMarkdownReport(t, runner.OutputDir, report); err == nil {
		t.Logf("Markdown report: %s", mdPath)
	} else {
		t.Logf("Warning: failed to write markdown report: %v", err)
	}

	t.Logf("Sysbench bundle: %s", runner.OutputDir)
}

// runSysBenchScenarioOnce executes one (scenario, target) tuple with CPU
// sampling and optional pprof capture, mirroring runScenarioOnce for pgbench.
func runSysBenchScenarioOnce(
	ctx context.Context,
	t *testing.T,
	setup *shardsetup.ShardSetup,
	runner *SysBenchRunner,
	scenario SysBenchScenario,
	target suiteutil.Target,
	mgwPid int,
	pgPidEnum pidEnumerator,
	capturePprof bool,
	captureHeap bool,
	allResults *[]ScenarioResult,
) {
	t.Helper()

	mpPid := setup.GetPrimary(t).Multipooler.Process.Process.Pid
	samplers := []*cpuSampler{
		newCPUSampler("postgres", pgPidEnum, time.Second),
		newCPUSampler("multigateway", staticPidEnumerator(mgwPid), time.Second),
		newCPUSampler("multipooler_primary", staticPidEnumerator(mpPid), time.Second),
	}
	for _, s := range samplers {
		s.Start(ctx)
	}

	mgwAddr := fmt.Sprintf("127.0.0.1:%d", setup.Multigateway.HttpPort)
	mpAddr := fmt.Sprintf("127.0.0.1:%d", setup.GetPrimary(t).Multipooler.HttpPort)
	profileDir := filepath.Join(runner.OutputDir, "pprof", scenario.Name, target.Name)
	services := map[string]string{
		"multigateway":        mgwAddr,
		"multipooler_primary": mpAddr,
	}

	if captureHeap {
		captureSnapshots(ctx, t, services, profileDir, "before")
	}

	var wg sync.WaitGroup
	if capturePprof {
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
				if fi, err := os.Stat(out); err != nil || fi.Size() == 0 {
					t.Errorf("%s CPU profile %s is empty (size=%d, err=%v)", label, out, sizeOrZero(fi), err)
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

	stats := make(map[string]CPUStats, len(samplers))
	for _, s := range samplers {
		st := s.Stop()
		stats[st.Label] = st
	}
	// Match the vitess bundle's filename so cross-stack comparison tools can
	// glob `cpu/<scenario>/processes.json` regardless of which stack wrote it.
	writeSysBenchCPUStatsFile(t, runner.OutputDir, scenario.Name, stats)
	for label, st := range stats {
		t.Logf("CPU %s/%s/%s: mean=%.1f%% max=%.1f%% (n=%d)",
			scenario.Name, target.Name, label,
			st.MeanPercent, st.MaxPercent, st.Samples)
	}

	if err != nil {
		t.Errorf("FAIL: sysbench failed for %s/%s: %v", target.Name, scenario.Name, err)
		return
	}
	*allResults = append(*allResults, *result)
	t.Logf("TPS=%.1f  AvgLatency=%.2fms  P99=%.2fms  Txns=%d  ps_mode=%s",
		result.TPS, result.LatencyAvg, result.LatencyP99, result.Transactions, result.PSMode)
}

func sizeOrZero(fi os.FileInfo) int64 {
	if fi == nil {
		return 0
	}
	return fi.Size()
}
