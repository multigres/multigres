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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/tools/executil"
)

const (
	sysbenchPSModePrepared = "prepared" // sysbench --db-ps-mode=auto
	sysbenchPSModeSimple   = "simple"   // sysbench --db-ps-mode=disable
)

// SysBenchScenario defines a single sysbench oltp_point_select scenario.
type SysBenchScenario struct {
	Name     string // e.g. "sustained_32c_prepared"
	Clients  int
	Duration int    // seconds
	PSMode   string // "prepared" or "simple"
}

// SysBenchRunner wraps the sysbench CLI: detection, prepare, scenario run,
// and stdout parsing.
type SysBenchRunner struct {
	binary    string
	OutputDir string

	// Tables / table-size used by both prepare and run. They MUST match
	// across phases — sysbench's run phase looks up the same sbtestN tables
	// it created in prepare.
	Tables    int
	TableSize int
}

// NewSysBenchRunner locates the sysbench binary, verifies pgsql driver
// support, and creates the output directory.
//
// If sysbench is missing or lacks pgsql, the test is skipped (not failed) —
// this matches how TestPgBench handles a missing pgbench binary.
func NewSysBenchRunner(t *testing.T) *SysBenchRunner {
	t.Helper()

	binary, err := exec.LookPath("sysbench")
	if err != nil {
		t.Skipf("sysbench binary not found: %v", err)
	}

	if err := checkSysBenchPgsqlDriver(binary); err != nil {
		t.Skipf("%v", err)
	}

	outputDir := strings.TrimSpace(os.Getenv("SYSBENCH_RESULTS_DIR"))
	if outputDir == "" {
		outputDir = filepath.Join("/tmp", "multigres_sysbench_results", time.Now().UTC().Format("20060102-150405"))
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("failed to create sysbench output directory: %v", err)
	}

	return &SysBenchRunner{
		binary:    binary,
		OutputDir: outputDir,
		Tables:    parseSysBenchInt(t, "SYSBENCH_TABLES", 4),
		TableSize: parseSysBenchInt(t, "SYSBENCH_TABLE_SIZE", 100000),
	}
}

// checkSysBenchPgsqlDriver returns nil if the sysbench binary supports the
// pgsql driver, otherwise returns an error with an actionable message.
//
// Detection probes by invoking sysbench with --db-driver=pgsql against an
// unreachable host. If the driver isn't compiled in, sysbench fails fast
// with "invalid database driver name". Any other failure (including the
// expected "Connection refused") means the driver is present.
func checkSysBenchPgsqlDriver(binary string) error {
	out, _ := exec.Command(binary,
		"--db-driver=pgsql",
		"--pgsql-host=127.0.0.1", "--pgsql-port=1",
		"oltp_point_select", "prepare",
	).CombinedOutput()
	if bytes.Contains(out, []byte("invalid database driver name")) {
		return errors.New(`sysbench is missing the pgsql driver. Build sysbench from source with
--with-pgsql, or install a distribution package that includes it
(Ubuntu: sysbench from sysbench-pgsql, macOS: brew install sysbench --with-pgsql
is unavailable; build from source: ./configure --with-pgsql).`)
	}
	return nil
}

// pgsqlConnArgs returns the --pgsql-* flags that identify a target.
func (r *SysBenchRunner) pgsqlConnArgs(target suiteutil.Target) []string {
	return []string{
		"--db-driver=pgsql",
		"--pgsql-host=" + target.Host,
		"--pgsql-port=" + strconv.Itoa(target.Port),
		"--pgsql-user=" + target.User,
		"--pgsql-password=" + target.Pass,
		"--pgsql-db=" + target.DB,
	}
}

// commonOltpArgs returns the workload args that are constant across phases.
func (r *SysBenchRunner) commonOltpArgs() []string {
	return []string{
		"oltp_point_select",
		"--tables=" + strconv.Itoa(r.Tables),
		"--table-size=" + strconv.Itoa(r.TableSize),
	}
}

// Prepare creates the sbtestN tables on the given target. Idempotent across
// runs only in the sense that we record stdout and surface errors loudly —
// sysbench itself errors if the tables already exist, which the caller can
// inspect in logs/sysbench-prepare.txt.
func (r *SysBenchRunner) Prepare(ctx context.Context, t *testing.T, target suiteutil.Target) error {
	t.Helper()

	args := append(r.pgsqlConnArgs(target), r.commonOltpArgs()...)
	args = append(args, "prepare")

	logPath := filepath.Join(r.OutputDir, "logs", "sysbench-prepare.txt")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("mkdir logs: %w", err)
	}

	out, err := executil.Command(ctx, r.binary, args...).CombinedOutput()
	_ = os.WriteFile(logPath, out, 0o644)
	if err != nil {
		t.Logf("sysbench prepare output:\n%s", string(out))
		return fmt.Errorf("sysbench prepare failed: %w", err)
	}
	t.Logf("sysbench prepared %d table(s) of %d rows on %s:%d (log: %s)",
		r.Tables, r.TableSize, target.Host, target.Port, logPath)
	return nil
}

// RunScenario executes one sysbench oltp_point_select scenario and parses
// the summary block from its stdout.
func (r *SysBenchRunner) RunScenario(ctx context.Context, t *testing.T, target suiteutil.Target, sc SysBenchScenario) (*ScenarioResult, error) {
	t.Helper()

	psFlag := "auto"
	if sc.PSMode == sysbenchPSModeSimple {
		psFlag = "disable"
	}

	args := append(r.pgsqlConnArgs(target), r.commonOltpArgs()...)
	args = append(args,
		"--threads="+strconv.Itoa(sc.Clients),
		"--time="+strconv.Itoa(sc.Duration),
		"--report-interval=5",
		// Force 99th-percentile reporting in the summary block. sysbench
		// 1.0.x defaults to 95 (which then ends up labelled "95th
		// percentile:" in the output and fails our regex).
		"--percentile=99",
		"--db-ps-mode="+psFlag,
		"run",
	)

	out, err := executil.Command(ctx, r.binary, args...).CombinedOutput()

	logPath := filepath.Join(r.OutputDir, "logs", "sysbench-"+sc.Name+".txt")
	_ = os.MkdirAll(filepath.Dir(logPath), 0o755)
	_ = os.WriteFile(logPath, out, 0o644)

	if err != nil {
		t.Logf("sysbench scenario %s output:\n%s", sc.Name, string(out))
		return nil, fmt.Errorf("sysbench run failed: %w", err)
	}

	res, parseErr := parseSysBenchOutput(out)
	if parseErr != nil {
		t.Logf("sysbench scenario %s output:\n%s", sc.Name, string(out))
		return nil, fmt.Errorf("parse sysbench output: %w", parseErr)
	}
	if res.TPS <= 0 || res.Transactions <= 0 {
		return nil, fmt.Errorf("sysbench reported zero throughput (tps=%.2f txns=%d) — workload failed silently",
			res.TPS, res.Transactions)
	}

	res.Target = target.Name
	res.Scenario = sc.Name
	res.Clients = sc.Clients
	res.Duration = sc.Duration
	res.PSMode = sc.PSMode
	return res, nil
}

// Sysbench summary lines we parse out of the stdout block. Examples (from
// sysbench 1.0.x and 1.1.x — both supported):
//
//	transactions:                        1940425 (32338.55 per sec.)
//	    avg:                                    0.99
//	    99th percentile:                        0.00
var (
	reTransactions = regexp.MustCompile(`transactions:\s+(\d+)\s+\(([\d.]+) per sec\.\)`)
	reAvgLatency   = regexp.MustCompile(`(?m)^\s*avg:\s+([\d.]+)`)
	reP99Latency   = regexp.MustCompile(`(?m)^\s*99th percentile:\s+([\d.]+)`)
)

// parseSysBenchOutput extracts TPS, transactions, avg latency and p99
// latency from sysbench's summary block. Returns an error if any required
// field is missing — we never want to silently record zero metrics.
func parseSysBenchOutput(out []byte) (*ScenarioResult, error) {
	res := &ScenarioResult{}

	if m := reTransactions.FindSubmatch(out); m != nil {
		txns, err := strconv.Atoi(string(m[1]))
		if err != nil {
			return nil, fmt.Errorf("parse transactions count %q: %w", m[1], err)
		}
		tps, err := strconv.ParseFloat(string(m[2]), 64)
		if err != nil {
			return nil, fmt.Errorf("parse tps %q: %w", m[2], err)
		}
		res.Transactions = txns
		res.TPS = tps
	} else {
		return nil, errors.New(`could not find "transactions: N (M per sec.)" line in sysbench output`)
	}

	if m := reAvgLatency.FindSubmatch(out); m != nil {
		v, err := strconv.ParseFloat(string(m[1]), 64)
		if err != nil {
			return nil, fmt.Errorf("parse avg latency %q: %w", m[1], err)
		}
		res.LatencyAvg = v
	} else {
		return nil, errors.New(`could not find "avg:" latency line in sysbench output`)
	}

	if m := reP99Latency.FindSubmatch(out); m != nil {
		v, err := strconv.ParseFloat(string(m[1]), 64)
		if err != nil {
			return nil, fmt.Errorf("parse p99 latency %q: %w", m[1], err)
		}
		res.LatencyP99 = v
	} else {
		return nil, errors.New(`could not find "99th percentile:" latency line in sysbench output`)
	}

	return res, nil
}

// DefaultSysBenchScenarios returns the cross-product of the env-driven
// client list and ps-mode list. Naming convention:
// `sustained_<clients>c_<ps_mode>` — distinct from the pgbench harness's
// `sustained_<clients>c_simple` scenario name to avoid output-dir collisions
// when both tests run in the same package.
func DefaultSysBenchScenarios(duration int, clientCounts []int, psModes []string) []SysBenchScenario {
	var out []SysBenchScenario
	for _, ps := range psModes {
		for _, c := range clientCounts {
			out = append(out, SysBenchScenario{
				Name:     fmt.Sprintf("sustained_%dc_%s", c, ps),
				Clients:  c,
				Duration: duration,
				PSMode:   ps,
			})
		}
	}
	return out
}

// CaptureSysBenchEnvironment fills the EnvironmentInfo block for a sysbench
// run. PlanCacheBytes is the multigateway --plan-cache-memory default; we
// hardcode it because the shared cluster setup doesn't override it.
func CaptureSysBenchEnvironment(t *testing.T, binary string) EnvironmentInfo {
	t.Helper()

	info := EnvironmentInfo{
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
		PlanCacheBytes: 4 * 1024 * 1024, // multigateway default; see go/services/multigateway/init.go
	}

	if out, err := exec.Command(binary, "--version").Output(); err == nil {
		info.SysBenchVersion = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("postgres", "--version").Output(); err == nil {
		info.PostgresVersion = strings.TrimSpace(string(out))
	}
	if sha := gitHeadSha(); sha != "" {
		info.MultigresVersion = "git-HEAD-" + sha
	}
	return info
}

// gitHeadSha returns the short HEAD sha of the repository the test binary
// was built from, or "" if `git` isn't available.
func gitHeadSha() string {
	out, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// parseSysBenchInt reads an optional positive int env var with a default.
func parseSysBenchInt(t *testing.T, name string, def int) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		t.Fatalf("invalid %s=%q (must be positive int)", name, raw)
	}
	return n
}

// ParseSysBenchClients reads SYSBENCH_CLIENTS as a comma-separated list of
// thread counts. Default: 1,8,32 — same as the handoff sweep.
func ParseSysBenchClients(t *testing.T) []int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("SYSBENCH_CLIENTS"))
	if raw == "" {
		return []int{1, 8, 32}
	}
	var out []int
	for s := range strings.SplitSeq(raw, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			t.Fatalf("invalid SYSBENCH_CLIENTS value %q: must be positive int", s)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		t.Fatalf("SYSBENCH_CLIENTS=%q produced no client counts", raw)
	}
	return out
}

// ParseSysBenchDuration reads SYSBENCH_DURATION (seconds, default 60).
func ParseSysBenchDuration(t *testing.T) int {
	t.Helper()
	return parseSysBenchInt(t, "SYSBENCH_DURATION", 60)
}

// ParseSysBenchPSModes reads SYSBENCH_PS_MODES; default is both
// {prepared,simple}. Unknown values fail the test loudly.
func ParseSysBenchPSModes(t *testing.T) []string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("SYSBENCH_PS_MODES"))
	if raw == "" {
		return []string{sysbenchPSModePrepared, sysbenchPSModeSimple}
	}
	var out []string
	for s := range strings.SplitSeq(raw, ",") {
		s = strings.TrimSpace(s)
		switch s {
		case sysbenchPSModePrepared, sysbenchPSModeSimple:
			out = append(out, s)
		case "":
		default:
			t.Fatalf("invalid SYSBENCH_PS_MODES value %q (valid: prepared, simple)", s)
		}
	}
	if len(out) == 0 {
		t.Fatalf("SYSBENCH_PS_MODES=%q produced no ps modes", raw)
	}
	return out
}

// ParseSysBenchTargets reads SYSBENCH_TARGETS; default is multigateway only.
func ParseSysBenchTargets(t *testing.T) (postgres, multigateway, pgbouncer bool) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("SYSBENCH_TARGETS"))
	if raw == "" {
		return false, true, false
	}
	for s := range strings.SplitSeq(raw, ",") {
		switch strings.TrimSpace(s) {
		case "postgres":
			postgres = true
		case "multigateway":
			multigateway = true
		case "pgbouncer":
			pgbouncer = true
		case "":
		default:
			t.Fatalf("SYSBENCH_TARGETS contains unknown target %q (valid: postgres, multigateway, pgbouncer)", s)
		}
	}
	return postgres, multigateway, pgbouncer
}

// ParseSysBenchQuiesce reads SYSBENCH_QUIESCE_SECONDS (default 5).
func ParseSysBenchQuiesce(t *testing.T) int {
	t.Helper()
	return parseSysBenchInt(t, "SYSBENCH_QUIESCE_SECONDS", 5)
}
