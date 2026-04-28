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
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/suiteutil"
	"github.com/multigres/multigres/go/tools/executil"
)

// ScenarioConfig defines a single pgbench scenario to run.
type ScenarioConfig struct {
	Name      string // Human-readable name, e.g. "sustained_10c_extended"
	Clients   int    // -c flag
	Duration  int    // -T flag (seconds)
	Protocol  string // "simple" or "extended" (-M flag)
	ConnChurn bool   // -C flag (new connection per transaction)
}

// ScenarioResult holds computed metrics from a pgbench run.
type ScenarioResult struct {
	Target       string  `json:"target"`
	Scenario     string  `json:"scenario"`
	TPS          float64 `json:"tps"`
	LatencyAvg   float64 `json:"latency_avg_ms"`
	LatencyP50   float64 `json:"latency_p50_ms"`
	LatencyP99   float64 `json:"latency_p99_ms"`
	Clients      int     `json:"clients"`
	Duration     int     `json:"duration_seconds"`
	Protocol     string  `json:"protocol"`
	ConnChurn    bool    `json:"connection_churn"`
	Transactions int     `json:"transactions"`
}

// BenchmarkReport is the top-level report containing all results.
type BenchmarkReport struct {
	Timestamp   string           `json:"timestamp"`
	Results     []ScenarioResult `json:"results"`
	Environment EnvironmentInfo  `json:"environment"`
}

// EnvironmentInfo captures the test environment for reproducibility.
type EnvironmentInfo struct {
	PostgresVersion string `json:"postgres_version"`
	PgBenchVersion  string `json:"pgbench_version"`
	OS              string `json:"os"`
	GOMAXPROCS      int    `json:"gomaxprocs"`
}

// PgBenchRunner wraps pgbench execution: initialization, scenario runs, and output parsing.
type PgBenchRunner struct {
	pgbenchBinary string
	OutputDir     string
}

// NewPgBenchRunner creates a runner after locating the pgbench binary.
func NewPgBenchRunner(t *testing.T) *PgBenchRunner {
	t.Helper()

	binary, err := exec.LookPath("pgbench")
	if err != nil {
		t.Fatalf("pgbench binary not found: %v", err)
	}

	outputDir := filepath.Join("/tmp", "multigres_pgbench_results", time.Now().Format("20060102-150405"))
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("failed to create output directory: %v", err)
	}

	return &PgBenchRunner{
		pgbenchBinary: binary,
		OutputDir:     outputDir,
	}
}

// Initialize runs pgbench -i to create the benchmark tables.
func (r *PgBenchRunner) Initialize(ctx context.Context, t *testing.T, target suiteutil.Target, scaleFactor int) error {
	t.Helper()

	args := []string{
		"-i",
		"-s", strconv.Itoa(scaleFactor),
		"-h", target.Host,
		"-p", strconv.Itoa(target.Port),
		"-U", target.User,
		target.DB,
	}

	cmd := executil.Command(ctx, r.pgbenchBinary, args...).
		AddEnv("PGPASSWORD=" + target.Pass)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("pgbench init output:\n%s", string(output))
		return fmt.Errorf("pgbench init failed: %w", err)
	}
	t.Logf("pgbench initialized (scale=%d) on %s:%d", scaleFactor, target.Host, target.Port)
	return nil
}

// RunScenario executes a single pgbench scenario and computes metrics from
// the per-transaction log file (--log), avoiding fragile text output parsing.
//
// pgbench --log writes one line per completed transaction:
//
//	client_id  transaction_no  time_epoch_us  latency_us  schedule_lag_us
//
// We compute TPS, average latency, p50, and p99 directly from this data.
func (r *PgBenchRunner) RunScenario(ctx context.Context, t *testing.T, target suiteutil.Target, scenario ScenarioConfig) (*ScenarioResult, error) {
	t.Helper()

	// Create a unique log file prefix for this run inside the output directory.
	// Using output dir (not t.TempDir) so files survive for debugging if needed.
	logDir := filepath.Join(r.OutputDir, "logs", scenario.Name, target.Name)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create log dir: %w", err)
	}
	logPrefix := filepath.Join(logDir, "pgbench_log")

	args := []string{
		"-h", target.Host,
		"-p", strconv.Itoa(target.Port),
		"-U", target.User,
		"-c", strconv.Itoa(scenario.Clients),
		"-T", strconv.Itoa(scenario.Duration),
		"-M", scenario.Protocol,
		"--log", "--log-prefix", logPrefix,
	}
	if scenario.ConnChurn {
		args = append(args, "-C")
	}
	args = append(args, target.DB)

	cmd := executil.Command(ctx, r.pgbenchBinary, args...).
		AddEnv("PGPASSWORD=" + target.Pass)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("pgbench scenario %s/%s output:\n%s", scenario.Name, target.Name, string(output))
		return nil, fmt.Errorf("pgbench failed: %w", err)
	}

	// pgbench creates log files named <prefix>.<pid> (one thread) or
	// <prefix>.<pid>.<thread> (multi-thread). Glob for all of them.
	logFiles, err := filepath.Glob(logPrefix + ".*")
	if err != nil || len(logFiles) == 0 {
		return nil, errors.New("pgbench did not produce a transaction log file")
	}

	latencies, err := readTransactionLatencies(t, logFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction log: %w", err)
	}
	if len(latencies) == 0 {
		return nil, errors.New("transaction log is empty — no completed transactions")
	}

	return computeResult(latencies, scenario, target.Name), nil
}

// readTransactionLatencies reads per-transaction latency values (in microseconds)
// from one or more pgbench log files.
//
// Each line has the format: client_id  txn_no  latency_us  script_no  epoch_secs  epoch_usecs
// The latency field is at column index 2.
func readTransactionLatencies(t *testing.T, logFiles []string) ([]float64, error) {
	t.Helper()
	var latencies []float64

	for _, path := range logFiles {
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) < 3 {
				continue
			}
			usec, err := strconv.ParseFloat(fields[2], 64)
			if err != nil {
				continue
			}
			latencies = append(latencies, usec)
		}
		f.Close()

		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("scan %s: %w", path, err)
		}
	}

	return latencies, nil
}

// computeResult derives TPS, averages, and percentiles from raw latency data.
func computeResult(latencies []float64, scenario ScenarioConfig, targetName string) *ScenarioResult {
	n := len(latencies)

	// Average latency in microseconds.
	var sum float64
	for _, v := range latencies {
		sum += v
	}
	avgUs := sum / float64(n)

	// Sort for percentile computation.
	sort.Float64s(latencies)

	return &ScenarioResult{
		Target:       targetName,
		Scenario:     scenario.Name,
		TPS:          float64(n) / float64(scenario.Duration),
		LatencyAvg:   avgUs / 1000, // us → ms
		LatencyP50:   percentile(latencies, 50) / 1000,
		LatencyP99:   percentile(latencies, 99) / 1000,
		Clients:      scenario.Clients,
		Duration:     scenario.Duration,
		Protocol:     scenario.Protocol,
		ConnChurn:    scenario.ConnChurn,
		Transactions: n,
	}
}

// percentile returns the p-th percentile of a sorted slice using linear interpolation.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := (p / 100) * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// CaptureEnvironment collects environment info for the report.
func CaptureEnvironment(t *testing.T, pgbenchBinary string) EnvironmentInfo {
	t.Helper()

	info := EnvironmentInfo{
		OS:         runtime.GOOS,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}

	if out, err := exec.Command(pgbenchBinary, "--version").Output(); err == nil {
		info.PgBenchVersion = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("postgres", "--version").Output(); err == nil {
		info.PostgresVersion = strings.TrimSpace(string(out))
	}

	return info
}

// DefaultScenarios generates the cross-product of benchmark scenarios.
//
// PGBENCH_PROTOCOLS (comma-separated subset of "simple,extended", default
// both) restricts which protocols are exercised. PGBENCH_NO_CHURN=1 skips
// the churn variants — useful for scaling sweeps where churn is noise.
func DefaultScenarios(duration int, clientCounts []int) []ScenarioConfig {
	var scenarios []ScenarioConfig

	protocols := []string{"simple", "extended"}
	if raw := strings.TrimSpace(os.Getenv("PGBENCH_PROTOCOLS")); raw != "" {
		protocols = nil
		for s := range strings.SplitSeq(raw, ",") {
			s = strings.TrimSpace(s)
			if s == "simple" || s == "extended" {
				protocols = append(protocols, s)
			}
		}
	}
	includeChurn := os.Getenv("PGBENCH_NO_CHURN") != "1"

	for _, protocol := range protocols {
		// Sustained load scenarios (all client counts)
		for _, clients := range clientCounts {
			scenarios = append(scenarios, ScenarioConfig{
				Name:     fmt.Sprintf("sustained_%dc_%s", clients, protocol),
				Clients:  clients,
				Duration: duration,
				Protocol: protocol,
			})
		}

		if !includeChurn {
			continue
		}
		// Connection churn scenarios (limited client counts — churn with many clients is very slow)
		for _, clients := range clientCounts {
			if clients > 10 {
				continue
			}
			scenarios = append(scenarios, ScenarioConfig{
				Name:      fmt.Sprintf("churn_%dc_%s", clients, protocol),
				Clients:   clients,
				Duration:  duration,
				Protocol:  protocol,
				ConnChurn: true,
			})
		}
	}

	return scenarios
}

// ParseClients parses the PGBENCH_CLIENTS env var into a sorted list of client counts.
func ParseClients(t *testing.T) []int {
	t.Helper()

	raw := os.Getenv("PGBENCH_CLIENTS")
	if raw == "" {
		return []int{1, 10, 50}
	}

	var counts []int
	for s := range strings.SplitSeq(raw, ",") {
		s = strings.TrimSpace(s)
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("invalid PGBENCH_CLIENTS value %q: %v", s, err)
		}
		counts = append(counts, n)
	}
	return counts
}

// ParseDuration reads the PGBENCH_DURATION env var (default: 30 seconds).
func ParseDuration(t *testing.T) int {
	t.Helper()

	raw := os.Getenv("PGBENCH_DURATION")
	if raw == "" {
		return 30
	}
	d, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("invalid PGBENCH_DURATION value %q: %v", raw, err)
	}
	return d
}
