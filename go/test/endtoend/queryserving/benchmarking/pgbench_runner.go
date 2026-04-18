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

// BenchmarkTarget identifies what pgbench connects to.
type BenchmarkTarget struct {
	Name string // "postgres", "multigateway", "pgbouncer"
	Host string
	Port int
	User string
	Pass string
	DB   string
}

// ScenarioResult holds parsed pgbench output for one scenario+target run.
type ScenarioResult struct {
	Target        string  `json:"target"`
	Scenario      string  `json:"scenario"`
	TPS           float64 `json:"tps"`
	TPSIncluding  float64 `json:"tps_including"`
	LatencyAvg    float64 `json:"latency_avg_ms"`
	LatencyStddev float64 `json:"latency_stddev_ms"`
	Clients       int     `json:"clients"`
	Duration      int     `json:"duration_seconds"`
	Protocol      string  `json:"protocol"`
	ConnChurn     bool    `json:"connection_churn"`
	Transactions  int     `json:"transactions"`
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

// Regex patterns for parsing pgbench output.
var (
	tpsExcludingRe  = regexp.MustCompile(`tps = ([\d.]+) \(without initial connection time\)`)
	tpsIncludingRe  = regexp.MustCompile(`tps = ([\d.]+) \(including initial connection time\)`)
	latencyAvgRe    = regexp.MustCompile(`latency average = ([\d.]+) ms`)
	latencyStddevRe = regexp.MustCompile(`latency stddev = ([\d.]+) ms`)
	transactionsRe  = regexp.MustCompile(`number of transactions actually processed: (\d+)`)
)

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
func (r *PgBenchRunner) Initialize(ctx context.Context, t *testing.T, target BenchmarkTarget, scaleFactor int) error {
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

// RunScenario executes a single pgbench scenario against the given target.
func (r *PgBenchRunner) RunScenario(ctx context.Context, t *testing.T, target BenchmarkTarget, scenario ScenarioConfig) (*ScenarioResult, error) {
	t.Helper()

	args := []string{
		"-h", target.Host,
		"-p", strconv.Itoa(target.Port),
		"-U", target.User,
		"-c", strconv.Itoa(scenario.Clients),
		"-T", strconv.Itoa(scenario.Duration),
		"-M", scenario.Protocol,
	}
	if scenario.ConnChurn {
		args = append(args, "-C")
	}
	args = append(args, target.DB)

	cmd := executil.Command(ctx, r.pgbenchBinary, args...).
		AddEnv("PGPASSWORD=" + target.Pass)

	output, err := cmd.CombinedOutput()
	outputStr := string(output)
	if err != nil {
		t.Logf("pgbench scenario %s/%s output:\n%s", scenario.Name, target.Name, outputStr)
		return nil, fmt.Errorf("pgbench failed: %w", err)
	}

	result, parseErr := parsePgBenchOutput(outputStr, scenario, target.Name)
	if parseErr != nil {
		t.Logf("pgbench output (failed to parse):\n%s", outputStr)
		return nil, fmt.Errorf("failed to parse pgbench output: %w", parseErr)
	}

	return result, nil
}

// parsePgBenchOutput extracts metrics from pgbench's text output.
func parsePgBenchOutput(output string, scenario ScenarioConfig, targetName string) (*ScenarioResult, error) {
	result := &ScenarioResult{
		Target:    targetName,
		Scenario:  scenario.Name,
		Clients:   scenario.Clients,
		Duration:  scenario.Duration,
		Protocol:  scenario.Protocol,
		ConnChurn: scenario.ConnChurn,
	}

	if m := tpsExcludingRe.FindStringSubmatch(output); len(m) > 1 {
		v, _ := strconv.ParseFloat(m[1], 64)
		result.TPS = v
	}
	if m := tpsIncludingRe.FindStringSubmatch(output); len(m) > 1 {
		v, _ := strconv.ParseFloat(m[1], 64)
		result.TPSIncluding = v
	}
	if m := latencyAvgRe.FindStringSubmatch(output); len(m) > 1 {
		v, _ := strconv.ParseFloat(m[1], 64)
		result.LatencyAvg = v
	}
	if m := latencyStddevRe.FindStringSubmatch(output); len(m) > 1 {
		v, _ := strconv.ParseFloat(m[1], 64)
		result.LatencyStddev = v
	}
	if m := transactionsRe.FindStringSubmatch(output); len(m) > 1 {
		v, _ := strconv.Atoi(m[1])
		result.Transactions = v
	}

	// TPS is the essential field; if we can't parse it, the output format may have changed.
	if result.TPS == 0 && result.TPSIncluding == 0 {
		return nil, errors.New("could not parse TPS from pgbench output")
	}

	return result, nil
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
func DefaultScenarios(duration int, clientCounts []int) []ScenarioConfig {
	var scenarios []ScenarioConfig

	for _, protocol := range []string{"simple", "extended"} {
		// Sustained load scenarios (all client counts)
		for _, clients := range clientCounts {
			scenarios = append(scenarios, ScenarioConfig{
				Name:     fmt.Sprintf("sustained_%dc_%s", clients, protocol),
				Clients:  clients,
				Duration: duration,
				Protocol: protocol,
			})
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
