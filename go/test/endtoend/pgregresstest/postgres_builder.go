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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/multigres/multigres/go/test/endtoend/pgbuilder"
	"github.com/multigres/multigres/go/tools/executil"
)

// Re-export pgbuilder constants so existing callers and scripts that reference
// pgregresstest.PostgresVersion / PostgresGitRepo keep working unchanged.
const (
	PostgresGitRepo  = pgbuilder.PostgresGitRepo
	PostgresVersion  = pgbuilder.PostgresVersion
	PostgresCacheDir = pgbuilder.PostgresCacheDir
)

// PostgresBuilder wraps pgbuilder.Builder with regression/isolation-suite
// specific helpers (run tests, parse TAP output, render reports).
//
// The source/build/install/cleanup logic lives in pgbuilder and is also used
// by the sqllogictest differential harness.
type PostgresBuilder struct {
	*pgbuilder.Builder
}

// TestResults contains the results from running PostgreSQL regression tests.
type TestResults struct {
	TotalTests     int
	PassedTests    int
	FailedTests    int
	SkippedTests   int
	TimedOut       bool // true if the suite was killed by context timeout
	Duration       time.Duration
	FailureDetails []TestFailure
	Tests          []IndividualTestResult // Per-test results in order
}

// IndividualTestResult represents a single test's result.
type IndividualTestResult struct {
	Name     string `json:"name"`
	Status   string `json:"status"` // "pass", "fail", "skip"
	Duration string `json:"duration"`
}

// TestFailure represents a single test failure.
type TestFailure struct {
	TestName string
	Error    string
}

// NewPostgresBuilder creates a new PostgresBuilder with unique build directories.
func NewPostgresBuilder(t *testing.T) *PostgresBuilder {
	t.Helper()
	return &PostgresBuilder{Builder: pgbuilder.New(t)}
}

// CheckBuildDependencies verifies that required build tools are available.
func CheckBuildDependencies(t *testing.T) error {
	return pgbuilder.CheckBuildDependencies(t)
}

// testSuiteConfig holds configuration for running a PostgreSQL test suite
// via the shared runTestSuite helper.
type testSuiteConfig struct {
	suiteName string // for log messages, e.g. "Regression" or "Isolation"
	outputDir string // where to copy regression.out and regression.diffs
	srcOutDir string // build directory containing regression.out and regression.diffs
}

// runTestSuite executes a pre-built test command and handles result parsing,
// artifact copying, and failure reporting. Both RunRegressionTests and
// RunIsolationTests delegate to this after constructing their command.
func (pb *PostgresBuilder) runTestSuite(t *testing.T, ctx context.Context, cmd *executil.Cmd, cfg testSuiteConfig, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create %s output directory: %w", cfg.suiteName, err)
	}

	t.Logf("%s test results will be saved to: %s", cfg.suiteName, cfg.outputDir)

	// Cap how long Run() waits for I/O after the process exits. Without this,
	// grandchildren (e.g. psql) holding pipes open would cause a hang.
	cmd.SetWaitDelay(10 * time.Second)

	cmd.AddEnv(
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
		"PGPASSWORD="+password,
		"PGDATABASE=postgres",
		"PGCONNECT_TIMEOUT=10",
	)

	// Capture stdout for result parsing while still printing to the terminal.
	// pg_regress deletes regression.out when all tests pass, so stdout is our
	// reliable source for TAP output. We still try the on-disk file first
	// because it contains partial results if the process is killed mid-run.
	var stdoutBuf bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdoutBuf)
	cmd.Stderr = os.Stderr

	srcOut := filepath.Join(cfg.srcOutDir, "regression.out")

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)

	t.Logf("%s test execution completed in %v (err=%v)", cfg.suiteName, duration, err)

	outData, readErr := os.ReadFile(srcOut)
	if readErr != nil {
		outData = stdoutBuf.Bytes()
	}
	if len(outData) == 0 {
		return nil, fmt.Errorf("no %s TAP output: regression.out missing and stdout empty", cfg.suiteName)
	}
	results, parseErr := pb.ParseTestResults(string(outData))
	if parseErr != nil {
		return nil, fmt.Errorf("failed to parse %s regression.out: %w", cfg.suiteName, parseErr)
	}
	results.Duration = duration
	results.TimedOut = ctx.Err() == context.DeadlineExceeded

	srcDiffs := filepath.Join(cfg.srcOutDir, "regression.diffs")
	dstDiffs := filepath.Join(cfg.outputDir, "regression.diffs")
	dstOut := filepath.Join(cfg.outputDir, "regression.out")

	if diffsData, err := os.ReadFile(srcDiffs); err == nil {
		if err := os.WriteFile(dstDiffs, diffsData, 0o644); err != nil {
			t.Logf("Warning: Failed to copy %s regression.diffs: %v", cfg.suiteName, err)
		}
	}

	if err := os.WriteFile(dstOut, outData, 0o644); err != nil {
		t.Logf("Warning: Failed to copy %s regression.out: %v", cfg.suiteName, err)
	}

	t.Logf("")
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("%s test results saved to: %s", cfg.suiteName, cfg.outputDir)
	t.Logf("  • Summary:     %s", dstOut)
	t.Logf("  • Differences: %s", dstDiffs)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("")

	if results.FailedTests > 0 {
		if diffsContent, err := os.ReadFile(dstDiffs); err == nil {
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("%s Differences (from %s):", cfg.suiteName, dstDiffs)
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("%s", string(diffsContent))
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		} else {
			t.Logf("Warning: Could not read %s regression.diffs: %v", cfg.suiteName, err)
		}
	}

	if results.TotalTests > 0 {
		return results, err
	}

	if err != nil {
		return nil, fmt.Errorf("%s test harness failed to execute: %w", cfg.suiteName, err)
	}

	return results, nil
}

// RunRegressionTests runs PostgreSQL regression tests against multigateway.
//
// Uses make installcheck-tests with TESTS variable to run specific regression tests
// against the existing PostgreSQL server (multigateway).
//
// The installcheck-tests target runs specific tests against an already-running
// PostgreSQL server, unlike installcheck which runs the entire parallel_schedule.
//
// From PostgreSQL's src/test/regress/GNUmakefile:
//
//	installcheck: runs --schedule=parallel_schedule (all tests)
//	installcheck-tests: runs $(TESTS) (specific tests only)
//
// Environment variables that pg_regress reads:
//
//	PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE - connection params
//
// Reference: https://github.com/postgres/postgres/blob/master/src/test/regress/GNUmakefile
func (pb *PostgresBuilder) RunRegressionTests(t *testing.T, ctx context.Context, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	t.Logf("Running PostgreSQL regression tests against multigateway on port %d...", multigatewayPort)

	regressDir := filepath.Join(pb.BuildDir, "src", "test", "regress")
	makeArgs := []string{"-C", regressDir}

	if testsEnv := os.Getenv("PGREGRESS_TESTS"); testsEnv != "" {
		makeArgs = append(makeArgs, "installcheck-tests", "TESTS="+testsEnv)
		t.Logf("Running selective regression tests: %s", testsEnv)
	} else {
		makeArgs = append(makeArgs, "installcheck")
		t.Logf("Running full PostgreSQL regression test suite (installcheck)")
	}

	cmd := executil.Command(ctx, "make", makeArgs...).WithProcessGroup()

	return pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
		suiteName: "Regression",
		outputDir: pb.OutputDir,
		srcOutDir: regressDir,
	}, multigatewayPort, password)
}

// ParseTestResults parses pg_regress TAP output to extract test results.
// Returns an error if no TAP-formatted lines are found in the output.
func (pb *PostgresBuilder) ParseTestResults(output string) (*TestResults, error) {
	results := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}

	// Parse TAP format output from pg_regress
	// Example lines:
	//   ok 1         - test_setup                                178 ms  (serial)
	//   ok 2         + boolean                                    61 ms  (parallel)
	//   not ok 3     + char                                       39 ms  (parallel)
	tapLine := regexp.MustCompile(`(?m)^(ok|not ok)\s+(\d+)\s+[-+]\s+(\S+)\s+(\d+)\s+ms`)
	for _, match := range tapLine.FindAllStringSubmatch(output, -1) {
		status := "pass"
		if match[1] == "not ok" {
			status = "fail"
		}
		results.Tests = append(results.Tests, IndividualTestResult{
			Name:     match[3],
			Status:   status,
			Duration: match[4] + "ms",
		})
		if status == "pass" {
			results.PassedTests++
		} else {
			results.FailedTests++
			results.FailureDetails = append(results.FailureDetails, TestFailure{
				TestName: match[3],
				Error:    "Test failed (see regression.diffs for details)",
			})
		}
	}

	if len(results.Tests) == 0 {
		return nil, errors.New("no TAP-formatted test output found in pg_regress output")
	}

	summaryPassPattern := regexp.MustCompile(`All (\d+) tests? passed`)
	summaryFailPattern := regexp.MustCompile(`(\d+) of (\d+) tests? failed`)

	if matches := summaryPassPattern.FindStringSubmatch(output); len(matches) > 1 {
		total, _ := strconv.Atoi(matches[1])
		results.TotalTests = total
		results.PassedTests = total
		results.FailedTests = 0
	} else if matches := summaryFailPattern.FindStringSubmatch(output); len(matches) > 2 {
		failed, _ := strconv.Atoi(matches[1])
		total, _ := strconv.Atoi(matches[2])
		results.TotalTests = total
		results.FailedTests = failed
		results.PassedTests = total - failed
	}

	if results.TotalTests == 0 && (results.PassedTests > 0 || results.FailedTests > 0) {
		results.TotalTests = results.PassedTests + results.FailedTests + results.SkippedTests
	}

	return results, nil
}

// CountScheduleTests parses a PostgreSQL schedule file and returns the number
// of tests listed. Each line starting with "test:" contains space-separated
// test names (parallel groups have multiple tests per line).
func CountScheduleTests(scheduleFile string) (int, error) {
	data, err := os.ReadFile(scheduleFile)
	if err != nil {
		return 0, err
	}
	count := 0
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, "test:"); ok {
			count += len(strings.Fields(rest))
		}
	}
	return count, nil
}

// SuiteResult holds results for one test suite.
type SuiteResult struct {
	Name          string       // e.g. "Regression Tests", "Isolation Tests"
	Results       *TestResults // parsed TAP results
	ExpectedTests int          // total tests from schedule file (0 = unknown)
}

// badgeColor returns a shields.io color based on the pass rate.
func badgeColor(passed, total int) string {
	if total == 0 {
		return "lightgrey"
	}
	if passed == total {
		return "brightgreen"
	}
	pct := passed * 100 / total
	switch {
	case pct >= 80:
		return "yellow"
	case pct >= 50:
		return "orange"
	default:
		return "red"
	}
}

// WriteMarkdownSummary generates a unified markdown report covering one or more
// test suites. It writes the report to pb.OutputDir/compatibility-report.md and
// appends it to GITHUB_STEP_SUMMARY when running in CI.
//
// Each suite gets its own badge showing pass rate and timeout status. Full diffs
// are available in the CI artifact (regression.diffs).
func (pb *PostgresBuilder) WriteMarkdownSummary(t *testing.T, suites []SuiteResult) (string, error) {
	t.Helper()

	if err := os.MkdirAll(pb.OutputDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	var sb strings.Builder

	sb.WriteString("## PostgreSQL Compatibility Report\n\n")

	for _, s := range suites {
		passed := s.Results.PassedTests
		ran := s.Results.TotalTests
		color := badgeColor(passed, ran)

		label := strings.TrimSuffix(s.Name, " Tests")
		value := fmt.Sprintf("%d%%2F%d_passed", passed, ran)
		if s.ExpectedTests > 0 && s.ExpectedTests > ran {
			value = fmt.Sprintf("%d%%2F%d_passed_(of_%d)", passed, ran, s.ExpectedTests)
		}
		if s.Results.TimedOut {
			value += "_(timed_out)"
			if color == "brightgreen" {
				color = "yellow"
			}
		}

		fmt.Fprintf(&sb, "![%s](https://img.shields.io/badge/%s-%s-%s) ", label, label, value, color)
	}
	sb.WriteString("\n\n")

	fmt.Fprintf(&sb, "**PostgreSQL Version:** `%s`\n", PostgresVersion)
	fmt.Fprintf(&sb, "**Timestamp:** %s\n\n", time.Now().UTC().Format(time.RFC3339))

	for _, s := range suites {
		fmt.Fprintf(&sb, "### %s\n\n", s.Name)

		if s.Results.TimedOut {
			ran := s.Results.TotalTests
			if s.ExpectedTests > 0 {
				fmt.Fprintf(&sb, "> **Timed out** — %d of %d scheduled tests executed before the deadline.\n\n", ran, s.ExpectedTests)
			} else {
				fmt.Fprintf(&sb, "> **Timed out** — %d tests executed before the deadline.\n\n", ran)
			}
		}

		sb.WriteString("| # | Test | Status | Duration |\n")
		sb.WriteString("|---|------|--------|----------|\n")

		for i, test := range s.Results.Tests {
			status := "✅ ok"
			switch test.Status {
			case "fail":
				status = "❌ FAIL"
			case "skip":
				status = "⏭️ skip"
			}
			duration := test.Duration
			if duration == "" {
				duration = "-"
			}
			fmt.Fprintf(&sb, "| %d | %s | %s | %s |\n", i+1, test.Name, status, duration)
		}
		sb.WriteString("\n")
	}

	summary := sb.String()

	summaryPath := filepath.Join(pb.OutputDir, "compatibility-report.md")
	if err := os.WriteFile(summaryPath, []byte(summary), 0o644); err != nil {
		return summary, fmt.Errorf("failed to write markdown summary: %w", err)
	}

	t.Logf("Markdown summary written to: %s", summaryPath)

	if summaryFile := os.Getenv("GITHUB_STEP_SUMMARY"); summaryFile != "" {
		f, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.WriteString(summary)
			f.Close()
			t.Logf("Written to GitHub Actions job summary (%d bytes)", len(summary))
		}
	}

	return summary, nil
}

// jsonSuiteResult is the JSON-serializable representation of a single test suite's results.
type jsonSuiteResult struct {
	Name  string                 `json:"name"`
	Tests []IndividualTestResult `json:"tests"`
}

// WriteJSONResults serializes suite results to pb.OutputDir/results.json.
// This file is consumed by CI scripts that compare runs to detect regressions.
func (pb *PostgresBuilder) WriteJSONResults(t *testing.T, suites []SuiteResult) (string, error) {
	t.Helper()

	if err := os.MkdirAll(pb.OutputDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	var out []jsonSuiteResult
	for _, s := range suites {
		out = append(out, jsonSuiteResult{
			Name:  s.Name,
			Tests: s.Results.Tests,
		})
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal results: %w", err)
	}

	resultsPath := filepath.Join(pb.OutputDir, "results.json")
	if err := os.WriteFile(resultsPath, data, 0o644); err != nil {
		return "", fmt.Errorf("failed to write results JSON: %w", err)
	}

	t.Logf("JSON results written to: %s", resultsPath)
	return resultsPath, nil
}

// BuildIsolation builds the PostgreSQL isolation test tools (isolationtester and
// pg_isolation_regress). Must be called after Build().
func (pb *PostgresBuilder) BuildIsolation(t *testing.T, ctx context.Context) error {
	t.Helper()

	isolationDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")

	t.Logf("Building isolation test tools in %s...", isolationDir)
	cmd := executil.Command(ctx, "make", "-C", isolationDir, "all")
	cmd.Cmd.Stdout = os.Stdout
	cmd.Cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("make isolation tools failed: %w", err)
	}

	t.Logf("Isolation test tools built successfully")
	return nil
}

// RunIsolationTests runs PostgreSQL isolation tests against multigateway.
// Isolation tests exercise multi-connection concurrency (deadlocks, serialization
// anomalies, lock contention, concurrent DDL) using isolationtester.
//
// The isolation Makefile has no installcheck-tests target, so for selective tests
// we invoke pg_isolation_regress directly with test names as positional args.
func (pb *PostgresBuilder) RunIsolationTests(t *testing.T, ctx context.Context, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	t.Logf("Running PostgreSQL isolation tests against multigateway on port %d...", multigatewayPort)

	isolationBuildDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")
	isolationSourceDir := filepath.Join(pb.SourceDir, "src", "test", "isolation")
	outputIsoDir := filepath.Join(isolationBuildDir, "output_iso")

	if err := os.MkdirAll(outputIsoDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output_iso directory: %w", err)
	}

	pgIsoRegress := filepath.Join(isolationBuildDir, "pg_isolation_regress")
	if _, err := os.Stat(pgIsoRegress); os.IsNotExist(err) {
		t.Logf("Building pg_isolation_regress...")
		buildCmd := executil.Command(ctx, "make", "-C", isolationBuildDir, "all")
		if out, err := buildCmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("failed to build pg_isolation_regress: %w\n%s", err, out)
		}
	}

	var cmd *executil.Cmd
	if testsEnv := os.Getenv("PGISOLATION_TESTS"); testsEnv != "" {
		args := []string{
			"--inputdir=" + isolationSourceDir,
			"--outputdir=" + outputIsoDir,
			"--host=localhost",
			fmt.Sprintf("--port=%d", multigatewayPort),
			"--user=postgres",
			"--dbname=postgres",
			"--use-existing",
			"--dlpath=" + isolationBuildDir,
		}
		args = append(args, strings.Fields(testsEnv)...)
		cmd = executil.Command(ctx, pgIsoRegress, args...).WithProcessGroup()
		t.Logf("Running selective isolation tests: %s", testsEnv)
	} else {
		cmd = executil.Command(ctx, "make", "-C", isolationBuildDir, "installcheck").WithProcessGroup()
		t.Logf("Running full PostgreSQL isolation test suite (installcheck)")
	}

	return pb.runTestSuite(t, ctx, cmd, testSuiteConfig{
		suiteName: "Isolation",
		outputDir: filepath.Join(pb.OutputDir, "isolation"),
		srcOutDir: outputIsoDir,
	}, multigatewayPort, password)
}
