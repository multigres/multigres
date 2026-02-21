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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// PostgresGitRepo is the official PostgreSQL git repository
	PostgresGitRepo = "https://github.com/postgres/postgres"

	// PostgresVersion is the git tag to checkout
	PostgresVersion = "REL_17_6"

	// PostgresCacheDir is the default cache directory for PostgreSQL source and builds
	PostgresCacheDir = "/tmp/multigres_pg_cache"
)

// PostgresBuilder manages PostgreSQL source checkout, build, and test execution
type PostgresBuilder struct {
	SourceDir  string // Shared source cache: /tmp/multigres_pg_cache/source/postgres
	BuildDir   string // Per-test build: /tmp/multigres_pg_cache/builds/<timestamp>/build
	InstallDir string // Per-test install: /tmp/multigres_pg_cache/builds/<timestamp>/install
	OutputDir  string // Persistent test results: /tmp/multigres_pg_cache/results/<timestamp>
}

// TestResults contains the results from running PostgreSQL regression tests
type TestResults struct {
	TotalTests     int
	PassedTests    int
	FailedTests    int
	SkippedTests   int
	Duration       time.Duration
	FailureDetails []TestFailure
	Tests          []IndividualTestResult // Per-test results in order
}

// IndividualTestResult represents a single test's result
type IndividualTestResult struct {
	Name     string `json:"name"`
	Status   string `json:"status"` // "pass", "fail", "skip"
	Duration string `json:"duration"`
}

// TestFailure represents a single test failure
type TestFailure struct {
	TestName string
	Error    string
}

// NewPostgresBuilder creates a new PostgresBuilder with unique build directories
func NewPostgresBuilder(t *testing.T) *PostgresBuilder {
	t.Helper()

	// Get cache directory from environment or use default
	cacheDir := os.Getenv("MULTIGRES_PG_CACHE_DIR")
	if cacheDir == "" {
		cacheDir = PostgresCacheDir
	}

	// Create unique build directory using timestamp
	timestamp := time.Now().Format("20060102-150405")
	buildRoot := filepath.Join(cacheDir, "builds", timestamp)

	return &PostgresBuilder{
		SourceDir:  filepath.Join(cacheDir, "source", "postgres"),
		BuildDir:   filepath.Join(buildRoot, "build"),
		InstallDir: filepath.Join(buildRoot, "install"),
		OutputDir:  filepath.Join(cacheDir, "results", timestamp), // Persistent results directory
	}
}

// CheckBuildDependencies verifies that required build tools are available
func CheckBuildDependencies(t *testing.T) error {
	t.Helper()

	required := []string{"make", "gcc"}
	var missing []string

	for _, tool := range required {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing build dependencies: %v. Install with: apt-get install build-essential", missing)
	}

	return nil
}

// EnsureSource ensures PostgreSQL source is available, cloning if necessary
func (pb *PostgresBuilder) EnsureSource(t *testing.T, ctx context.Context) error {
	t.Helper()

	// Check if cached source exists and is correct version
	if _, err := os.Stat(pb.SourceDir); err == nil {
		t.Logf("Found cached PostgreSQL source at %s, verifying version...", pb.SourceDir)

		// Verify it's the correct version
		cmd := exec.CommandContext(ctx, "git", "-C", pb.SourceDir, "describe", "--tags", "--exact-match")
		output, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(output)) == PostgresVersion {
			t.Logf("Using cached PostgreSQL source (version %s)", PostgresVersion)
			return nil
		}

		t.Logf("Cached source version mismatch or invalid, re-cloning...")
		if err := os.RemoveAll(pb.SourceDir); err != nil {
			return fmt.Errorf("failed to remove old cache: %w", err)
		}
	}

	// Create parent directory
	if err := os.MkdirAll(filepath.Dir(pb.SourceDir), 0o755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Clone PostgreSQL repository
	t.Logf("Cloning PostgreSQL %s from %s...", PostgresVersion, PostgresGitRepo)
	cmd := exec.CommandContext(ctx, "git", "clone",
		"--depth=1",
		"--branch", PostgresVersion,
		PostgresGitRepo,
		pb.SourceDir)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to clone PostgreSQL: %w (stderr: %s)", err, stderr.String())
	}

	t.Logf("Successfully cloned PostgreSQL %s", PostgresVersion)
	return nil
}

// Build builds PostgreSQL using traditional ./configure and make
func (pb *PostgresBuilder) Build(t *testing.T, ctx context.Context) error {
	t.Helper()

	// Create build directory
	if err := os.MkdirAll(pb.BuildDir, 0o755); err != nil {
		return fmt.Errorf("failed to create build directory: %w", err)
	}

	// Step 1: Run ./configure
	t.Logf("Configuring PostgreSQL with ./configure...")
	configureCmd := exec.CommandContext(ctx, filepath.Join(pb.SourceDir, "configure"),
		"--prefix="+pb.InstallDir,
		"--enable-cassert=no",
		"--enable-tap-tests=no",
		"--without-icu", // Disable ICU support to avoid dependency
	)
	configureCmd.Dir = pb.BuildDir
	configureCmd.Stdout = os.Stdout
	configureCmd.Stderr = os.Stderr

	if err := configureCmd.Run(); err != nil {
		return fmt.Errorf("configure failed: %w", err)
	}

	// Step 2: Run make
	t.Logf("Building PostgreSQL with make...")
	makeCmd := exec.CommandContext(ctx, "make", "-j", "4") // Use 4 parallel jobs
	makeCmd.Dir = pb.BuildDir
	makeCmd.Stdout = os.Stdout
	makeCmd.Stderr = os.Stderr

	if err := makeCmd.Run(); err != nil {
		return fmt.Errorf("make failed: %w", err)
	}

	// Step 3: Run make install
	t.Logf("Installing PostgreSQL to %s...", pb.InstallDir)
	installCmd := exec.CommandContext(ctx, "make", "install")
	installCmd.Dir = pb.BuildDir
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr

	if err := installCmd.Run(); err != nil {
		return fmt.Errorf("make install failed: %w", err)
	}

	t.Logf("PostgreSQL build completed successfully")
	return nil
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
func (pb *PostgresBuilder) runTestSuite(t *testing.T, cmd *exec.Cmd, cfg testSuiteConfig, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	// Create output directory for test results
	if err := os.MkdirAll(cfg.outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create %s output directory: %w", cfg.suiteName, err)
	}

	t.Logf("%s test results will be saved to: %s", cfg.suiteName, cfg.outputDir)

	// Start the process in its own process group so we can kill the entire tree
	// (make → pg_regress → psql) when the context expires. Without this,
	// exec.CommandContext only kills the direct child (make), leaving grandchildren
	// like pg_regress and psql alive, which prevents cmd.Run() from returning.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}

	// Set environment variables for connection
	cmd.Env = append(os.Environ(),
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
		"PGPASSWORD="+password,
		"PGDATABASE=postgres",
		"PGCONNECT_TIMEOUT=10",
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	// The regression.out file is written incrementally by pg_regress to the build
	// directory as tests complete. We use it as the primary source for parsing results
	// because it contains partial results even if the process hangs or is killed.
	srcOut := filepath.Join(cfg.srcOutDir, "regression.out")

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)

	t.Logf("%s test execution completed in %v (err=%v)", cfg.suiteName, duration, err)

	// Parse results from the on-disk regression.out file.
	outData, readErr := os.ReadFile(srcOut)
	if readErr != nil {
		return nil, fmt.Errorf("failed to read %s regression.out: %w", cfg.suiteName, readErr)
	}
	results, parseErr := pb.ParseTestResults(string(outData))
	if parseErr != nil {
		return nil, fmt.Errorf("failed to parse %s regression.out: %w", cfg.suiteName, parseErr)
	}
	results.Duration = duration

	// Copy artifacts to output directory
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

	cmd := exec.CommandContext(ctx, "make", makeArgs...)

	return pb.runTestSuite(t, cmd, testSuiteConfig{
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

	// Parse summary line
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

	// If we didn't find a summary but found individual results, calculate total
	if results.TotalTests == 0 && (results.PassedTests > 0 || results.FailedTests > 0) {
		results.TotalTests = results.PassedTests + results.FailedTests + results.SkippedTests
	}

	return results, nil
}

// SuiteResult holds results for one test suite along with the directory
// containing its regression.diffs file.
type SuiteResult struct {
	Name     string       // e.g. "Regression Tests", "Isolation Tests"
	Results  *TestResults // parsed TAP results
	DiffsDir string       // directory containing regression.diffs for this suite
}

// WriteMarkdownSummary generates a unified markdown report covering one or more
// test suites. It writes the report to pb.OutputDir/compatibility-report.md and
// appends it to GITHUB_STEP_SUMMARY when running in CI.
func (pb *PostgresBuilder) WriteMarkdownSummary(t *testing.T, suites []SuiteResult) (string, error) {
	t.Helper()

	if err := os.MkdirAll(pb.OutputDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	var sb strings.Builder

	// Header
	sb.WriteString("## PostgreSQL Compatibility Report\n\n")

	// Aggregate totals across all suites
	var totalPassed, totalFailed, totalAll int
	var totalDuration time.Duration
	for _, s := range suites {
		totalPassed += s.Results.PassedTests
		totalFailed += s.Results.FailedTests
		totalAll += s.Results.TotalTests
		totalDuration += s.Results.Duration
	}

	badgeColor := "red"
	if totalFailed == 0 && totalAll > 0 {
		badgeColor = "brightgreen"
	} else if totalPassed > 0 {
		pct := totalPassed * 100 / totalAll
		if pct >= 80 {
			badgeColor = "yellow"
		} else if pct >= 50 {
			badgeColor = "orange"
		}
	}
	sb.WriteString(fmt.Sprintf("**Status:** ![PG Compatibility](https://img.shields.io/badge/PG_Compatibility-%d%%2F%d_tests-%s)\n",
		totalPassed, totalAll, badgeColor))
	sb.WriteString(fmt.Sprintf("**PostgreSQL Version:** `%s`\n", PostgresVersion))
	sb.WriteString(fmt.Sprintf("**Duration:** %v\n", totalDuration.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("**Timestamp:** %s\n\n", time.Now().UTC().Format(time.RFC3339)))

	// Per-suite sections
	for _, s := range suites {
		sb.WriteString(fmt.Sprintf("### %s\n\n", s.Name))

		// Parse per-test diffs for inline display
		testDiffs := make(map[string]string)
		diffsPath := filepath.Join(s.DiffsDir, "regression.diffs")
		if data, err := os.ReadFile(diffsPath); err == nil && len(data) > 0 {
			testDiffs = splitDiffsByTest(string(data))
			t.Logf("Parsed %d per-test diffs from %s (%d bytes)", len(testDiffs), diffsPath, len(data))
		}

		sb.WriteString("```\n")
		sb.WriteString(fmt.Sprintf(" %-3s %-15s %-7s %s\n", "#", "Test", "Status", "Duration"))
		sb.WriteString(fmt.Sprintf(" %-3s %-15s %-7s %s\n", "---", "---------------", "-------", "--------"))

		inCodeBlock := true

		const maxDiffBytes = 2000 // truncate individual diffs to keep report small

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

			if !inCodeBlock {
				sb.WriteString("```\n")
				inCodeBlock = true
			}

			sb.WriteString(fmt.Sprintf(" %2d  %-15s %-7s %8s\n", i+1, test.Name, status, duration))

			if test.Status == "fail" {
				if diff, ok := testDiffs[test.Name]; ok {
					truncated := diff
					if len(truncated) > maxDiffBytes {
						truncated = truncated[:maxDiffBytes] + "\n... (truncated)"
					}
					sb.WriteString("```\n")
					inCodeBlock = false
					sb.WriteString(fmt.Sprintf("<details>\n<summary>Show diff</summary>\n\n```diff\n%s\n```\n</details>\n\n", truncated))
				}
			}
		}

		if inCodeBlock {
			sb.WriteString("```\n")
		}
		sb.WriteString("\n")
	}

	summary := sb.String()

	// Write to file
	summaryPath := filepath.Join(pb.OutputDir, "compatibility-report.md")
	if err := os.WriteFile(summaryPath, []byte(summary), 0o644); err != nil {
		return summary, fmt.Errorf("failed to write markdown summary: %w", err)
	}

	t.Logf("Markdown summary written to: %s", summaryPath)

	// Write to GitHub Actions job summary if available.
	// GitHub limits GITHUB_STEP_SUMMARY to 1 MB. If the full report exceeds
	// the limit, write a truncated version with only the results table
	// (no per-test diffs).
	const maxStepSummaryBytes = 900 * 1024 // 900 KB, with headroom for other steps
	if summaryFile := os.Getenv("GITHUB_STEP_SUMMARY"); summaryFile != "" {
		ciSummary := summary
		if len(ciSummary) > maxStepSummaryBytes {
			t.Logf("Full report is %d bytes, exceeds %d byte limit for GITHUB_STEP_SUMMARY; writing table-only version",
				len(summary), maxStepSummaryBytes)
			ciSummary = pb.buildTableOnlySummary(suites, totalPassed, totalAll, totalDuration, badgeColor)
		}
		f, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.WriteString(ciSummary)
			f.Close()
			t.Logf("Written to GitHub Actions job summary (%d bytes)", len(ciSummary))
		}
	}

	return summary, nil
}

// buildTableOnlySummary generates a compact markdown report without per-test
// diffs, used when the full report would exceed the GITHUB_STEP_SUMMARY limit.
func (pb *PostgresBuilder) buildTableOnlySummary(suites []SuiteResult, totalPassed, totalAll int, totalDuration time.Duration, badgeColor string) string {
	var sb strings.Builder

	sb.WriteString("## PostgreSQL Compatibility Report\n\n")
	sb.WriteString(fmt.Sprintf("**Status:** ![PG Compatibility](https://img.shields.io/badge/PG_Compatibility-%d%%2F%d_tests-%s)\n",
		totalPassed, totalAll, badgeColor))
	sb.WriteString(fmt.Sprintf("**PostgreSQL Version:** `%s`\n", PostgresVersion))
	sb.WriteString(fmt.Sprintf("**Duration:** %v\n", totalDuration.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("**Timestamp:** %s\n\n", time.Now().UTC().Format(time.RFC3339)))
	sb.WriteString("> **Note:** Per-test diffs omitted to fit GitHub step summary size limit. See the full report in CI artifacts.\n\n")

	for _, s := range suites {
		sb.WriteString(fmt.Sprintf("### %s\n\n", s.Name))
		sb.WriteString("```\n")
		sb.WriteString(fmt.Sprintf(" %-3s %-15s %-7s %s\n", "#", "Test", "Status", "Duration"))
		sb.WriteString(fmt.Sprintf(" %-3s %-15s %-7s %s\n", "---", "---------------", "-------", "--------"))

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
			sb.WriteString(fmt.Sprintf(" %2d  %-15s %-7s %8s\n", i+1, test.Name, status, duration))
		}
		sb.WriteString("```\n\n")
	}

	return sb.String()
}

// splitDiffsByTest splits a combined regression.diffs file into per-test diffs.
// Each test's diff starts with "diff -U3 .../expected/<testname>.out .../results/<testname>.out"
func splitDiffsByTest(allDiffs string) map[string]string {
	result := make(map[string]string)
	lines := strings.Split(allDiffs, "\n")

	var currentTest string
	var currentLines []string

	for _, line := range lines {
		if strings.HasPrefix(line, "diff -U3 ") {
			// Save previous test's diff
			if currentTest != "" {
				result[currentTest] = strings.Join(currentLines, "\n")
			}
			// Extract test name from path like .../expected/<testname>.out
			parts := strings.Split(line, "/")
			for i, p := range parts {
				if p == "expected" && i+1 < len(parts) {
					name := parts[i+1]
					// Remove anything after space (the second path in diff header)
					if idx := strings.Index(name, " "); idx >= 0 {
						name = name[:idx]
					}
					name = strings.TrimSuffix(name, ".out")
					currentTest = name
					break
				}
			}
			currentLines = []string{line}
		} else if currentTest != "" {
			currentLines = append(currentLines, line)
		}
	}
	// Save last test
	if currentTest != "" {
		result[currentTest] = strings.Join(currentLines, "\n")
	}

	return result
}

// BuildIsolation builds the PostgreSQL isolation test tools (isolationtester and
// pg_isolation_regress). Must be called after Build().
func (pb *PostgresBuilder) BuildIsolation(t *testing.T, ctx context.Context) error {
	t.Helper()

	isolationDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")

	t.Logf("Building isolation test tools in %s...", isolationDir)
	cmd := exec.CommandContext(ctx, "make", "-C", isolationDir, "all")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

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

	isolationDir := filepath.Join(pb.BuildDir, "src", "test", "isolation")
	outputIsoDir := filepath.Join(isolationDir, "output_iso")

	// Ensure the output_iso directory exists. make installcheck creates it, but
	// pg_isolation_regress (used for selective tests) may not.
	if err := os.MkdirAll(outputIsoDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output_iso directory: %w", err)
	}

	var cmd *exec.Cmd
	if testsEnv := os.Getenv("PGISOLATION_TESTS"); testsEnv != "" {
		pgIsoRegress := filepath.Join(isolationDir, "pg_isolation_regress")
		args := []string{
			"--inputdir=" + isolationDir,
			"--outputdir=" + outputIsoDir,
			"--host=localhost",
			fmt.Sprintf("--port=%d", multigatewayPort),
			"--user=postgres",
			"--dbname=postgres",
			"--use-existing",
			"--dlpath=" + isolationDir,
		}
		args = append(args, strings.Fields(testsEnv)...)
		cmd = exec.CommandContext(ctx, pgIsoRegress, args...)
		t.Logf("Running selective isolation tests: %s", testsEnv)
	} else {
		cmd = exec.CommandContext(ctx, "make", "-C", isolationDir, "installcheck")
		t.Logf("Running full PostgreSQL isolation test suite (installcheck)")
	}

	return pb.runTestSuite(t, cmd, testSuiteConfig{
		suiteName: "Isolation",
		outputDir: filepath.Join(pb.OutputDir, "isolation"),
		srcOutDir: outputIsoDir,
	}, multigatewayPort, password)
}

// Cleanup removes build artifacts but preserves source cache
func (pb *PostgresBuilder) Cleanup() {
	// Remove the build root directory (contains both build and install directories)
	if pb.BuildDir != "" {
		buildRoot := filepath.Dir(pb.BuildDir) // Go up from build/ to builds/<timestamp>/
		_ = os.RemoveAll(buildRoot)            // Best-effort cleanup
	}

	// Keep source cache for next run
}
