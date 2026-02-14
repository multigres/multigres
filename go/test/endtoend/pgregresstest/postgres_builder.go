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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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

// RegressionTests is the list of PostgreSQL regression tests to run, following the
// order from src/test/regress/parallel_schedule. Tests are added incrementally as
// multigres compatibility improves.
//
// Group 1: basic types and operators
var RegressionTests = []string{
	// Required setup
	"test_setup",

	// Group 1: basic types
	"boolean", "char", "name", "varchar", "text",
	"int2", "int4", "int8", "oid",
	"float4", "float8",
	"bit", "numeric", "txid", "uuid", "enum",
	"money", "rangetypes", "pg_lsn", "regproc",
}

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
func NewPostgresBuilder(t *testing.T, baseTempDir string) *PostgresBuilder {
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

// RunRegressionTests runs PostgreSQL regression tests against multigateway
func (pb *PostgresBuilder) RunRegressionTests(t *testing.T, ctx context.Context, multigatewayPort int, password string) (*TestResults, error) {
	t.Helper()

	t.Logf("Running PostgreSQL regression tests against multigateway on port %d...", multigatewayPort)

	// Create output directory for test results (persistent, outside build dir)
	if err := os.MkdirAll(pb.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	t.Logf("Test results will be saved to: %s", pb.OutputDir)

	// Use make installcheck-tests with TESTS variable to run specific regression tests
	// against the existing PostgreSQL server (multigateway).
	//
	// The installcheck-tests target runs specific tests against an already-running
	// PostgreSQL server, unlike installcheck which runs the entire parallel_schedule.
	//
	// From PostgreSQL's src/test/regress/GNUmakefile:
	//   installcheck: runs --schedule=parallel_schedule (all tests)
	//   installcheck-tests: runs $(TESTS) (specific tests only)
	//
	// Examples:
	//
	// 1. Run specific tests:
	//    make installcheck-tests TESTS="boolean char"
	//
	// 2. Run a single test:
	//    make installcheck-tests TESTS="boolean"
	//
	// 3. Run all tests (use installcheck instead):
	//    make installcheck
	//
	// Environment variables that pg_regress reads:
	//   PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE - connection params
	//
	// Reference: https://github.com/postgres/postgres/blob/master/src/test/regress/GNUmakefile

	cmd := exec.CommandContext(ctx, "make",
		"-C", filepath.Join(pb.BuildDir, "src/test/regress"), // Regress directory
		"installcheck-tests", // Target for running specific tests against existing server
		"TESTS="+strings.Join(RegressionTests, " "),
	)

	// Set environment variables for connection
	cmd.Env = append(os.Environ(),
		"PGHOST=localhost",
		fmt.Sprintf("PGPORT=%d", multigatewayPort),
		"PGUSER=postgres",
		"PGPASSWORD="+password,
		"PGDATABASE=postgres",
		"PGCONNECT_TIMEOUT=10",
	)

	// Capture output
	var stdout, stderr bytes.Buffer
	cmd.Stdout = io.MultiWriter(&stdout, os.Stdout)
	cmd.Stderr = io.MultiWriter(&stderr, os.Stderr)

	startTime := time.Now()
	err := cmd.Run()
	duration := time.Since(startTime)

	// Parse results even if command failed (some tests may have passed)
	results, parseErr := pb.ParseTestResults(&stdout, &stderr)
	if parseErr != nil {
		return nil, fmt.Errorf("failed to parse test results: %w", parseErr)
	}
	results.Duration = duration

	t.Logf("Test execution completed in %v", duration)

	// Copy regression test result files from build directory to persistent OutputDir
	// The make installcheck-tests target writes results to <builddir>/src/test/regress/
	buildRegressDir := filepath.Join(pb.BuildDir, "src", "test", "regress")

	// Define source and destination paths
	srcDiffs := filepath.Join(buildRegressDir, "regression.diffs")
	srcOut := filepath.Join(buildRegressDir, "regression.out")
	regressionDiffs := filepath.Join(pb.OutputDir, "regression.diffs")
	regressionOut := filepath.Join(pb.OutputDir, "regression.out")

	// Copy regression.diffs if it exists
	if diffsData, err := os.ReadFile(srcDiffs); err == nil {
		if err := os.WriteFile(regressionDiffs, diffsData, 0o644); err != nil {
			t.Logf("Warning: Failed to copy regression.diffs: %v", err)
		}
	}

	// Copy regression.out if it exists
	if outData, err := os.ReadFile(srcOut); err == nil {
		if err := os.WriteFile(regressionOut, outData, 0o644); err != nil {
			t.Logf("Warning: Failed to copy regression.out: %v", err)
		}
	}

	t.Logf("")
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("Test results saved to: %s", pb.OutputDir)
	t.Logf("  • Summary:     %s", regressionOut)
	t.Logf("  • Differences: %s", regressionDiffs)
	t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("")

	// If there are failures, read and display the regression.diffs content
	if results.FailedTests > 0 {
		if diffsContent, err := os.ReadFile(regressionDiffs); err == nil {
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("Regression Differences (from %s):", regressionDiffs)
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
			t.Logf("%s", string(diffsContent))
			t.Logf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		} else {
			t.Logf("Warning: Could not read regression.diffs: %v", err)
		}
	}

	// If tests ran, return results even if some failed
	if results.TotalTests > 0 {
		return results, err
	}

	// No tests ran - this is a test harness error
	if err != nil {
		return nil, fmt.Errorf("test harness failed to execute: %w", err)
	}

	return results, nil
}

// ParseTestResults parses pg_regress TAP output to extract test results.
// Returns an error if no TAP-formatted lines are found in the output.
func (pb *PostgresBuilder) ParseTestResults(stdout, stderr *bytes.Buffer) (*TestResults, error) {
	results := &TestResults{
		FailureDetails: []TestFailure{},
		Tests:          []IndividualTestResult{},
	}

	output := stdout.String()
	combinedOutput := output + "\n" + stderr.String()

	// Parse TAP format output from pg_regress
	// Example lines:
	//   ok 1         - test_setup                                178 ms
	//   ok 2         - boolean                                    61 ms
	//   not ok 3     - char                                       39 ms
	tapLine := regexp.MustCompile(`(?m)^(ok|not ok)\s+(\d+)\s+-\s+(\S+)\s+(\d+)\s+ms`)
	for _, match := range tapLine.FindAllStringSubmatch(combinedOutput, -1) {
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
		return nil, fmt.Errorf("no TAP-formatted test output found in pg_regress output")
	}

	// Parse summary line
	summaryPassPattern := regexp.MustCompile(`All (\d+) tests? passed`)
	summaryFailPattern := regexp.MustCompile(`(\d+) of (\d+) tests? failed`)

	if matches := summaryPassPattern.FindStringSubmatch(combinedOutput); len(matches) > 1 {
		total, _ := strconv.Atoi(matches[1])
		results.TotalTests = total
		results.PassedTests = total
		results.FailedTests = 0
	} else if matches := summaryFailPattern.FindStringSubmatch(combinedOutput); len(matches) > 2 {
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

// WriteMarkdownSummary generates a markdown summary suitable for GitHub Actions job summary
func (pb *PostgresBuilder) WriteMarkdownSummary(t *testing.T, results *TestResults) (string, error) {
	t.Helper()

	var sb strings.Builder

	// Header
	sb.WriteString("## PostgreSQL Compatibility Report\n\n")

	// Summary with shields.io badge
	badgeColor := "red"
	if results.FailedTests == 0 && results.TotalTests > 0 {
		badgeColor = "brightgreen"
	} else if results.PassedTests > 0 {
		pct := results.PassedTests * 100 / results.TotalTests
		if pct >= 80 {
			badgeColor = "yellow"
		} else if pct >= 50 {
			badgeColor = "orange"
		}
	}
	sb.WriteString(fmt.Sprintf("**Status:** ![PG Compatibility](https://img.shields.io/badge/PG_Compatibility-%d%%2F%d_tests-%s)\n",
		results.PassedTests, results.TotalTests, badgeColor))
	sb.WriteString(fmt.Sprintf("**PostgreSQL Version:** `%s`\n", PostgresVersion))
	sb.WriteString(fmt.Sprintf("**Duration:** %v\n", results.Duration.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("**Timestamp:** %s\n\n", time.Now().UTC().Format(time.RFC3339)))

	// Parse per-test diffs for inline display
	testDiffs := make(map[string]string)
	diffsPath := filepath.Join(pb.OutputDir, "regression.diffs")
	if data, err := os.ReadFile(diffsPath); err == nil && len(data) > 0 {
		testDiffs = splitDiffsByTest(string(data))
		t.Logf("Parsed %d per-test diffs from %s (%d bytes)", len(testDiffs), diffsPath, len(data))
	} else if err != nil {
		t.Logf("Warning: could not read diffs file %s: %v", diffsPath, err)
	}

	// Test results in aligned columnar format inside code blocks.
	// Code blocks break at each failure to allow an expandable <details> diff inline.
	sb.WriteString("### Test Results\n\n")

	// Header
	sb.WriteString("```\n")
	sb.WriteString(fmt.Sprintf(" %-3s %-15s %-7s %s\n", "#", "Test", "Status", "Duration"))
	sb.WriteString(fmt.Sprintf(" %-3s %-15s %-7s %s\n", "---", "---------------", "-------", "--------"))

	inCodeBlock := true

	for i, test := range results.Tests {
		status := "✅ ok"
		if test.Status == "fail" {
			status = "❌ FAIL"
		} else if test.Status == "skip" {
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

		// Break out of code block for expandable diff
		if test.Status == "fail" {
			if diff, ok := testDiffs[test.Name]; ok {
				sb.WriteString("```\n")
				inCodeBlock = false
				sb.WriteString(fmt.Sprintf("<details>\n<summary>Show diff</summary>\n\n```diff\n%s\n```\n</details>\n\n", diff))
			}
		}
	}

	if inCodeBlock {
		sb.WriteString("```\n")
	}

	summary := sb.String()

	// Write to file
	summaryPath := filepath.Join(pb.OutputDir, "compatibility-report.md")
	if err := os.WriteFile(summaryPath, []byte(summary), 0o644); err != nil {
		return summary, fmt.Errorf("failed to write markdown summary: %w", err)
	}

	t.Logf("Markdown summary written to: %s", summaryPath)

	// Write to GitHub Actions job summary if available
	if summaryFile := os.Getenv("GITHUB_STEP_SUMMARY"); summaryFile != "" {
		f, err := os.OpenFile(summaryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err == nil {
			_, _ = f.WriteString(summary)
			f.Close()
			t.Logf("Written to GitHub Actions job summary")
		}
	}

	return summary, nil
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

// Cleanup removes build artifacts but preserves source cache
func (pb *PostgresBuilder) Cleanup() {
	// Remove the build root directory (contains both build and install directories)
	if pb.BuildDir != "" {
		buildRoot := filepath.Dir(pb.BuildDir) // Go up from build/ to builds/<timestamp>/
		_ = os.RemoveAll(buildRoot)            // Best-effort cleanup
	}

	// Keep source cache for next run
}
