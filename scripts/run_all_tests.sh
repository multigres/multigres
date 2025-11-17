#!/usr/bin/env bash
# Copyright 2025 Supabase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# run_all_tests.sh - Comprehensive Go test runner with coverage
#
# USAGE:
#   ./run_all_tests.sh
#
# ENVIRONMENT VARIABLES:
#   COVERDIR       Directory for runtime coverage output (default: ./coverage)
#   OUTDIR         Directory for test artifacts (default: ./test_artifacts)
#   JSON_OUT       Path for merged JSON test output (default: ./all_tests.json)
#   BUILD_MAIN     Whether to build main binary (default: true, set "false" to skip)
#   COVERPKG       Coverage package scope (default: ./...)
#   ARTIFACT_DIR   Directory for GitHub Actions artifacts (default: test-artifacts-upload)
#
# OUTPUTS:
#   coverage.out            - Merged coverage profile (use with go tool cover)
#   all_tests.json          - JSON test output from all packages (for dorny/test-reporter)
#   test_artifacts/         - Directory with test binaries, profiles, JSON
#   test-artifacts-upload/  - Organized directory for CI artifact upload
#
# EXAMPLES:
#   # Run with default settings
#   ./run_all_tests.sh
#
#   # Skip main binary build (for library projects)
#   BUILD_MAIN=false ./run_all_tests.sh
#
#   # Custom coverage directory
#   COVERDIR=/tmp/coverage ./run_all_tests.sh
#
# GITHUB ACTIONS INTEGRATION:
#   - Automatically detects GitHub Actions environment
#   - Annotates test failures as errors with ::error::
#   - Creates organized artifact directory for upload
#   - JSON output compatible with dorny/test-reporter
#
# EXIT CODES:
#   0 - All tests passed (or no tests found)
#   1 - Test failures or build errors occurred
#
set -euo pipefail

# Configuration
BUILD_MAIN="${BUILD_MAIN:-true}"
COVERPKG="${COVERPKG:-./...}"
COVERDIR="${COVERDIR:-$PWD/coverage}"
OUTDIR="${OUTDIR:-$PWD/test_artifacts}"
JSON_OUT="${JSON_OUT:-$PWD/all_tests.json}"
ARTIFACT_DIR="${ARTIFACT_DIR:-test-artifacts-upload}"

# Track failures but continue execution to collect all coverage
declare -a failed_packages=()
has_errors=0

# Detect GitHub Actions
IN_GITHUB_ACTIONS="${GITHUB_ACTIONS:-false}"

echo "==> Go Coverage Test Runner"
echo "Build main: $BUILD_MAIN"
echo "Coverage scope: $COVERPKG"
echo ""

# Build main binary if requested
if [ "$BUILD_MAIN" = "true" ]; then
    echo "==> Building binaries"
    make build-coverage
else
    echo "==> Skipping binary builds (BUILD_MAIN=false)"
fi

# Clean and create output directories (removed ./merged as it's unused)
echo "==> Cleaning output directories"
rm -rf "$COVERDIR" "$OUTDIR" "$JSON_OUT" "$ARTIFACT_DIR"
mkdir -p "$COVERDIR" "$OUTDIR"/json "$OUTDIR"/bins "$OUTDIR"/profiles

# Discover packages with tests
echo "==> Discovering packages containing tests"
packages=$(go list ./... | while read -r pkg; do
    if go list -f '{{len .TestGoFiles}}' "$pkg" | grep -qv '^0$'; then
        echo "$pkg"
    fi
done)

if [ -z "$packages" ]; then
    echo "No test packages found."
    exit 0
fi

package_count=$(echo "$packages" | wc -l | tr -d ' ')
echo "Found $package_count package(s) with tests"
echo ""

# GitHub Actions: Start test group
if [ "$IN_GITHUB_ACTIONS" = "true" ]; then
    echo "::group::Compiling and running test binaries"
fi

# Compile and run tests
echo "==> Compiling and running test binaries with coverage instrumentation"
current=0
for pkg in $packages; do
    current=$((current + 1))
    bin="$OUTDIR/bins/$(echo "$pkg" | tr '/.' '_')"
    json="$OUTDIR/json/$(basename "$bin").json"
    coverprofile="$OUTDIR/profiles/$(basename "$bin").out"

    echo "[$current/$package_count] Building $pkg"

    # Compile test binary with coverage instrumentation
    if ! go test -c -cover -coverpkg "$COVERPKG" -o "$bin" "$pkg" 2>&1; then
        echo "ERROR: Failed to compile tests for $pkg"
        failed_packages+=("$pkg (build failed)")
        has_errors=1
        continue
    fi

    echo "[$current/$package_count] Running tests for $pkg"

    # Run tests from package directory to support relative paths in tests
    # GOCOVERDIR captures subprocess coverage (e.g., coverage-instrumented binaries)
    # -test.coverprofile captures direct test coverage (test package code)
    (
        pkg_dir=$(go list -f '{{.Dir}}' "$pkg")
        cd "$pkg_dir"

        # Run test binary, continue on failure to collect all test results
        # The || true ensures we continue even if tests fail
        if ! GOCOVERDIR="$COVERDIR" "$bin" -test.v -test.coverprofile="$coverprofile" 2>&1 \
            | go tool test2json -t > "$json"; then
            # Test execution failed, but output was captured to JSON
            :
        fi
    )

    # Check if any tests failed by examining the JSON output
    if grep -q '"Action":"fail"' "$json" 2>/dev/null; then
        failed_packages+=("$pkg")
        has_errors=1

        # GitHub Actions: Annotate failure for visibility in Actions UI
        if [ "$IN_GITHUB_ACTIONS" = "true" ]; then
            echo "::error::Tests failed in package: $pkg"
        fi
    fi
done

# GitHub Actions: End test group
if [ "$IN_GITHUB_ACTIONS" = "true" ]; then
    echo "::endgroup::"
fi

echo ""
echo "==> Merging test results"

# Merge JSON output (handle empty directory gracefully)
if compgen -G "$OUTDIR/json/*.json" > /dev/null 2>&1; then
    cat "$OUTDIR"/json/*.json > "$JSON_OUT"
    echo "Test JSON: $JSON_OUT"
else
    echo "WARNING: No JSON test output found"
    echo "" > "$JSON_OUT"
fi

# Merge coverage data
echo "==> Merging coverage data"

# Convert GOCOVERDIR (subprocess coverage) to text format
# Check if directory has files before attempting conversion
if [ -n "$(ls -A "$COVERDIR" 2>/dev/null)" ]; then
    go tool covdata textfmt -i="$COVERDIR" -o=subprocess.out
else
    echo "WARNING: No subprocess coverage data found in $COVERDIR"
    touch subprocess.out
fi

# Merge all coverage files into a single coverage.out
# This combines subprocess coverage with test package coverage
{
    echo "mode: set"
    # Add subprocess coverage if it exists and is not empty
    if [ -s subprocess.out ]; then
        tail -n +2 subprocess.out
    fi
    # Add test coverage profiles if they exist
    if compgen -G "$OUTDIR/profiles/*.out" > /dev/null 2>&1; then
        find "$OUTDIR/profiles" -name "*.out" -exec tail -n +2 {} \;
    fi
} > coverage.out

echo "Coverage report: coverage.out"

# Calculate coverage percentage for display
if [ -s coverage.out ] && [ "$(wc -l < coverage.out)" -gt 1 ]; then
    coverage_percent=$(go tool cover -func=coverage.out 2>/dev/null | grep total | awk '{print $3}' || echo "0.0%")
    echo "Total coverage: $coverage_percent"
else
    echo "WARNING: No coverage data available"
    coverage_percent="0.0%"
fi

# Create organized artifact directory for CI upload
echo "==> Creating artifact directory for CI upload"
mkdir -p "$ARTIFACT_DIR"

# Copy key files to artifact directory
cp coverage.out "$ARTIFACT_DIR/" 2>/dev/null || touch "$ARTIFACT_DIR/coverage.out"
cp "$JSON_OUT" "$ARTIFACT_DIR/" 2>/dev/null || touch "$ARTIFACT_DIR/all_tests.json"

# Generate HTML coverage report for easy viewing
if [ -s coverage.out ] && [ "$(wc -l < coverage.out)" -gt 1 ]; then
    go tool cover -html=coverage.out -o="$ARTIFACT_DIR/coverage.html"
    echo "HTML coverage report: $ARTIFACT_DIR/coverage.html"
fi

echo "Artifact directory: $ARTIFACT_DIR"

# Count test results from JSON
total_tests=$(grep -c '"Action":"pass"\|"Action":"fail"' "$JSON_OUT" 2>/dev/null || echo 0)
failed_tests=$(grep -c '"Action":"fail"' "$JSON_OUT" 2>/dev/null || echo 0)
passed_tests=$((total_tests - failed_tests))

# Summary
echo ""
echo "==> Done."
echo ""
echo "📊 Results:"
echo "   Tests: $passed_tests passed, $failed_tests failed (total: $total_tests)"
echo "   Coverage: $coverage_percent"
echo ""
echo "📁 Outputs:"
echo "   • coverage.out - Coverage profile"
echo "   • $JSON_OUT - JSON test results (for dorny/test-reporter)"
echo "   • $ARTIFACT_DIR/ - CI artifacts (ready for upload)"
echo ""
echo "🔍 View coverage:"
echo "   go tool cover -func=coverage.out"
echo "   go tool cover -html=coverage.out"
echo "   open $ARTIFACT_DIR/coverage.html"
echo ""

# Exit with error if any tests failed (fail-last approach)
if [ $has_errors -eq 1 ]; then
    echo "❌ Tests failed in ${#failed_packages[@]} package(s):"
    for pkg in "${failed_packages[@]}"; do
        echo "   - $pkg"
    done
    exit 1
fi

echo "✅ All tests passed!"
exit 0
