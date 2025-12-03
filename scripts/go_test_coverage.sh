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

# Development helper script for collecting comprehensive coverage data.
# Runs tests twice: once with direct coverage and once with subprocess coverage,
# then merges the results.
#
# Usage: go_test_coverage.sh [go test flags] <packages...>
#
# Examples:
#   ./scripts/go_test_coverage.sh ./go/test/endtoend
#   ./scripts/go_test_coverage.sh -v -run TestEndToEnd ./go/test/endtoend
#   ./scripts/go_test_coverage.sh ./...

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Generate unique run ID
RUN_ID="$(date +%Y%m%d-%H%M%S)"
COVERAGE_DIR="$PROJECT_ROOT/coverage/$RUN_ID"

# Coverage output files
DIRECT_COV="$COVERAGE_DIR/direct.txt"
SUBPROCESS_COV="$COVERAGE_DIR/subprocess.txt"
SUBPROCESS_RAWDIR="$COVERAGE_DIR/subprocess-raw"
MERGED_COV="$COVERAGE_DIR/merged.txt"

echo "=========================================="
echo "Comprehensive Coverage Test Runner"
echo "=========================================="
echo "Run ID: $RUN_ID"
echo "Coverage dir: $COVERAGE_DIR"
echo ""

# Create coverage directory
mkdir -p "$COVERAGE_DIR"
mkdir -p "$SUBPROCESS_RAWDIR"

# Check if coverage binaries exist
if [ ! -d "$PROJECT_ROOT/bin/cov" ]; then
  echo "Warning: Coverage binaries not found at bin/cov/"
  echo "Running: make build-coverage"
  echo ""
  cd "$PROJECT_ROOT"
  make build-coverage
fi

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Phase 1: Direct Test Coverage"
echo "=========================================="
echo "Running: go test -cover -coverprofile=$DIRECT_COV -coverpkg=./... $*"
echo ""

if go test -cover -covermode=atomic -coverprofile="$DIRECT_COV" -coverpkg=./... "$@"; then
  DIRECT_RESULT="✓ PASS"
  DIRECT_PCT=$(go tool cover -func="$DIRECT_COV" 2>&1 | tail -1 | grep -oE '[0-9]+\.[0-9]+%' || echo "N/A")
  echo ""
  echo "Direct coverage: $DIRECT_PCT"
else
  DIRECT_RESULT="✗ FAIL"
  DIRECT_PCT="N/A"
  echo ""
  echo "⚠️  Direct test run failed, but continuing with subprocess coverage..."
fi

echo ""
echo "=========================================="
echo "Phase 2: Subprocess Coverage"
echo "=========================================="
echo "Running: GOCOVERDIR=$SUBPROCESS_RAWDIR go test $*"
echo ""

# Export GOCOVERDIR so PrependBinToPath will use coverage binaries
export GOCOVERDIR="$SUBPROCESS_RAWDIR"

if go test "$@"; then
  SUBPROCESS_RESULT="✓ PASS"

  # Check if coverage was actually collected
  if [ -z "$(ls -A "$SUBPROCESS_RAWDIR")" ]; then
    echo "⚠️  Warning: No subprocess coverage files generated!"
    echo "    This may indicate:"
    echo "    - Tests don't spawn subprocess binaries"
    echo "    - PrependBinToPath() not using coverage binaries"
    SUBPROCESS_PCT="0%"
  else
    # Convert binary coverage to text format
    echo ""
    echo "Converting subprocess coverage to text format..."
    go tool covdata textfmt -i="$SUBPROCESS_RAWDIR" -o="$SUBPROCESS_COV"
    SUBPROCESS_PCT=$(go tool cover -func="$SUBPROCESS_COV" 2>&1 | tail -1 | grep -oE '[0-9]+\.[0-9]+%' || echo "N/A")
    echo "Subprocess coverage: $SUBPROCESS_PCT"
  fi
else
  SUBPROCESS_RESULT="✗ FAIL"
  SUBPROCESS_PCT="N/A"
  echo ""
  echo "⚠️  Subprocess test run failed"
fi

# Unset GOCOVERDIR
unset GOCOVERDIR

echo ""
echo "=========================================="
echo "Phase 3: Merge Coverage"
echo "=========================================="

# Check if we have coverage data to merge
HAS_DIRECT=false
HAS_SUBPROCESS=false

if [ -f "$DIRECT_COV" ] && [ -s "$DIRECT_COV" ]; then
  HAS_DIRECT=true
fi

if [ -f "$SUBPROCESS_COV" ] && [ -s "$SUBPROCESS_COV" ]; then
  HAS_SUBPROCESS=true
fi

if [ "$HAS_DIRECT" = true ] && [ "$HAS_SUBPROCESS" = true ]; then
  echo "Merging direct and subprocess coverage..."
  "$SCRIPT_DIR/merge-coverage.sh" "$DIRECT_COV" "$SUBPROCESS_COV" "$MERGED_COV"
  MERGED_PCT=$(go tool cover -func="$MERGED_COV" 2>&1 | tail -1 | grep -oE '[0-9]+\.[0-9]+%' || echo "N/A")
  echo "Merged coverage: $MERGED_PCT"
elif [ "$HAS_DIRECT" = true ]; then
  echo "Only direct coverage available, copying to merged output..."
  cp "$DIRECT_COV" "$MERGED_COV"
  MERGED_PCT="$DIRECT_PCT"
elif [ "$HAS_SUBPROCESS" = true ]; then
  echo "Only subprocess coverage available, copying to merged output..."
  cp "$SUBPROCESS_COV" "$MERGED_COV"
  MERGED_PCT="$SUBPROCESS_PCT"
else
  echo "⚠️  No coverage data collected!"
  MERGED_PCT="0%"
fi

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "Direct test:       $DIRECT_RESULT  ($DIRECT_PCT)"
echo "Subprocess test:   $SUBPROCESS_RESULT  ($SUBPROCESS_PCT)"
echo "Merged coverage:   $MERGED_PCT"
echo ""
echo "Coverage files:"
echo "  Direct:      $DIRECT_COV"
if [ "$HAS_SUBPROCESS" = true ]; then
  echo "  Subprocess:  $SUBPROCESS_COV"
fi
echo "  Merged:      $MERGED_COV"
echo ""
echo "View coverage:"
echo "  go tool cover -html=$MERGED_COV"
echo "=========================================="

# Exit with error if any test failed
if [ "$DIRECT_RESULT" = "✗ FAIL" ] || [ "$SUBPROCESS_RESULT" = "✗ FAIL" ]; then
  exit 1
fi
