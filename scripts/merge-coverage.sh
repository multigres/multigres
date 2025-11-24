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

# Merge multiple Go coverage files into a single coverage file.
# Uses gocovmerge to properly handle overlapping coverage data.
#
# Usage: merge-coverage.sh input1.txt input2.txt ... output.txt
#
# Example:
#   merge-coverage.sh coverage-direct.txt coverage-subprocess.txt coverage.txt

set -euo pipefail

if [ $# -lt 3 ]; then
  echo "Usage: $0 <input1> <input2> [input3...] <output>" >&2
  echo "" >&2
  echo "Merges multiple Go coverage files into a single output file." >&2
  echo "" >&2
  echo "Example:" >&2
  echo "  $0 coverage-direct.txt coverage-subprocess.txt merged.txt" >&2
  exit 1
fi

# Last argument is output file
OUTPUT="${!#}"

# All arguments except last are input files
INPUTS=("${@:1:$#-1}")

# Verify all input files exist
for input in "${INPUTS[@]}"; do
  if [ ! -f "$input" ]; then
    echo "Error: Input file not found: $input" >&2
    exit 1
  fi
done

# Create temporary directory for filtered files
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Filter out generated parser code (postgres.y, yaccpar, yacctab) from each input file
# This avoids issues with go tool cover and potential merge conflicts from yacc-generated code
FILTERED_INPUTS=()
for i in "${!INPUTS[@]}"; do
  FILTERED="$TEMP_DIR/filtered-$i.txt"
  grep -v -e "/go/parser/postgres\.y:" -e "/go/parser/yaccpar:" -e "/go/parser/yacctab:" "${INPUTS[$i]}" >"$FILTERED" || true
  FILTERED_INPUTS+=("$FILTERED")
done

# Merge the filtered coverage files using gocovmerge
# Note: gocovmerge is installed via: go get -tool github.com/wadey/gocovmerge@latest
if ! go tool gocovmerge "${FILTERED_INPUTS[@]}" >"$OUTPUT"; then
  echo "Error: Failed to merge coverage files" >&2
  exit 1
fi

echo "Successfully merged ${#INPUTS[@]} coverage files into $OUTPUT (excluding generated parser code)"
