#!/usr/bin/env bash
# Copyright 2026 Supabase, Inc.
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

# Compares two pgregress results.json files and detects regressions.
# A regression is a test that was "pass" in the baseline but "fail" in the current run.
#
# Usage: detect-regressions.sh <baseline.json> <current.json> [output-dir]
#
# Exit codes:
#   0 - No regressions detected
#   1 - Regressions detected (regressions.json written to output-dir)
#   2 - Usage error or missing dependencies

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <baseline.json> <current.json> [output-dir]" >&2
  exit 2
fi

if ! command -v jq &>/dev/null; then
  echo "Error: jq is required but not installed" >&2
  exit 2
fi

BASELINE="$1"
CURRENT="$2"
OUTPUT_DIR="${3:-.}"

if [[ ! -f "$BASELINE" ]]; then
  echo "No baseline found at $BASELINE — skipping regression detection (first run)."
  exit 0
fi

if [[ ! -f "$CURRENT" ]]; then
  echo "Error: current results not found at $CURRENT" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

# Build a map of baseline test statuses: { "suite/test": "status" }
# Then compare against current results to find regressions.
regressions=$(jq -n \
  --slurpfile baseline "$BASELINE" \
  --slurpfile current "$CURRENT" \
  '
  # Build lookup: "SuiteName/TestName" -> status
  ($baseline[0] | [.[] | .name as $suite | .tests[] | {key: ($suite + "/" + .name), value: .status}] | from_entries) as $base_map |

  # Find tests that passed in baseline but fail now
  [
    $current[0][] | .name as $suite | .tests[] |
    select(.status == "fail") |
    ($suite + "/" + .name) as $key |
    select($base_map[$key] == "pass") |
    {suite: $suite, test: .name}
  ]
')

count=$(echo "$regressions" | jq 'length')

if [[ "$count" -eq 0 ]]; then
  echo "No regressions detected."
  exit 0
fi

# Write regressions to file
echo "$regressions" | jq '.' >"$OUTPUT_DIR/regressions.json"

# Print human-readable summary
echo "=== REGRESSIONS DETECTED ==="
echo ""
echo "$count test(s) regressed (were passing, now failing):"
echo ""
echo "$regressions" | jq -r '.[] | "  - [\(.suite)] \(.test)"'
echo ""

exit 1
