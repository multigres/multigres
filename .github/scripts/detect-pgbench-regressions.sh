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

# Compares two pgbench results.json files and detects TPS regressions.
# A regression is a >15% TPS drop for any scenario on the multigateway target.
#
# Usage: detect-pgbench-regressions.sh <baseline.json> <current.json> [output-dir]
#
# Exit codes:
#   0 - No regressions detected
#   1 - Regressions detected (regressions.json written to output-dir)
#   2 - Usage error or missing dependencies

set -euo pipefail

THRESHOLD=5  # percentage TPS drop that counts as a regression

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

# Compare multigateway TPS values between baseline and current.
# A regression is when TPS drops by more than THRESHOLD%.
regressions=$(jq -n \
  --slurpfile baseline "$BASELINE" \
  --slurpfile current "$CURRENT" \
  --argjson threshold "$THRESHOLD" \
  '
  # Build lookup: scenario -> tps for multigateway results in baseline
  ($baseline[0].results | map(select(.target == "multigateway")) |
    [.[] | {key: .scenario, value: .tps}] | from_entries) as $base_map |

  # Find scenarios where multigateway TPS dropped by more than threshold%
  [
    $current[0].results[] |
    select(.target == "multigateway") |
    . as $curr |
    $base_map[.scenario] as $base_tps |
    select($base_tps != null and $base_tps > 0) |
    (($base_tps - .tps) / $base_tps * 100) as $drop_pct |
    select($drop_pct > $threshold) |
    {
      scenario: .scenario,
      baseline_tps: $base_tps,
      current_tps: .tps,
      drop_pct: ($drop_pct * 10 | round / 10)
    }
  ]
')

count=$(echo "$regressions" | jq 'length')

if [[ "$count" -eq 0 ]]; then
  echo "No TPS regressions detected (threshold: ${THRESHOLD}%)."
  exit 0
fi

# Write regressions to file
echo "$regressions" | jq '.' >"$OUTPUT_DIR/regressions.json"

# Print human-readable summary
echo "=== TPS REGRESSIONS DETECTED ==="
echo ""
echo "$count scenario(s) regressed (>${THRESHOLD}% TPS drop vs baseline):"
echo ""
echo "$regressions" | jq -r '.[] | "  - \(.scenario): \(.baseline_tps | round) -> \(.current_tps | round) TPS (-\(.drop_pct)%)"'
echo ""

exit 1
