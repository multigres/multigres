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

set -euo pipefail

dir="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Each case supplies input.jsonl and expected.jsonl. Test the public wrapper,
# including its exit status, interrupted count, warning and complete output.
check() {
  echo "Checking $1"
  local count
  count="$(jq -R -n -r --arg mode interrupted -f "$dir/normalize-test-results.jq" "$work/input.jsonl")"
  [[ "$count" == "$2" ]]
  "$dir/normalize-test-results.sh" "$work/input.jsonl" >"$work/output.log" 2>&1
  jq -s -S . "$work/expected.jsonl" >"$work/expected.json"
  jq -s -S . "$work/input.jsonl" >"$work/actual.json"
  diff -u "$work/expected.json" "$work/actual.json"
  if [[ "$2" -gt 0 ]]; then
    grep -Fq "::warning::" "$work/output.log"
    grep -Fq "$2 test(s) had no terminal event and were marked failed (interrupted stream)" "$work/output.log"
  else
    [[ ! -s "$work/output.log" ]]
  fi
}

# Clean streams retain every event, including earlier failed executions.
cat >"$work/input.jsonl" <<'EOF'
{"Action":"run","Package":"p","Test":"TestA"}
{"Action":"fail","Package":"p","Test":"TestA","Elapsed":0.01}
{"Action":"run","Package":"p","Test":"TestA"}
{ "Action": "pass", "Package": "p", "Test": "TestA", "Elapsed": 0.02 }
EOF
cp "$work/input.jsonl" "$work/expected.jsonl"
check "clean stream unchanged" 0
cmp "$work/expected.jsonl" "$work/input.jsonl"

# Trailing output must preserve pass/fail/skip, keyed by package and test.
cat >"$work/input.jsonl" <<'EOF'
{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0.01}
{"Action":"fail","Package":"q","Test":"TestA","Elapsed":0.02}
{"Action":"skip","Package":"p","Test":"TestB","Elapsed":0}
{"Action":"output","Package":"p","Test":"TestA","Output":"late log\n"}
{"Action":"output","Package":"q","Test":"TestA","Output":"late log\n"}
{"Action":"output","Package":"p","Test":"TestB","Output":"late log\n"}
EOF
cp "$work/input.jsonl" "$work/expected.jsonl"
cat >>"$work/expected.jsonl" <<'EOF'
{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0.01}
{"Action":"fail","Package":"q","Test":"TestA","Elapsed":0.02}
{"Action":"skip","Package":"p","Test":"TestB","Elapsed":0}
EOF
check "trailing output preserves verdicts" 0

# Only the unfinished test gets a synthetic failure, not the package output.
cat >"$work/input.jsonl" <<'EOF'
{"Action":"run","Package":"p","Test":"TestC"}
{"Action":"output","Package":"p","Test":"TestC","Output":"hang...\n"}
{"Action":"output","Package":"p","Output":"panic: timeout\n"}
EOF
cp "$work/input.jsonl" "$work/expected.jsonl"
printf '%s' '{"Action":"output","Package":"p","Tes' >>"$work/input.jsonl"
cat >>"$work/expected.jsonl" <<'EOF'
{"Action":"fail","Package":"p","Test":"TestC","Elapsed":0}
EOF
check "interrupted test and truncated line" 1

echo "Checking missing file"
"$dir/normalize-test-results.sh" "$work/missing.jsonl" >"$work/output.log" 2>&1
grep -Fq "skipping normalization" "$work/output.log"
[[ ! -e "$work/missing.jsonl" ]]

echo "All normalization tests passed"
