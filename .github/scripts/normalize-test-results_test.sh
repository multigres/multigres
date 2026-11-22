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

# Tests for normalize-test-results.jq / .sh. Runs the real filter and wrapper
# against hand-built `go test -json` streams and asserts both what the
# normalizer MUST do (preserve passes/failures, satisfy the reporter contract)
# and what it must NOT do (mask a failure as a pass, invent events for a clean
# stream, silently pass an interrupted test, choke on garbage).
set -euo pipefail

dir="$(cd "$(dirname "$0")" && pwd)"
filter="$dir/normalize-test-results.jq"
script="$dir/normalize-test-results.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

pass=0
fail=0

ok() {
  pass=$((pass + 1))
  printf '  ok   %s\n' "$1"
}

bad() {
  fail=$((fail + 1))
  printf '  FAIL %s\n' "$1"
  if [[ $# -gt 1 ]]; then
    printf '       %s\n' "$2"
  fi
}

# normalize <infile> -> normalized stream on stdout.
normalize() {
  jq -R -n -r --arg mode stream -f "$filter" "$1"
}

# interrupted_count <infile> -> number of groups with no terminal event at all.
interrupted_count() {
  jq -R -n -r --arg mode interrupted -f "$filter" "$1"
}

# contract_violations <stream-file> -> JSON array of [Package,Test] groups whose
# LAST event (in stream order) lacks a terminal action carrying Elapsed. This is
# exactly what dorny/test-reporter's golang-json parser rejects. "[]" means the
# stream satisfies the reporter contract.
contract_violations() {
  jq -c -s '
    [ .[] | select((.Test // "") != "") ]
    | group_by([.Package, .Test])
    | map(.[-1] | select(.Elapsed == null or ([.Action] | inside(["pass", "fail", "skip"]) | not)) | [.Package, .Test])
  ' "$1"
}

# terminal_action <stream-file> <package> <test> -> Action of that group's last event.
terminal_action() {
  jq -s -r --arg p "$2" --arg t "$3" '
    [ .[] | select(.Package == $p and .Test == $t) ] | last | .Action
  ' "$1"
}

assert_eq() {
  if [[ "$2" == "$3" ]]; then
    ok "$1"
  else
    bad "$1" "want [$3], got [$2]"
  fi
}

# ---------------------------------------------------------------------------
# Fixtures.
# ---------------------------------------------------------------------------

# A passing test whose group ends on a stray output line after the pass — the
# exact flake this normalizer exists to fix.
stray="$work/stray.jsonl"
printf '%s\n' \
  '{"Action":"run","Package":"p","Test":"TestA"}' \
  '{"Action":"output","Package":"p","Test":"TestA","Output":"=== RUN TestA\n"}' \
  '{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0.01}' \
  '{"Action":"output","Package":"p","Test":"TestA","Output":"stray goroutine log\n"}' \
  >"$stray"

# A failing test, likewise followed by a stray output line.
realfail="$work/realfail.jsonl"
printf '%s\n' \
  '{"Action":"run","Package":"p","Test":"TestB"}' \
  '{"Action":"output","Package":"p","Test":"TestB","Output":"boom\n"}' \
  '{"Action":"fail","Package":"p","Test":"TestB","Elapsed":0.02}' \
  '{"Action":"output","Package":"p","Test":"TestB","Output":"stray after fail\n"}' \
  >"$realfail"

# A test that never reaches a terminal event (killed binary / timeout / OOM).
interrupted="$work/interrupted.jsonl"
printf '%s\n' \
  '{"Action":"run","Package":"p","Test":"TestC"}' \
  '{"Action":"output","Package":"p","Test":"TestC","Output":"hang...\n"}' \
  >"$interrupted"

# A truncated final line (killed mid-write) plus a package-level line with no
# per-test terminal — neither should produce a synthetic test event.
truncated="$work/truncated.jsonl"
printf '%s\n' \
  '{"Action":"run","Package":"p","Test":"TestD"}' \
  '{"Action":"pass","Package":"p","Test":"TestD","Elapsed":0.03}' \
  '{"Action":"output","Package":"p","Output":"PASS\n"}' \
  '{"Action":"pass","Package":"p","Elapsed":0.5}' \
  '{"Action":"output","Package":"p","Tes' \
  >"$truncated"

# A clean, well-formed stream that already satisfies the contract.
clean="$work/clean.jsonl"
printf '%s\n' \
  '{"Action":"run","Package":"p","Test":"TestE"}' \
  '{"Action":"output","Package":"p","Test":"TestE","Output":"ok\n"}' \
  '{"Action":"pass","Package":"p","Test":"TestE","Elapsed":0.04}' \
  >"$clean"

# ---------------------------------------------------------------------------
# Positive assertions: the normalizer does its job.
# ---------------------------------------------------------------------------

normalize "$stray" >"$work/stray.norm"
assert_eq "stray: contract satisfied after normalize" \
  "$(contract_violations "$work/stray.norm")" "[]"
assert_eq "stray: passing test still ends on pass" \
  "$(terminal_action "$work/stray.norm" p TestA)" "pass"

normalize "$clean" >"$work/clean.norm"
assert_eq "clean: contract satisfied" \
  "$(contract_violations "$work/clean.norm")" "[]"
# A clean stream must pass through byte-for-byte: no invented or duplicated events.
assert_eq "clean: stream passes through unchanged" \
  "$(cat "$work/clean.norm")" "$(cat "$clean")"

assert_eq "interrupted: one group marked failed" \
  "$(interrupted_count "$interrupted")" "1"

# ---------------------------------------------------------------------------
# Negative assertions: the normalizer does NOT do the wrong thing.
# ---------------------------------------------------------------------------

# A real failure must never be rewritten into a pass by the trailing stray line.
normalize "$realfail" >"$work/realfail.norm"
assert_eq "NEG realfail: failing test still ends on fail (not masked as pass)" \
  "$(terminal_action "$work/realfail.norm" p TestB)" "fail"
assert_eq "NEG realfail: contract satisfied without flipping the verdict" \
  "$(contract_violations "$work/realfail.norm")" "[]"

# An interrupted test must be marked failed, never silently passed.
normalize "$interrupted" >"$work/interrupted.norm"
assert_eq "NEG interrupted: no-terminal test ends on fail (not pass/skip)" \
  "$(terminal_action "$work/interrupted.norm" p TestC)" "fail"
assert_eq "NEG interrupted: contract satisfied" \
  "$(contract_violations "$work/interrupted.norm")" "[]"

# The garbage trailing line must be dropped, not echoed, and must not crash jq.
normalize "$truncated" >"$work/truncated.norm"
assert_eq "NEG truncated: unparseable trailing line is dropped" \
  "$(grep -c '"Tes$' "$work/truncated.norm" || true)" "0"
assert_eq "NEG truncated: every output line is valid JSON" \
  "$(jq -R -r 'try (fromjson|empty) catch "BAD"' "$work/truncated.norm" | grep -c BAD || true)" "0"
assert_eq "NEG truncated: contract satisfied" \
  "$(contract_violations "$work/truncated.norm")" "[]"
# A package-level line (empty Test) is not a test group: it must not be counted
# as interrupted nor gain a synthetic per-test terminal.
assert_eq "NEG truncated: package-level event spawns no synthetic test" \
  "$(interrupted_count "$truncated")" "0"

# Meta-guard: the contract check must have teeth. The RAW (un-normalized) stray
# stream must itself violate the contract — otherwise a no-op filter would pass
# every assertion above for the wrong reason.
assert_eq "NEG meta: raw un-normalized stray stream violates the contract" \
  "$(contract_violations "$stray")" '[["p","TestA"]]'

# ---------------------------------------------------------------------------
# Wrapper script: in-place rewrite, interrupted warning, missing-file skip.
# ---------------------------------------------------------------------------

cp "$interrupted" "$work/wrap.jsonl"
# The wrapper emits the ::warning:: workflow command on stdout, so capture both.
warn="$("$script" "$work/wrap.jsonl" 2>&1 || true)"
assert_eq "wrapper: rewritten file satisfies the contract" \
  "$(contract_violations "$work/wrap.jsonl")" "[]"
case "$warn" in
*"::warning::"*"interrupted stream"*) ok "wrapper: emits interrupted warning" ;;
*) bad "wrapper: emits interrupted warning" "stderr was [$warn]" ;;
esac

missing_out="$("$script" "$work/does-not-exist.jsonl" 2>&1 || true)"
case "$missing_out" in
*"skipping normalization"*) ok "wrapper: missing file is skipped, not an error" ;;
*) bad "wrapper: missing file is skipped, not an error" "output was [$missing_out]" ;;
esac

# ---------------------------------------------------------------------------
printf '\n%d passed, %d failed\n' "$pass" "$fail"
[[ $fail -eq 0 ]]
