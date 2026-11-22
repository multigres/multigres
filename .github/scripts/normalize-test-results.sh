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

# Rewrites each `go test -json` results file in place so dorny/test-reporter can
# parse it even when a test group's final event lacks Elapsed. See
# normalize-test-results.jq for the why and the exact guarantees. A missing file
# is skipped (not an error), matching the timing-summary step, so a results file
# that was never produced does not turn a green run red.
set -euo pipefail

filter="$(dirname "$0")/normalize-test-results.jq"

for f in "$@"; do
  if [[ ! -f $f ]]; then
    echo "No $f found — skipping normalization"
    continue
  fi

  # Count interrupted groups from the original before rewriting it.
  interrupted="$(jq -R -n -r --arg mode interrupted -f "$filter" "$f")"

  # Write to a temp file in the same directory and rename atomically, so a crash
  # mid-write can never leave a half-written results file.
  tmp="$(mktemp "${f}.norm-XXXXXX")"
  jq -R -n -r --arg mode stream -f "$filter" "$f" >"$tmp"
  mv "$tmp" "$f"

  if [[ $interrupted -gt 0 ]]; then
    # A test with no terminal event means the stream was interrupted (timeout,
    # panic, OOM). Surface it; the failing run itself is what fails the job.
    echo "::warning::$f: $interrupted test(s) had no terminal event and were marked failed (interrupted stream)"
  fi
done
