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

# Resolves one slice of the test tree for the sharded CI workflows
# (coverage.yml, test-integration.yml) and prints it for the caller:
#
#   line 1      a -run regex for the shard, or an empty line for "all tests"
#   lines 2..   one package per line
#
# Inputs (environment):
#   TEST_PACKAGES   space-separated package patterns (required)
#   SKIP_PACKAGES   regex; packages matching it are dropped after `go list`
#   SHARD           "i/n": keep every n-th top-level test by sorted name
#
# Shard i/n takes every n-th top-level test by sorted name. Tests are
# independent (each gets its own cluster, or shares one per package), so any
# split is valid; round-robin keeps the shards within a couple of minutes of
# each other. Read the result with
#   slice=$(.github/scripts/resolve-test-slice.sh)
#   run_regex=${slice%%$'\n'*}
#   read -ra packages <<<"$(tail -n +2 <<<"$slice" | tr '\n' ' ')"

set -euo pipefail

read -ra packages <<<"${TEST_PACKAGES:?TEST_PACKAGES is required}"
if [ -n "${SKIP_PACKAGES:-}" ]; then
  # Package paths never contain whitespace, so word-splitting is safe and
  # keeps this working on bash 3 (macOS) where mapfile does not exist.
  read -ra packages <<<"$(go list "${packages[@]}" | grep -vE "$SKIP_PACKAGES" | tr '\n' ' ')"
fi

regex=""
if [ -n "${SHARD:-}" ]; then
  i=${SHARD%/*}
  n=${SHARD#*/}
  tests=$(go test -list '.*' "${packages[@]}" | grep '^Test' | sort |
    awk -v i="$i" -v n="$n" 'NR % n == i % n' | paste -sd'|' -)
  regex="^(${tests})$"
  echo "shard $SHARD runs: ${tests//|/ }" >&2
fi

echo "$regex"
printf '%s\n' "${packages[@]}"
