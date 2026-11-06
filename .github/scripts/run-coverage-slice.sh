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

# Runs one slice of the Code Coverage workflow and leaves two profiles in the
# working directory: coverage-direct.txt (the test process, via -coverpkg)
# and coverage-subprocess.txt (the spawned bin/cov services, via
# MULTIGRES_SUBPROCESS_GOCOVERDIR — see pathutil.PrependBinToPath for why
# plain GOCOVERDIR cannot be used under `go test -cover`).
#
# Inputs: TEST_PACKAGES, SKIP_PACKAGES and SHARD, as documented in
# resolve-test-slice.sh.
#
# Needs `make build-coverage` and a running port pool server
# (MULTIGRES_PORT_POOL_ADDR) first, exactly like the workflow.

set -euo pipefail

slice=$(.github/scripts/resolve-test-slice.sh)
run_regex=${slice%%$'\n'*}
read -ra packages <<<"$(tail -n +2 <<<"$slice" | tr '\n' ' ')"
# The [@]+ expansion keeps an empty array legal under set -u on bash 3.
run_args=()
if [ -n "$run_regex" ]; then
  run_args=(-run "$run_regex")
fi

mkdir -p coverage-subprocess-raw
export MULTIGRES_SUBPROCESS_GOCOVERDIR="$PWD/coverage-subprocess-raw"

# Coverage runs cannot use gotestsum's --rerun-fails: a retry would re-invoke
# `go test` against the failing package and overwrite the single -coverprofile
# file, truncating coverage for packages that passed the first time. We instead
# retry the whole slice on failure, which preserves coverage correctness at the
# cost of extra passes on a flake. Unlike the run-once Trunk test jobs, coverage
# keeps its retries to produce a complete profile without manual restarts.
attempt=1
max_attempts=3
while [ $attempt -le $max_attempts ]; do
  # Wipe the subprocess coverage dir before each attempt so the final profile
  # reflects only the successful pass. The :? guard aborts if the variable is
  # ever empty, so this can never expand to rm -rf /*.
  rm -rf "${MULTIGRES_SUBPROCESS_GOCOVERDIR:?}"/*
  # 30m matches test-integration.yml; the per-package go-test default of 10m
  # is too tight for the end-to-end packages that spin up real Postgres.
  if go test -json -v -timeout=30m -skip TestPostgreSQLRegression \
    -cover -covermode=atomic \
    -coverprofile=coverage-direct.txt \
    -coverpkg=./... ${run_args[@]+"${run_args[@]}"} "${packages[@]}" |
    tee coverage-test-results.jsonl |
    go tool tparse -follow; then
    break
  fi
  if [ $attempt -ge $max_attempts ]; then
    echo "::error::coverage tests failed after $max_attempts attempts"
    exit 1
  fi
  echo "::warning::coverage attempt $attempt failed; retrying"
  attempt=$((attempt + 1))
done

go tool covdata textfmt -i=coverage-subprocess-raw -o=coverage-subprocess.txt
