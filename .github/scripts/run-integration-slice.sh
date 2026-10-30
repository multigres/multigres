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

# Runs one slice of the integration test workflow under gotestsum and leaves
# integration-test-results.jsonl and integration-test-results.xml in the
# working directory.
#
# Inputs: TEST_PACKAGES, SKIP_PACKAGES and SHARD, as documented in
# resolve-test-slice.sh.
#
# Needs `make build`, gotestsum and a running port pool server
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

gotestsum \
  --format=testname \
  --packages="${packages[*]}" \
  --jsonfile=integration-test-results.jsonl \
  --junitfile=integration-test-results.xml \
  -- \
  ${run_args[@]+"${run_args[@]}"} -skip TestPostgreSQLRegression -timeout=30m
