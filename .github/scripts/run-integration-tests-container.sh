#!/bin/sh
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

# ENTRYPOINT for the supabase-postgres-test container image.
# Builds multigres from source and runs the full integration test suite.
#
# The multigres source tree must be volume-mounted at /multigres (the working
# directory).  Result files are written there and appear on the host:
#   integration-test-results.jsonl
#   integration-test-results.xml
#
# Environment variables honoured:
#   TEST_PRINT_LOGS       Set non-empty to print service logs on test failure.
#   AWS_ACCESS_KEY_ID     Forwarded to tests (default set by outer script).
#   AWS_SECRET_ACCESS_KEY Forwarded to tests (default set by outer script).
#   RERUN_FAILS           gotestsum --rerun-fails count; empty = no reruns.
#                         Set to 3 in CI for flake tolerance.
set -eu

make build

./bin/portpoolserver --socket /tmp/multigres-port-pool.sock &
PORT_POOL_PID=$!
trap 'kill $PORT_POOL_PID 2>/dev/null || true' EXIT

rerun_flags=""
if [ -n "${RERUN_FAILS:-}" ]; then
  rerun_flags="--rerun-fails=${RERUN_FAILS} --rerun-fails-max-failures=10"
fi

# SC2086: rerun_flags is intentionally unquoted so it word-splits into
# separate flags when non-empty and produces no argument when empty.
# shellcheck disable=SC2086
MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock \
  gotestsum \
  --format=testname \
  ${rerun_flags} \
  --packages="./go/test/endtoend/..." \
  --jsonfile=integration-test-results.jsonl \
  --junitfile=integration-test-results.xml \
  -- -skip TestPostgreSQLRegression -timeout=30m
