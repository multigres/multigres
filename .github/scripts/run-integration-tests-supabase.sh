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

# Runs the full integration test suite inside the supabase Postgres test container.
# Must be run from the multigres repository root — the directory is volume-mounted
# into the container at /multigres so the build and test run against the live source.
#
# The container image's ENTRYPOINT (.github/scripts/run-integration-tests-container.sh,
# baked into the image at /usr/local/bin/run-integration-tests) handles the actual
# build and test steps.  This script is just the host-side wrapper that invokes
# docker run with the right volumes and environment.
#
# Test result files are written to the current directory:
#   integration-test-results.jsonl
#   integration-test-results.xml
#
# Optional environment variables:
#   TEST_IMAGE              Image to run (default: supabase-postgres-test:local).
#   GO_CACHE_DIR            Host path for the Go build cache (default: /tmp/go-cache).
#   TEST_PRINT_LOGS         Set non-empty to print service logs on test failure.
#   AWS_ACCESS_KEY_ID       Forwarded into container (default: test-access-key).
#   AWS_SECRET_ACCESS_KEY   Forwarded into container (default: test-secret-key).
#   RERUN_FAILS             gotestsum --rerun-fails count; empty = no reruns (default).
#                           Set to 3 in CI for flake tolerance.

set -euo pipefail

TEST_IMAGE="${TEST_IMAGE:-supabase-postgres-test:local}"
GO_CACHE_DIR="${GO_CACHE_DIR:-/tmp/go-cache}"
MULTIGRES_DIR="${MULTIGRES_DIR:-$(pwd)}"

mkdir -p "${GO_CACHE_DIR}"

RERUN_FAILS="${RERUN_FAILS:-}"

docker run --rm \
  --name multigres-integration-test \
  -v "${MULTIGRES_DIR}:/multigres" \
  -v "${GO_CACHE_DIR}:/home/postgres/.cache" \
  -w /multigres \
  -e "TEST_PRINT_LOGS=${TEST_PRINT_LOGS:-}" \
  -e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test-access-key}" \
  -e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test-secret-key}" \
  -e "RERUN_FAILS=${RERUN_FAILS}" \
  "${TEST_IMAGE}"
