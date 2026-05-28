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

# Builds the integration test runner image (supabase Postgres + Go + etcd).
# Must be run from the multigres repository root — Dockerfile.integration-test
# must be present in the current directory.
#
# Optional environment variables:
#   SUPABASE_IMAGE  Base image to build from (default: supabase-postgres:local).
#                   Build this first with build-supabase-postgres.sh.
#   TEST_IMAGE      Output image tag (default: supabase-postgres-test:local).

set -euo pipefail

SUPABASE_IMAGE="${SUPABASE_IMAGE:-supabase-postgres:local}"
TEST_IMAGE="${TEST_IMAGE:-supabase-postgres-test:local}"

echo "Building ${TEST_IMAGE} from ${SUPABASE_IMAGE} ..."
docker build \
  -f Dockerfile.integration-test \
  --build-arg "SUPABASE_IMAGE=${SUPABASE_IMAGE}" \
  -t "${TEST_IMAGE}" \
  .
echo "Built ${TEST_IMAGE}"
