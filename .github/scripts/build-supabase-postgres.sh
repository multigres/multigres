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

# Builds the supabase Postgres base Docker image from a local checkout of
# github.com/supabase/postgres using its Dockerfile-multigres --target variant-17.
#
# Required environment variable:
#   SUPABASE_POSTGRES_DIR  Path to the supabase/postgres checkout.
#                          Locally defaults to $HOME/repos/supabase/postgres;
#                          in CI it should point at the checked-out repo.
#
# Optional environment variable:
#   SUPABASE_IMAGE         Output image tag (default: supabase-postgres:local).

set -euo pipefail

SUPABASE_POSTGRES_DIR="${SUPABASE_POSTGRES_DIR:?SUPABASE_POSTGRES_DIR must be set to the path of the supabase/postgres checkout}"
SUPABASE_IMAGE="${SUPABASE_IMAGE:-supabase-postgres:local}"

echo "Building ${SUPABASE_IMAGE} from ${SUPABASE_POSTGRES_DIR} ..."
docker build \
  -f "${SUPABASE_POSTGRES_DIR}/Dockerfile-multigres" \
  --target variant-17 \
  -t "${SUPABASE_IMAGE}" \
  "${SUPABASE_POSTGRES_DIR}"
echo "Built ${SUPABASE_IMAGE}"
