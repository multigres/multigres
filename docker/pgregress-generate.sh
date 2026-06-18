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

# Regenerate the pgregress patch set inside an ubuntu-24.04 container that matches
# the CI environment. The regenerated patches are written straight back into the
# working tree (testdata/pg17/patches/) via the bind mount, ready to review and
# commit. See go/test/endtoend/pgregresstest/README.md for why this must run on
# Linux rather than directly on macOS.
#
# Usage:
#   docker/pgregress-generate.sh                  # full extended suite (regression+isolation+contrib+external)
#   RUN_VARS="RUN_PGREGRESS=1" docker/pgregress-generate.sh        # core regression only
#   RUN_VARS="RUN_PGEXTERNAL=1" docker/pgregress-generate.sh       # external extensions only
#   PGREGRESS_TESTS="boolean char" RUN_VARS="RUN_PGREGRESS=1" docker/pgregress-generate.sh   # subset
#
# Env knobs:
#   RUN_VARS           which suites to enable (default RUN_EXTENDED_QUERY_SERVING_TESTS=1)
#   PGREGRESS_PATCH_MODE   defaults to "generate" here; set "verify" to dry-run a check
#   PGREGRESS_TESTS / PGCONTRIB_TESTS / PGEXTERNAL_TESTS   optional subsets
#   PGREGRESS_TIMEOUT  go test timeout (default 90m)
#   PLATFORM           docker platform (default: host arch; set linux/amd64 for exact CI parity)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="multigres-pgregress:latest"

RUN_VARS="${RUN_VARS:-RUN_EXTENDED_QUERY_SERVING_TESTS=1}"
PATCH_MODE="${PGREGRESS_PATCH_MODE:-generate}"
TIMEOUT="${PGREGRESS_TIMEOUT:-90m}"

# If REPO_ROOT is a git worktree, its .git is a file pointing at a gitdir that
# lives OUTSIDE the bind mount (e.g. <main-repo>/.git/worktrees/<name>). Git then
# fatals on any command run from /workspace. Mount the real common gitdir at its
# original absolute path (read-only) so the worktree resolves inside the container.
GIT_COMMON_DIR="$(git -C "${REPO_ROOT}" rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)"
git_mount=()
if [[ -n "${GIT_COMMON_DIR}" && "${GIT_COMMON_DIR}" != "${REPO_ROOT}/.git" ]]; then
  git_mount=(-v "${GIT_COMMON_DIR}:${GIT_COMMON_DIR}:ro")
fi

# Build args. linux/arm64 (native on Apple Silicon) is fast and produces the same
# SQL-level regression output as CI's linux/amd64; pass PLATFORM=linux/amd64 for
# byte-for-byte CI parity at the cost of QEMU emulation.
build_args=(-f "${REPO_ROOT}/docker/Dockerfile.pgregress" -t "${IMAGE}" "${REPO_ROOT}/docker")
run_platform=()
if [[ -n "${PLATFORM:-}" ]]; then
  build_args=(--platform "${PLATFORM}" "${build_args[@]}")
  run_platform=(--platform "${PLATFORM}")
fi

echo "==> Building ${IMAGE}"
docker build "${build_args[@]}"

# Forward the suite-enable vars (RUN_VARS may contain several KEY=VAL pairs).
env_args=()
for kv in ${RUN_VARS}; do env_args+=(-e "${kv}"); done
env_args+=(-e "PGREGRESS_PATCH_MODE=${PATCH_MODE}" -e TEST_PRINT_LOGS=1)
# Disable Go's VCS stamping for the in-container build: the repo may be a git
# worktree whose real gitdir lives outside the bind mount, so `go build` can't
# run git. The embedded version doesn't affect regression output.
env_args+=(-e "GOFLAGS=-buildvcs=false")
# Pass the timeout into the container as an env var so the in-container script can
# reference it directly, instead of interpolating host values into the single-
# quoted `bash -c` body.
env_args+=(-e "PGREGRESS_TIMEOUT=${TIMEOUT}")

# Optional debug hook: redirect the per-run cluster tempdir (shardsetup_test_*,
# holding multipooler.log / pgctld.log / multiorch.log) to a bind-mounted host
# dir by pointing TMPDIR at it, so the cluster logs survive the container and can
# be tailed live. Enable with PGREGRESS_DEBUG_DIR=/abs/host/dir.
debug_mount=()
if [[ -n "${PGREGRESS_DEBUG_DIR:-}" ]]; then
  mkdir -p "${PGREGRESS_DEBUG_DIR}"
  debug_mount=(-v "${PGREGRESS_DEBUG_DIR}:/pgregress-debug")
  env_args+=(-e "TMPDIR=/pgregress-debug")
fi
for v in PGREGRESS_TESTS PGISOLATION_TESTS PGCONTRIB_TESTS PGEXTERNAL_TESTS PG_CONFIGURE_EXTRA_ARGS; do
  [[ -n "${!v:-}" ]] && env_args+=(-e "${v}=${!v}")
done

echo "==> Running pgregress (${RUN_VARS}, PATCH_MODE=${PATCH_MODE}) in container"
# Named volumes persist the Go caches and the from-source PostgreSQL build across
# runs (big speedup on re-runs). The bin/ volume shields the host's macOS binaries
# from the Linux `make build` output; patches still land in the bind-mounted tree.
docker run --rm -t ${run_platform[@]+"${run_platform[@]}"} \
  -v "${REPO_ROOT}:/workspace" \
  -v multigres-pgregress-gocache:/home/tester/.cache/go-build \
  -v multigres-pgregress-gomod:/home/tester/go/pkg/mod \
  -v multigres-pgregress-cargo:/home/tester/.cargo/registry \
  -v multigres-pgregress-pgcache:/tmp/multigres_pg_cache \
  -v multigres-pgregress-bin:/workspace/bin \
  ${git_mount[@]+"${git_mount[@]}"} \
  ${debug_mount[@]+"${debug_mount[@]}"} \
  "${env_args[@]}" \
  -w /workspace \
  "${IMAGE}" \
  bash -euo pipefail -c '
    git config --global --add safe.directory "*"
    echo "== make build =="
    make build
    echo "== start port pool server =="
    ./bin/portpoolserver --socket /tmp/multigres-port-pool.sock &
    PP=$!
    trap "kill $PP 2>/dev/null || true" EXIT
    export MULTIGRES_PORT_POOL_ADDR=/tmp/multigres-port-pool.sock
    echo "== pgregress (${PGREGRESS_PATCH_MODE}) =="
    go test -v -timeout "${PGREGRESS_TIMEOUT}" -run TestPostgreSQLRegression ./go/test/endtoend/pgregresstest/...
  '

echo
echo "==> Done. Review the regenerated patches:"
echo "    git -C \"${REPO_ROOT}\" status --short go/test/endtoend/pgregresstest/testdata/pg17/patches/"
