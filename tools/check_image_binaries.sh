#!/bin/bash
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

# check_image_binaries - Fail when a binary in the Makefile CMDS list that the
# multigres image is meant to ship is not in the image.
#
# A new CMDS entry is built into bin/ but reaches the image only through an
# explicit COPY in the Dockerfile, so forgetting it ships a pod that crashloops
# on "exec: no such file". Two modes:
#
#   check_image_binaries.sh                  static: every shipped binary has a
#                                            COPY to /multigres/bin/ in Dockerfile
#   check_image_binaries.sh --image <ref>    runtime: every shipped binary is
#                                            executable in that built image
#
# A CMDS entry the image deliberately omits must be listed in NOT_IN_IMAGE
# with the reason, so adding a binary forces a decision either way.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_BIN_DIR=/multigres/bin

# pgctld ships in its own image (Dockerfile.pgctld) next to postgres.
# portpoolserver is test and development infrastructure.
NOT_IN_IMAGE=(pgctld portpoolserver)

image=""
if [[ ${1:-} == "--image" ]]; then
  image="${2:?--image needs an image reference}"
elif [[ $# -gt 0 ]]; then
  echo "usage: $0 [--image <ref>]" >&2
  exit 2
fi

cmds=$(sed -n 's/^CMDS[[:space:]]*=[[:space:]]*//p' "${ROOT}/Makefile")
if [[ -z "${cmds}" ]]; then
  echo "::error::could not read CMDS from Makefile" >&2
  exit 1
fi

shipped=()
for cmd in ${cmds}; do
  skip=false
  for omitted in "${NOT_IN_IMAGE[@]}"; do
    [[ "${cmd}" == "${omitted}" ]] && skip=true
  done
  "${skip}" || shipped+=("${cmd}")
done

missing=()
for cmd in "${shipped[@]}"; do
  if [[ -n "${image}" ]]; then
    docker run --rm --entrypoint test "${image}" -x "${IMAGE_BIN_DIR}/${cmd}" || missing+=("${cmd}")
  else
    grep -Eq "^COPY[[:space:]].*/bin/${cmd}[[:space:]]+${IMAGE_BIN_DIR}/" "${ROOT}/Dockerfile" || missing+=("${cmd}")
  fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "::error::multigres image is missing binaries: ${missing[*]} (add a COPY to Dockerfile, or list in NOT_IN_IMAGE in tools/check_image_binaries.sh)" >&2
  exit 1
fi
echo "multigres image ships all ${#shipped[@]} expected binaries: ${shipped[*]}"
