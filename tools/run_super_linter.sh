#!/bin/bash
# Copyright 2025 Supabase, Inc.
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

set -euo pipefail

# Get the common git directory (handles worktrees)
GIT_COMMON_DIR=$(git rev-parse --git-common-dir)

# Use -it only if running in a TTY
if [ -t 0 ]; then
  TTY_FLAGS="-it"
else
  TTY_FLAGS=""
fi

docker run --platform linux/x86_64 \
  $TTY_FLAGS \
  --env-file ".github/super-linter.env" \
  --env-file ".github/local-super-linter.env" \
  -v "$PWD:/tmp/lint" \
  -v "$GIT_COMMON_DIR:$GIT_COMMON_DIR:ro" \
  --rm \
  ghcr.io/super-linter/super-linter:latest
