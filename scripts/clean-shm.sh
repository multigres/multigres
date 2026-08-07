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

# Reclaims orphaned System V shared-memory segments left behind by end-to-end
# tests that SIGKILL or crash postgres (which never releases its segment cleanly).
#
# ShardSetup.Cleanup already sweeps these at the end of a normal test run, but if
# `go test` hits its -timeout the binary is hard-killed and cleanup never runs,
# so segments leak. On macOS kern.sysv.shmmni defaults to 32 and a handful of
# leaks exhausts the table, making later postgres starts fail shmget. Run this to
# clean up manually:
#
#   scripts/clean-shm.sh          # remove orphaned segments
#   scripts/clean-shm.sh --dry-run # list what would be removed
#
# Only segments owned by the current user with no attached process (nattch == 0)
# are removed, and IPC_PRIVATE segments (key 0x00000000) are skipped, so a live
# postgres is never affected: an attached postmaster keeps nattch > 0.

set -euo pipefail

dry_run=false
if [[ "${1:-}" == "--dry-run" || "${1:-}" == "-n" ]]; then
  dry_run=true
fi

os="$(uname -s)"
if [[ "$os" != "Darwin" && "$os" != "Linux" ]]; then
  echo "clean-shm: unsupported OS '$os' (only Darwin and Linux); nothing to do."
  exit 0
fi

if ! command -v ipcs >/dev/null 2>&1; then
  echo "clean-shm: ipcs not found; nothing to do."
  exit 0
fi

me="$(id -un)"

# Emit the shmid of every orphaned segment (owner == me, nattch == 0, keyed).
# The two platforms' ipcs column layouts differ, so parse each explicitly.
orphaned_ids() {
  if [[ "$os" == "Darwin" ]]; then
    # macOS `ipcs -mo`:  m  ID  KEY  MODE  OWNER  GROUP  NATTCH
    ipcs -mo | awk -v me="$me" \
      '$1=="m" && $5==me && $7=="0" && $3!="0x00000000" {print $2}'
  else
    # Linux `ipcs -m`:  KEY  SHMID  OWNER  PERMS  BYTES  NATTCH  [STATUS]
    ipcs -m | awk -v me="$me" \
      '$1 ~ /^0x/ && $3==me && $6=="0" && $1!="0x00000000" {print $2}'
  fi
}

ids=$(orphaned_ids || true)

if [[ -z "$ids" ]]; then
  echo "clean-shm: no orphaned shared-memory segments owned by '$me'."
  exit 0
fi

count=0
removed=0
while IFS= read -r id; do
  [[ -n "$id" ]] || continue
  count=$((count + 1))
  if $dry_run; then
    echo "would remove shmid $id"
    continue
  fi
  if ipcrm -m "$id" 2>/dev/null; then
    removed=$((removed + 1))
  else
    echo "clean-shm: failed to remove shmid $id (already gone or in use)" >&2
  fi
done <<<"$ids"

if $dry_run; then
  echo "clean-shm: $count orphaned segment(s) would be removed (dry run)."
else
  echo "clean-shm: reclaimed $removed of $count orphaned segment(s) owned by '$me'."
fi
