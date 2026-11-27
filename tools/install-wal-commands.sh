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

# Install independent copies. Never write through an existing symlink or leave
# pgctld and an archive executable sharing mutable contents during a rollback.
set -eu
cd "$1"
for name in pgbackrest-archive-wal pgbackrest-restore-wal; do
  tmp=$(mktemp .wal-command.XXXXXX)
  trap 'rm -f "$tmp"' EXIT
  cp -p pgctld "$tmp"
  mv -f "$tmp" "$name"
done
