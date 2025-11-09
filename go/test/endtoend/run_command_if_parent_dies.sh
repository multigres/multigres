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

# run_command_if_parent_dies.sh
# Monitors a process and runs a cleanup command if it dies or testdata dir deleted.
# This is used in integration tests to ensure daemon processes (like postgres or etcd)
# are cleaned up if the test runner is killed unexpectedly.
#
# Usage: run_command_if_parent_dies.sh <command> [args...]
# Example: run_command_if_parent_dies.sh pg_ctl stop -D /path/to/data -m fast
#
# Environment variables:
#   MULTIGRES_TESTDATA_DIR - If set and directory is deleted, run cleanup command
#   MULTIGRES_TEST_PARENT_PID - If set, monitor this PID instead of parent

set -euo pipefail

# Get orphan detection environment variables
TESTDATA_DIR="${MULTIGRES_TESTDATA_DIR:-}"
TEST_PARENT_PID="${MULTIGRES_TEST_PARENT_PID:-}"

# Determine which PID to monitor (prefer TEST_PARENT_PID, fallback to parent)
if [ -n "$TEST_PARENT_PID" ]; then
  MONITOR_PID="$TEST_PARENT_PID"
else
  MONITOR_PID="$PPID"
fi

# Monitor loop
while kill -0 "$MONITOR_PID" 2>/dev/null; do
  # Check if testdata directory was deleted
  if [ -n "$TESTDATA_DIR" ] && [ ! -d "$TESTDATA_DIR" ]; then
    # Directory deleted, execute cleanup command
    exec "$@"
  fi
  sleep 1
done

# Monitored process died, execute the cleanup command
exec "$@"
