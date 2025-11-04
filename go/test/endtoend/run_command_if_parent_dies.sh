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
# Monitors the parent process and runs a cleanup command if the parent dies.
# This is used in integration tests to ensure daemon processes (like postgres or etcd)
# are cleaned up if the test runner is killed unexpectedly.
#
# Usage: run_command_if_parent_dies.sh <command> [args...]
# Example: run_command_if_parent_dies.sh pg_ctl stop -D /path/to/data -m fast

set -euo pipefail

# Save the parent PID
PARENT_PID=$PPID

# Monitor parent process
while kill -0 "$PARENT_PID" 2>/dev/null; do
  sleep 1
done

# Parent died, execute the cleanup command
exec "$@"
