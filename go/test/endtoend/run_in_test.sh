#!/usr/bin/env bash
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

# run_in_test wraps a command to monitor parent process and cleanup if parent dies
# Usage: run_in_test <command> [args...]
#
# This script is used in integration tests to ensure child processes are cleaned up
# if the parent test process dies or is killed. It monitors the parent PID and
# sends SIGTERM (then SIGKILL after 5s) to the child if the parent changes.

if [ $# -eq 0 ]; then
  echo "Usage: run_in_test <command> [args...]" >&2
  exit 1
fi

# Get current parent PID
parent_pid=$(ps -o ppid= -p $$ | tr -d ' ')

# Start the child process in background
"$@" &
child_pid=$!

# Cleanup function - sends SIGTERM, waits 5s, then SIGKILL
cleanup() {
  if kill -0 $child_pid 2>/dev/null; then
    echo "run_in_test: Sending SIGTERM to child process $child_pid" >&2
    kill -TERM $child_pid 2>/dev/null || true

    # Wait up to 5 seconds for graceful shutdown
    for _ in {1..50}; do
      if ! kill -0 $child_pid 2>/dev/null; then
        echo "run_in_test: Child process terminated gracefully" >&2
        return
      fi
      sleep 0.1
    done

    # Force kill if still running
    if kill -0 $child_pid 2>/dev/null; then
      echo "run_in_test: Sending SIGKILL to child process $child_pid" >&2
      kill -KILL $child_pid 2>/dev/null || true
    fi
  fi
}

# Trap signals to propagate to child
trap 'cleanup; exit 143' SIGTERM
trap 'cleanup; exit 130' SIGINT

# Monitor parent PID every second
while kill -0 $child_pid 2>/dev/null; do
  # Check if parent changed (orphaned)
  current_parent=$(ps -o ppid= -p $$ | tr -d ' ')
  if [ "$current_parent" != "$parent_pid" ]; then
    echo "run_in_test: Parent process died (was $parent_pid, now $current_parent), terminating child" >&2
    cleanup
    exit 0
  fi
  sleep 1
done

# Child exited normally, wait for exit code
wait $child_pid
exit_code=$?

exit $exit_code
