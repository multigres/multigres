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

# Demo script for Multigres backup and restore functionality.
#
# Prerequisites:
#   - Kind cluster running (see launch.sh)
#   - Multigres cluster deployed and healthy
#   - kubectl configured to access the cluster
#   - psql client installed
#   - Port forwards running:
#       ./port-forward-multigres-cluster.sh  (for PostgreSQL access)
#       ./port-forward-infra.sh              (for MultiAdmin gRPC)
#
# Usage:
#   cd demo/k8s
#   ./backup-restore.sh
#
# The script will interactively demonstrate:
#   - Listing backups via MultiAdmin gRPC API
#   - Performing incremental backups
#   - Creating test data (animals table with 100,000 rows)
#   - Verifying backups after data changes
#
# Press ENTER at each pause to continue to the next step.
# Press Ctrl+C to exit.

set -e

# Ensure we're in the k8s directory
if [[ $(basename "$PWD") != "k8s" ]]; then
  echo "Error: This script must be run from the demo/k8s directory"
  exit 1
fi

# Helper function to display and execute a command
run_cmd() {
  echo "+ $*"
  "$@"
}

# Helper function to display and execute a command in the background
run_cmd_bg() {
  echo "+ $* &"
  "$@" >/dev/null 2>&1 &
}

# Helper function to pause and wait for user input
pause_for_input() {
  echo ""
  echo "press ENTER to continue"
  read -r
}

# List existing backups (should show initial backup of primary)
echo ""
echo "Listing existing backups"
echo "========================"
run_cmd ../../bin/multigres cluster list-backups \
  --admin-server localhost:18070 \
  --database postgres \
  --config-file-not-found-handling ignore

pause_for_input

# Perform first incremental backup
echo ""
echo "Performing incremental backup"
echo "=============================="
run_cmd ../../bin/multigres cluster backup \
  --admin-server localhost:18070 \
  --database postgres \
  --type incremental \
  --config-file-not-found-handling ignore

pause_for_input

# Show new backup in list backups
echo ""
echo "Listing backups after first backup"
echo "==================================="
run_cmd ../../bin/multigres cluster list-backups \
  --admin-server localhost:18070 \
  --database postgres \
  --config-file-not-found-handling ignore

pause_for_input

# Create animals table
echo ""
echo "Creating 'animals' table"
echo "========================"
run_cmd psql --host=localhost --port=15433 -U postgres -d postgres -c "
CREATE TABLE IF NOT EXISTS animals (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);"

echo ""
echo "Verifying table structure:"
run_cmd psql --host=localhost --port=15433 -U postgres -d postgres -c "\d animals"

pause_for_input

# Insert data
echo ""
echo "Inserting rows into 'animals' table"
echo "===================================="
run_cmd psql --host=localhost --port=15433 -U postgres -d postgres -c "
INSERT INTO animals (name)
SELECT (ARRAY['cat', 'dog', 'bird', 'fish', 'rabbit', 'hamster'])[(random() * 5)::int + 1] || '_' || generate_series(1, 100000);"

echo ""
echo "Verifying row count:"
run_cmd psql --host=localhost --port=15433 -U postgres -d postgres -c "
SELECT COUNT(*) as total_animals FROM animals;"

pause_for_input

# Perform full backup
echo ""
echo "Performing full backup"
echo "======================="
run_cmd ../../bin/multigres cluster backup \
  --admin-server localhost:18070 \
  --database postgres \
  --type full \
  --config-file-not-found-handling ignore

pause_for_input

# List final backups
echo ""
echo "Listing all backups (final)"
echo "==========================="
run_cmd ../../bin/multigres cluster list-backups \
  --admin-server localhost:18070 \
  --database postgres \
  --config-file-not-found-handling ignore
