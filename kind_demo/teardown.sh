#!/usr/bin/env zsh
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

set -ex

# Kind cluster demo teardown - removes everything including data
# This script calls teardown-multigres-cluster.sh and teardown-infrastructure.sh,
# then deletes the Kind cluster and data directory

# Ensure we're in the kind_demo directory.
# This is because we use a relative path name to the data files.
if [[ $(basename "$PWD") != "kind_demo" ]]; then
  echo "Error: This script must be run from the kind_demo directory"
  exit 1
fi

# Teardown multigres cluster components if they exist
if kubectl get pods -l app=multipooler 2>/dev/null | grep -q multipooler; then
  echo "Tearing down multigres cluster components..."
  ./teardown-multigres-cluster.sh
fi

# Teardown infrastructure components if they exist
if kubectl get pods -l app=etcd 2>/dev/null | grep -q etcd; then
  echo "Tearing down infrastructure components..."
  ./teardown-infrastructure.sh
fi

# Delete the Kind cluster
echo "Deleting Kind cluster..."
kind delete cluster --name=multidemo

# Clean up data directory
echo "Cleaning up data directory..."
rm -rf data/*

set +x
echo ""
echo "========================================="
echo "Complete Teardown Finished"
echo "========================================="
echo ""
echo "Removed:"
echo "  - Multigres cluster components"
echo "  - Infrastructure components"
echo "  - Kind cluster"
echo "  - Data directory"
echo ""
echo "To start fresh: ./launch-infra.sh && ./launch-multigres-cluster.sh"
echo ""
