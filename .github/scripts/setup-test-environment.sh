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

# Shared setup script for GitHub Actions test environments.
# Sets up PostgreSQL, etcd, and other dependencies needed for integration tests.

set -euo pipefail

echo "==========================================="
echo "Setting up test environment"
echo "==========================================="

# Install PostgreSQL 17 and pgBackRest from PGDG.
# Ubuntu 24.04's default `postgresql` metapackage is PG 16, but multigres
# requires PG 17.x — `multigres cluster start` rejects other majors.
echo "Installing PostgreSQL 17 and pgBackRest from PGDG..."
sudo apt-get update
sudo apt-get install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get install -y postgresql-17 postgresql-client-17 postgresql-contrib-17 pgbackrest
pgbackrest version

# Set up postgres user password for tests
echo "Configuring PostgreSQL authentication..."
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';" || true

# Add PostgreSQL 17 binaries to PATH for subsequent commands
POSTGRES_BIN="/usr/lib/postgresql/17/bin"
export PATH="$POSTGRES_BIN:$PATH"
echo "$POSTGRES_BIN" >>"$GITHUB_PATH"

# Verify PostgreSQL installation
echo "Verifying PostgreSQL installation..."
postgres --version
initdb --version
pg_ctl --version
pg_isready --version
psql --version

# Install etcd binary
echo "Installing etcd..."
tools/download_tool.sh etcd v3.6.4 linux-amd64 etcd.tar.gz
echo "Extracting etcd binary..."
tar xzf etcd.tar.gz
sudo cp etcd-v3.6.4-linux-amd64/etcd /usr/local/bin/
rm -rf etcd-v3.6.4-linux-amd64 etcd.tar.gz
echo "✅ etcd binary installed successfully"
etcd --version

# Set environment variables for tests
echo "PGCONNECT_TIMEOUT=5" >>"$GITHUB_ENV"

echo "==========================================="
echo "Test environment setup complete"
echo "==========================================="
