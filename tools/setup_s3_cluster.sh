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

set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 --credentials-file <path> --backup-url <url> --region <region> [--config-path <path>]

Setup a local Multigres cluster with S3 backups.

Required arguments:
  --credentials-file <path>  Path to file containing AWS credentials (will be sourced)
  --backup-url <url>         S3 backup URL (format: s3://bucket/prefix)
  --region <region>          AWS region (e.g., us-east-1)

Optional arguments:
  --config-path <path>       Path for cluster configuration (default: ./multigres_local)
  -h, --help                 Show this help message

Examples:
  # Setup cluster with AWS S3
  $0 \\
    --credentials-file ~/.aws/staging-credentials \\
    --backup-url s3://my-bucket/multigres-backups/ \\
    --region us-east-1

  # Setup cluster with custom config path
  $0 \\
    --credentials-file ~/.aws/staging-credentials \\
    --backup-url s3://my-bucket/multigres-backups/ \\
    --region us-east-1 \\
    --config-path /tmp/my-cluster

Prerequisites:
  - No existing cluster (run: ./bin/multigres cluster stop --clean && rm -rf multigres_local)

The script will:
  1. Source credentials and verify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
  2. Initialize cluster with S3 backup configuration
  3. Start cluster (bootstrap automatically creates first backup)
  4. Wait for backup to appear (5 retries × 30s = 2.5 minutes timeout)
  5. Print next steps on success
EOF
}

# Parse command line arguments
credentials_file=""
backup_url=""
region=""
config_path="./multigres_local"

while [[ $# -gt 0 ]]; do
  case $1 in
  -h | --help)
    usage
    exit 0
    ;;
  --credentials-file)
    credentials_file="$2"
    shift 2
    ;;
  --backup-url)
    backup_url="$2"
    shift 2
    ;;
  --region)
    region="$2"
    shift 2
    ;;
  --config-path)
    config_path="$2"
    shift 2
    ;;
  *)
    echo "Error: Unknown option: $1"
    echo
    usage
    exit 1
    ;;
  esac
done

# Validate required parameters
if [[ -z "$credentials_file" ]]; then
  echo "Error: --credentials-file is required"
  echo
  usage
  exit 1
fi

if [[ -z "$backup_url" ]]; then
  echo "Error: --backup-url is required"
  echo
  usage
  exit 1
fi

if [[ -z "$region" ]]; then
  echo "Error: --region is required"
  echo
  usage
  exit 1
fi

# Validate credentials file exists
if [[ ! -f "$credentials_file" ]]; then
  echo "Error: Credentials file not found: $credentials_file"
  exit 1
fi

# Validate backup URL format
if [[ ! "$backup_url" =~ ^s3:// ]]; then
  echo "Error: Backup URL must start with s3://"
  echo "Got: $backup_url"
  exit 1
fi

# Check if cluster already exists
if [[ -f "$config_path/multigres.yaml" ]]; then
  echo "Error: Cluster already exists at $config_path/"
  echo
  echo "To clean up the existing cluster, run:"
  echo "  ./bin/multigres cluster stop --clean --config-path $config_path"
  echo "  rm -rf $config_path"
  exit 1
fi

# Source credentials file
# shellcheck source=/dev/null
source "$credentials_file"

# Verify required AWS environment variables are set
if [[ -z "${AWS_ACCESS_KEY_ID:-}" ]] || [[ -z "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  echo "Error: Credentials file did not set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
  echo
  echo "Expected format in $credentials_file:"
  echo "  export AWS_ACCESS_KEY_ID=..."
  echo "  export AWS_SECRET_ACCESS_KEY=..."
  echo "  export AWS_SESSION_TOKEN=...  # optional for temporary credentials"
  exit 1
fi

# Use fresh binaries
make build

# Initialize cluster with S3 backup configuration
./bin/multigres cluster init \
  --config-path="$config_path" \
  --backup-url="$backup_url" \
  --region="$region"

# Start cluster (bootstrap creates first backup)
./bin/multigres cluster start --config-path="$config_path"

# Wait for backup to appear (bootstrap creates it automatically)
max_attempts=5
wait_seconds=30

for attempt in $(seq 1 $max_attempts); do
  if ./bin/multigres cluster list-backups --config-path="$config_path" | grep -q "complete.*primary"; then
    echo "Backup found!"
    break
  fi

  if [[ $attempt -lt $max_attempts ]]; then
    sleep $wait_seconds
  else
    echo "Error: No backup appeared after $max_attempts attempts (${max_attempts}×${wait_seconds}s = $((max_attempts * wait_seconds))s)"
    echo
    echo "Troubleshooting:"
    echo "  - Check pgbackrest logs: tail $config_path/data/pooler_*/pg_data/log/pgbackrest-*.log"
    echo "  - Verify S3 access: aws s3 ls $backup_url --region $region"
    echo "  - Check cluster status: ./bin/multigres cluster status --config-path=$config_path"
    exit 1
  fi
done

# Print success message and next steps
config_flag=""
if [[ "$config_path" != "./multigres_local" ]]; then
  config_flag=" --config-path=$config_path"
fi

cat <<EOF

Setup complete! Your S3-backed Multigres cluster is running.

Next steps:
  # Check cluster status
  ./bin/multigres cluster status$config_flag

  # List backups
  ./bin/multigres cluster list-backups$config_flag

  # Connect to multigateway
  psql -h localhost -p 15432 -U postgres -d postgres

  # Create additional backup
  ./bin/multigres cluster backup$config_flag

  # Stop cluster (preserves data)
  ./bin/multigres cluster stop$config_flag

  # Clean up everything
  ./bin/multigres cluster stop --clean$config_flag && rm -rf $config_path
EOF
