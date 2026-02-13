# Development Tools

## S3 Cluster Setup Script

**Script:** `tools/setup_s3_cluster.sh`

Automates setup of a local Multigres cluster with S3 backups for development and testing.

### Usage

```bash
./tools/setup_s3_cluster.sh \
  --credentials-file <path-to-credentials> \
  --backup-url s3://bucket/prefix/ \
  --region <aws-region>
```

### Prerequisites

- Multigres binary built (`make build`)
- No existing cluster at `./multigres_local/`
- AWS credentials file with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- S3 bucket accessible with the provided credentials

### Example

```bash
# Create credentials file
cat > ~/.aws/staging-creds <<'EOF'
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_SESSION_TOKEN=...  # optional
EOF

# Run setup
./tools/setup_s3_cluster.sh \
  --credentials-file ~/.aws/staging-creds \
  --backup-url s3://my-test-bucket/multigres-backups/ \
  --region us-east-1

# Script will:
# 1. Validate credentials and parameters
# 2. Initialize cluster with S3 configuration
# 3. Start all cluster components
# 4. Wait for bootstrap backup to appear (up to 2.5 minutes)
# 5. Print next steps
```

### Cleanup

```bash
./bin/multigres cluster stop --clean
rm -rf multigres_local
```
