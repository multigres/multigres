// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgctld

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

func TestGeneratePgBackRestConfig(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := PgBackRestConfig{
		PoolerDir:     tmpDir,
		CertDir:       "/certs",
		Port:          8443,
		Pg1Port:       5432,
		Pg1SocketPath: filepath.Join(tmpDir, "socket"),
		Pg1Path:       "/var/lib/postgresql/data",
	}

	// S3 backup config
	backupLoc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:            "test-bucket",
				Region:            "us-west-2",
				KeyPrefix:         "backups",
				UseEnvCredentials: false, // Use auto credential detection for tests
			},
		},
	}
	backupConfig, err := backup.NewConfig(backupLoc)
	require.NoError(t, err)

	configPath, err := GeneratePgBackRestConfig(cfg, backupConfig)
	require.NoError(t, err)

	// Verify file was created
	assert.FileExists(t, configPath)

	// Read and verify content
	content, err := os.ReadFile(configPath)
	require.NoError(t, err)

	contentStr := string(content)
	assert.Contains(t, contentStr, "tls-server-port=8443")
	assert.Contains(t, contentStr, "pg1-port=5432")
	assert.Contains(t, contentStr, "[multigres]")
	assert.Contains(t, contentStr, "repo1-type=s3")
	assert.Contains(t, contentStr, "repo1-s3-bucket=test-bucket")

	// TLS certificate configuration
	assert.Contains(t, contentStr, "tls-server-cert-file=/certs/pgbackrest.crt")
	assert.Contains(t, contentStr, "tls-server-key-file=/certs/pgbackrest.key")
	assert.Contains(t, contentStr, "tls-server-ca-file=/certs/ca.crt")

	// Should NOT contain pg2-* settings (those are for backup only)
	assert.NotContains(t, contentStr, "pg2-host")
}

func TestGeneratePgBackRestConfig_Filesystem(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := PgBackRestConfig{
		PoolerDir:     tmpDir,
		CertDir:       "/certs",
		Port:          8443,
		Pg1Port:       5432,
		Pg1SocketPath: filepath.Join(tmpDir, "socket"),
		Pg1Path:       "/var/lib/postgresql/data",
	}

	// Filesystem backup config
	backupLoc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: "/backups",
			},
		},
	}
	backupConfig, err := backup.NewConfig(backupLoc)
	require.NoError(t, err)

	configPath, err := GeneratePgBackRestConfig(cfg, backupConfig)
	require.NoError(t, err)

	content, err := os.ReadFile(configPath)
	require.NoError(t, err)

	contentStr := string(content)
	assert.Contains(t, contentStr, "repo1-type=posix")
	assert.Contains(t, contentStr, "repo1-path=/backups")
}

func TestGeneratePgBackRestConfig_S3_WithEnvCredentials(t *testing.T) {
	tmpDir := t.TempDir()

	// Set mock AWS credentials in environment (without session token)
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "") // Explicitly clear any inherited session token

	cfg := PgBackRestConfig{
		PoolerDir:     tmpDir,
		CertDir:       "/certs",
		Port:          8443,
		Pg1Port:       5432,
		Pg1SocketPath: filepath.Join(tmpDir, "socket"),
		Pg1Path:       "/var/lib/postgresql/data",
	}

	// S3 backup config with UseEnvCredentials=true (local dev scenario)
	backupLoc := &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:            "my-production-bucket",
				Region:            "us-east-1",
				KeyPrefix:         "multigres/",
				UseEnvCredentials: true, // Local dev with AWS S3
			},
		},
	}
	backupConfig, err := backup.NewConfig(backupLoc)
	require.NoError(t, err)

	configPath, err := GeneratePgBackRestConfig(cfg, backupConfig)
	require.NoError(t, err)

	content, err := os.ReadFile(configPath)
	require.NoError(t, err)

	contentStr := string(content)

	// Should use "shared" key type for env credentials
	assert.Contains(t, contentStr, "repo1-s3-key-type=shared")

	// Should NOT use "auto" (which triggers EC2 metadata)
	assert.NotContains(t, contentStr, "repo1-s3-key-type=auto")

	// Should generate AWS endpoint (not custom)
	assert.Contains(t, contentStr, "repo1-s3-endpoint=s3.us-east-1.amazonaws.com")

	// Should embed credentials from environment in config
	assert.Contains(t, contentStr, "repo1-s3-key=test-access-key")
	assert.Contains(t, contentStr, "repo1-s3-key-secret=test-secret-key")

	// Should NOT contain session token if not set
	assert.NotContains(t, contentStr, "repo1-s3-token=")
}
