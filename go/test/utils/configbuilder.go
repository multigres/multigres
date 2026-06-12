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

package utils

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// TestConfigBuilder builds multigres.yaml configurations for testing
type TestConfigBuilder struct {
	provisioner string
	backupType  string
	localPath   string
	s3Config    *S3TestConfig
}

// S3TestConfig holds S3 backup configuration for tests
type S3TestConfig struct {
	Bucket      string
	Region      string
	Endpoint    string
	UseEnvCreds bool
	KeyPrefix   string
}

// NewTestConfig creates a new test config builder
func NewTestConfig() *TestConfigBuilder {
	return &TestConfigBuilder{
		provisioner: "local",
	}
}

// WithLocalBackup configures local backup storage
func (b *TestConfigBuilder) WithLocalBackup(path string) *TestConfigBuilder {
	b.backupType = "local"
	b.localPath = path
	b.s3Config = nil
	return b
}

// WithS3Backup configures S3 backup storage
func (b *TestConfigBuilder) WithS3Backup(bucket, region string) *TestConfigBuilder {
	b.backupType = "s3"
	b.s3Config = &S3TestConfig{
		Bucket:      bucket,
		Region:      region,
		UseEnvCreds: true,
	}
	b.localPath = ""
	return b
}

// WithEndpoint sets the S3 endpoint (for s3mock, etc.)
func (b *TestConfigBuilder) WithEndpoint(endpoint string) *TestConfigBuilder {
	if b.s3Config != nil {
		b.s3Config.Endpoint = endpoint
	}
	return b
}

// WithEnvCredentials configures whether to use environment credentials
func (b *TestConfigBuilder) WithEnvCredentials(use bool) *TestConfigBuilder {
	if b.s3Config != nil {
		b.s3Config.UseEnvCreds = use
	}
	return b
}

// WithKeyPrefix sets the S3 key prefix
func (b *TestConfigBuilder) WithKeyPrefix(prefix string) *TestConfigBuilder {
	if b.s3Config != nil {
		b.s3Config.KeyPrefix = prefix
	}
	return b
}

// buildBackupConfig creates the backup config map
func (b *TestConfigBuilder) buildBackupConfig() map[string]any {
	config := map[string]any{
		"type": b.backupType,
	}

	switch b.backupType {
	case "local":
		if b.localPath != "" {
			config["local"] = map[string]any{
				"path": b.localPath,
			}
		}
	case "s3":
		if b.s3Config != nil {
			s3Map := map[string]any{
				"bucket":              b.s3Config.Bucket,
				"region":              b.s3Config.Region,
				"use-env-credentials": b.s3Config.UseEnvCreds,
			}
			if b.s3Config.Endpoint != "" {
				s3Map["endpoint"] = b.s3Config.Endpoint
			}
			if b.s3Config.KeyPrefix != "" {
				s3Map["key-prefix"] = b.s3Config.KeyPrefix
			}
			config["s3"] = s3Map
		}
	}

	return config
}

// WriteToDir writes the config to multigres.yaml in the specified directory
func (b *TestConfigBuilder) WriteToDir(t *testing.T, dir string) {
	t.Helper()

	config := map[string]any{
		"provisioner": b.provisioner,
	}

	if b.backupType != "" {
		config["provisioner-config"] = map[string]any{
			"backup": b.buildBackupConfig(),
		}
	}

	data, err := yaml.Marshal(config)
	require.NoError(t, err)

	err = os.WriteFile(filepath.Join(dir, "multigres.yaml"), data, 0o644)
	require.NoError(t, err)
}

// PgBackrestOpts holds options for pgbackrest.conf generation
type PgBackrestOpts struct {
	AccessKey    string
	SecretKey    string
	SessionToken string
	Bucket       string
	Region       string
	Endpoint     string
}

// WritePgBackrestConfig creates a pgbackrest.conf file with test credentials
func WritePgBackrestConfig(t *testing.T, dir string, opts PgBackrestOpts) {
	t.Helper()

	poolerDir := filepath.Join(dir, "data", "pooler_001", "pgbackrest")
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))

	conf := buildPgBackrestConfig(opts)
	err := os.WriteFile(filepath.Join(poolerDir, "pgbackrest.conf"), []byte(conf), 0o600)
	require.NoError(t, err)
}

// buildPgBackrestConfig generates pgbackrest.conf content
func buildPgBackrestConfig(opts PgBackrestOpts) string {
	var lines []string

	lines = append(lines, "[global]")
	lines = append(lines, "log-path=/var/log/pgbackrest")

	if opts.AccessKey != "" && opts.SecretKey != "" {
		lines = append(lines, "repo1-s3-key-type=shared")
		lines = append(lines, "repo1-s3-key="+opts.AccessKey)
		lines = append(lines, "repo1-s3-key-secret="+opts.SecretKey)
		if opts.SessionToken != "" {
			lines = append(lines, "repo1-s3-token="+opts.SessionToken)
		}
	}

	lines = append(lines, "")
	lines = append(lines, "[multigres]")
	lines = append(lines, "repo1-type=s3")

	if opts.Bucket != "" {
		lines = append(lines, "repo1-s3-bucket="+opts.Bucket)
	}

	if opts.Region != "" {
		lines = append(lines, "repo1-s3-region="+opts.Region)
	}

	if opts.Endpoint != "" {
		lines = append(lines, "repo1-s3-endpoint="+opts.Endpoint)
	}

	return strings.Join(lines, "\n") + "\n"
}

// FilesystemBackupLocation creates a BackupLocation for filesystem backups.
func FilesystemBackupLocation(path string) *clustermetadatapb.BackupLocation {
	return &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_Filesystem{
			Filesystem: &clustermetadatapb.FilesystemBackup{
				Path: path,
			},
		},
	}
}

// S3BackupLocation creates a BackupLocation for S3 backups with basic configuration.
// Use S3Option functions to customize the configuration.
func S3BackupLocation(bucket, region string, opts ...S3Option) *clustermetadatapb.BackupLocation {
	s3 := &clustermetadatapb.S3Backup{
		Bucket: bucket,
		Region: region,
	}

	for _, opt := range opts {
		opt(s3)
	}

	return &clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: s3,
		},
	}
}

// S3Option is a functional option for configuring S3 backup locations.
type S3Option func(*clustermetadatapb.S3Backup)

// WithS3KeyPrefix sets the key prefix for S3 backups.
func WithS3KeyPrefix(prefix string) S3Option {
	return func(s3 *clustermetadatapb.S3Backup) {
		s3.KeyPrefix = prefix
	}
}

// WithS3Endpoint sets a custom endpoint for S3 backups (e.g., s3mock).
func WithS3Endpoint(endpoint string) S3Option {
	return func(s3 *clustermetadatapb.S3Backup) {
		s3.Endpoint = endpoint
	}
}

// WithS3EnvCredentials configures S3 to use environment credentials.
func WithS3EnvCredentials() S3Option {
	return func(s3 *clustermetadatapb.S3Backup) {
		s3.UseEnvCredentials = true
	}
}
