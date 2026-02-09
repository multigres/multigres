// Copyright 2025 Supabase, Inc.
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

package backup_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/backup"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/test/utils"
)

func TestNewConfig_Filesystem(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/var/backups")

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)
	assert.Equal(t, "filesystem", cfg.Type())
}

func TestConfig_FullPath_Filesystem(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/var/backups")

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	path, err := cfg.FullPath("mydb", "default", "0")
	require.NoError(t, err)
	assert.Equal(t, "/var/backups/mydb/default/0", path)
}

func TestConfig_FullPath_S3(t *testing.T) {
	loc := utils.S3BackupLocation("my-backups", "us-east-1",
		utils.WithS3KeyPrefix("prod/"))

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	path, err := cfg.FullPath("mydb", "default", "0")
	require.NoError(t, err)
	assert.Equal(t, "s3://my-backups/prod/mydb/default/0", path)
}

func TestConfig_PgBackRestConfig_Filesystem(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/var/backups")

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "posix", pgbrCfg["repo1-type"])
	assert.Equal(t, "/var/backups", pgbrCfg["repo1-path"])
}

func TestConfig_PgBackRestConfig_S3_Basic(t *testing.T) {
	loc := utils.S3BackupLocation("my-backups", "us-east-1")

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "s3", pgbrCfg["repo1-type"])
	assert.Equal(t, "my-backups", pgbrCfg["repo1-s3-bucket"])
	assert.Equal(t, "us-east-1", pgbrCfg["repo1-s3-region"])
	assert.Equal(t, "auto", pgbrCfg["repo1-s3-key-type"])
	assert.Equal(t, "/multigres", pgbrCfg["repo1-path"])

	// S3-specific performance and efficiency settings
	assert.Equal(t, "y", pgbrCfg["repo1-block"])
	assert.Equal(t, "y", pgbrCfg["repo1-bundle"])
	assert.Equal(t, "10MiB", pgbrCfg["repo1-storage-upload-chunk-size"])
	assert.Equal(t, "n", pgbrCfg["repo1-symlink"])

	// S3-specific retention policies
	assert.Equal(t, "1", pgbrCfg["repo1-retention-diff"])
	assert.Equal(t, "28", pgbrCfg["repo1-retention-full"])
	assert.Equal(t, "time", pgbrCfg["repo1-retention-full-type"])
	assert.Equal(t, "0", pgbrCfg["repo1-retention-history"])
}

func TestConfig_PgBackRestConfig_S3_WithPrefix(t *testing.T) {
	loc := utils.S3BackupLocation("my-backups", "us-east-1",
		utils.WithS3KeyPrefix("prod/"))

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	assert.Equal(t, "/prod/multigres", pgbrCfg["repo1-path"])
}

func TestConfig_PgBackRestConfig_S3_WithEndpoint(t *testing.T) {
	loc := utils.S3BackupLocation("my-backups", "us-east-1",
		utils.WithS3Endpoint("https://s3.example.com"))

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	// Custom endpoint should be used as-is
	assert.Equal(t, "https://s3.example.com", pgbrCfg["repo1-s3-endpoint"])
	// Custom endpoints should use path-style URIs
	assert.Equal(t, "path", pgbrCfg["repo1-s3-uri-style"])
	// Custom endpoints should skip TLS verification
	assert.Equal(t, "n", pgbrCfg["repo1-s3-verify-tls"])
}

func TestConfig_PgBackRestConfig_S3_AWS_AutoEndpoint(t *testing.T) {
	loc := utils.S3BackupLocation("my-backups", "us-east-1")

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	pgbrCfg, err := cfg.PgBackRestConfig("multigres")
	require.NoError(t, err)

	// AWS S3 should auto-generate endpoint
	assert.Equal(t, "s3.us-east-1.amazonaws.com", pgbrCfg["repo1-s3-endpoint"])
	// AWS S3 should use virtual-hosted style
	assert.Equal(t, "host", pgbrCfg["repo1-s3-uri-style"])
	// AWS S3 should use TLS verification (not explicitly set, defaults to y)
	assert.NotContains(t, pgbrCfg, "repo1-s3-verify-tls")
}

func TestConfig_PgBackRestConfig_S3_AWS_MultipleRegions(t *testing.T) {
	tests := []struct {
		name             string
		region           string
		expectedEndpoint string
	}{
		{
			name:             "us-west-2",
			region:           "us-west-2",
			expectedEndpoint: "s3.us-west-2.amazonaws.com",
		},
		{
			name:             "eu-west-1",
			region:           "eu-west-1",
			expectedEndpoint: "s3.eu-west-1.amazonaws.com",
		},
		{
			name:             "ap-southeast-1",
			region:           "ap-southeast-1",
			expectedEndpoint: "s3.ap-southeast-1.amazonaws.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc := utils.S3BackupLocation("my-backups", tt.region)

			cfg, err := backup.NewConfig(loc)
			require.NoError(t, err)

			pgbrCfg, err := cfg.PgBackRestConfig("multigres")
			require.NoError(t, err)

			assert.Equal(t, tt.expectedEndpoint, pgbrCfg["repo1-s3-endpoint"])
			assert.Equal(t, "host", pgbrCfg["repo1-s3-uri-style"])
		})
	}
}

func TestNewConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		loc     *clustermetadatapb.BackupLocation
		wantErr string
	}{
		{
			name:    "nil location",
			loc:     nil,
			wantErr: "backup location cannot be nil",
		},
		{
			name:    "no location set",
			loc:     &clustermetadatapb.BackupLocation{},
			wantErr: "no backup location configured",
		},
		{
			name:    "filesystem missing path",
			loc:     utils.FilesystemBackupLocation(""),
			wantErr: "filesystem path is required",
		},
		{
			name:    "s3 missing bucket",
			loc:     utils.S3BackupLocation("", "us-east-1"),
			wantErr: "s3 bucket is required",
		},
		{
			name:    "s3 missing region",
			loc:     utils.S3BackupLocation("my-backups", ""),
			wantErr: "s3 region is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := backup.NewConfig(tt.loc)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfig_FullPath_Validation(t *testing.T) {
	loc := utils.FilesystemBackupLocation("/var/backups")

	cfg, err := backup.NewConfig(loc)
	require.NoError(t, err)

	tests := []struct {
		name       string
		database   string
		tableGroup string
		shard      string
		wantErr    string
	}{
		{
			name:       "empty database",
			database:   "",
			tableGroup: "default",
			shard:      "0",
			wantErr:    "database cannot be empty",
		},
		{
			name:       "empty tableGroup",
			database:   "mydb",
			tableGroup: "",
			shard:      "0",
			wantErr:    "table group cannot be empty",
		},
		{
			name:       "empty shard",
			database:   "mydb",
			tableGroup: "default",
			shard:      "",
			wantErr:    "shard cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cfg.FullPath(tt.database, tt.tableGroup, tt.shard)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestUsesEnvCredentials(t *testing.T) {
	tests := []struct {
		name     string
		location *clustermetadatapb.BackupLocation
		expected bool
	}{
		{
			name: "S3 with UseEnvCredentials true",
			location: utils.S3BackupLocation("test-bucket", "us-east-1",
				utils.WithS3EnvCredentials()),
			expected: true,
		},
		{
			name:     "S3 with UseEnvCredentials false",
			location: utils.S3BackupLocation("test-bucket", "us-east-1"),
			expected: false,
		},
		{
			name:     "Filesystem backup",
			location: utils.FilesystemBackupLocation("/backups"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := backup.NewConfig(tt.location)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, cfg.UsesEnvCredentials())
		})
	}
}

func TestPgBackRestConfig_SharedCredentials(t *testing.T) {
	// Set environment variables for test
	os.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")

	location := utils.S3BackupLocation("test-bucket", "us-east-1",
		utils.WithS3EnvCredentials())

	cfg, err := backup.NewConfig(location)
	require.NoError(t, err)

	result, err := cfg.PgBackRestConfig("test-stanza")
	require.NoError(t, err)

	assert.Equal(t, "s3", result["repo1-type"])
	assert.Equal(t, "shared", result["repo1-s3-key-type"])
	// Credentials should NOT be in main config when UseEnvCredentials is true
	assert.NotContains(t, result, "repo1-s3-key")
	assert.NotContains(t, result, "repo1-s3-key-secret")
	assert.Equal(t, "test-bucket", result["repo1-s3-bucket"])
	assert.Equal(t, "us-east-1", result["repo1-s3-region"])
}

func TestPgBackRestConfig_AutoCredentials(t *testing.T) {
	location := utils.S3BackupLocation("test-bucket", "us-east-1")

	cfg, err := backup.NewConfig(location)
	require.NoError(t, err)

	result, err := cfg.PgBackRestConfig("test-stanza")
	require.NoError(t, err)

	assert.Equal(t, "s3", result["repo1-type"])
	assert.Equal(t, "auto", result["repo1-s3-key-type"])
	assert.NotContains(t, result, "repo1-s3-key")
	assert.NotContains(t, result, "repo1-s3-key-secret")
}

func TestPgBackRestConfig_ExcludesCredentialsWhenSeparate(t *testing.T) {
	// Set environment variables for test
	os.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	os.Setenv("AWS_SESSION_TOKEN", "test-token")
	defer os.Unsetenv("AWS_ACCESS_KEY_ID")
	defer os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	defer os.Unsetenv("AWS_SESSION_TOKEN")

	location := utils.S3BackupLocation("test-bucket", "us-east-1",
		utils.WithS3EnvCredentials())

	cfg, err := backup.NewConfig(location)
	require.NoError(t, err)

	result, err := cfg.PgBackRestConfig("test-stanza")
	require.NoError(t, err)

	// Should NOT contain credentials when separate file is needed
	assert.NotContains(t, result, "repo1-s3-key")
	assert.NotContains(t, result, "repo1-s3-key-secret")
	assert.NotContains(t, result, "repo1-s3-token")

	// Should still contain other S3 config
	assert.Equal(t, "s3", result["repo1-type"])
	assert.Equal(t, "test-bucket", result["repo1-s3-bucket"])
	assert.Equal(t, "us-east-1", result["repo1-s3-region"])
}

func TestPgBackRestCredentials(t *testing.T) {
	tests := []struct {
		name     string
		location *clustermetadatapb.BackupLocation
		envVars  map[string]string
		want     map[string]string
		wantErr  bool
	}{
		{
			name: "S3 with env credentials - all vars set",
			location: utils.S3BackupLocation("test-bucket", "us-east-1",
				utils.WithS3EnvCredentials()),
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"AWS_SESSION_TOKEN":     "FwoGZXIvYXdzEBYaDEXAMPLE",
			},
			want: map[string]string{
				"repo1-s3-key":        "AKIAIOSFODNN7EXAMPLE",
				"repo1-s3-key-secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"repo1-s3-token":      "FwoGZXIvYXdzEBYaDEXAMPLE",
			},
			wantErr: false,
		},
		{
			name: "S3 with env credentials - no session token",
			location: utils.S3BackupLocation("test-bucket", "us-east-1",
				utils.WithS3EnvCredentials()),
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAIOSFODNN7EXAMPLE",
				"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			want: map[string]string{
				"repo1-s3-key":        "AKIAIOSFODNN7EXAMPLE",
				"repo1-s3-key-secret": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			wantErr: false,
		},
		{
			name: "S3 with env credentials - missing access key",
			location: utils.S3BackupLocation("test-bucket", "us-east-1",
				utils.WithS3EnvCredentials()),
			envVars: map[string]string{
				"AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "S3 with env credentials - missing secret key",
			location: utils.S3BackupLocation("test-bucket", "us-east-1",
				utils.WithS3EnvCredentials()),
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:     "S3 without env credentials",
			location: utils.S3BackupLocation("test-bucket", "us-east-1"),
			envVars:  map[string]string{},
			want:     nil,
			wantErr:  false,
		},
		{
			name:     "Filesystem backup",
			location: utils.FilesystemBackupLocation("/backups"),
			envVars:  map[string]string{},
			want:     nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all AWS env vars first
			os.Unsetenv("AWS_ACCESS_KEY_ID")
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			os.Unsetenv("AWS_SESSION_TOKEN")

			// Set test env vars
			for k, v := range tt.envVars {
				os.Setenv(k, v)
				defer os.Unsetenv(k)
			}

			cfg, err := backup.NewConfig(tt.location)
			require.NoError(t, err)

			got, err := cfg.PgBackRestCredentials()
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
