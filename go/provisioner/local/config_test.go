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

package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestBuildBackupConfig_S3UseEnvCredentials(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected BackupConfig
	}{
		{
			name: "s3 with env credentials true",
			input: map[string]string{
				"type":                   "s3",
				"s3-bucket":              "test-bucket",
				"s3-region":              "us-east-1",
				"s3-use-env-credentials": "true",
			},
			expected: BackupConfig{
				Type: "s3",
				S3: &S3Backup{
					Bucket:            "test-bucket",
					Region:            "us-east-1",
					UseEnvCredentials: true,
				},
			},
		},
		{
			name: "s3 with env credentials false",
			input: map[string]string{
				"type":      "s3",
				"s3-bucket": "test-bucket",
				"s3-region": "us-east-1",
			},
			expected: BackupConfig{
				Type: "s3",
				S3: &S3Backup{
					Bucket:            "test-bucket",
					Region:            "us-east-1",
					UseEnvCredentials: false,
				},
			},
		},
		{
			name: "s3 with all fields",
			input: map[string]string{
				"type":                   "s3",
				"s3-bucket":              "test-bucket",
				"s3-region":              "us-west-2",
				"s3-endpoint":            "https://s3.example.com",
				"s3-key-prefix":          "backups/",
				"s3-use-env-credentials": "true",
			},
			expected: BackupConfig{
				Type: "s3",
				S3: &S3Backup{
					Bucket:            "test-bucket",
					Region:            "us-west-2",
					Endpoint:          "https://s3.example.com",
					KeyPrefix:         "backups/",
					UseEnvCredentials: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildBackupConfig(tt.input, "/tmp/test")
			assert.Equal(t, tt.expected.Type, result.Type)
			require.NotNil(t, result.S3)
			assert.Equal(t, tt.expected.S3.Bucket, result.S3.Bucket)
			assert.Equal(t, tt.expected.S3.Region, result.S3.Region)
			assert.Equal(t, tt.expected.S3.Endpoint, result.S3.Endpoint)
			assert.Equal(t, tt.expected.S3.KeyPrefix, result.S3.KeyPrefix)
			assert.Equal(t, tt.expected.S3.UseEnvCredentials, result.S3.UseEnvCredentials)
		})
	}
}

func TestBuildBackupConfig_NestedStructs(t *testing.T) {
	tests := []struct {
		name      string
		input     map[string]string
		baseDir   string
		wantType  string
		checkFunc func(*testing.T, BackupConfig)
	}{
		{
			name: "local with explicit path",
			input: map[string]string{
				"type": "local",
				"path": "/custom/backups",
			},
			baseDir:  "/base",
			wantType: "local",
			checkFunc: func(t *testing.T, cfg BackupConfig) {
				require.NotNil(t, cfg.Local)
				require.Equal(t, "/custom/backups", cfg.Local.Path)
				require.Nil(t, cfg.S3)
			},
		},
		{
			name: "local with default path",
			input: map[string]string{
				"type": "local",
			},
			baseDir:  "/base",
			wantType: "local",
			checkFunc: func(t *testing.T, cfg BackupConfig) {
				require.NotNil(t, cfg.Local)
				require.Equal(t, "/base/data/backups", cfg.Local.Path)
				require.Nil(t, cfg.S3)
			},
		},
		{
			name: "s3 with all fields",
			input: map[string]string{
				"type":                   "s3",
				"s3-bucket":              "test-bucket",
				"s3-region":              "us-west-2",
				"s3-endpoint":            "https://s3.example.com",
				"s3-key-prefix":          "backups/",
				"s3-use-env-credentials": "true",
			},
			baseDir:  "/base",
			wantType: "s3",
			checkFunc: func(t *testing.T, cfg BackupConfig) {
				require.Nil(t, cfg.Local)
				require.NotNil(t, cfg.S3)
				require.Equal(t, "test-bucket", cfg.S3.Bucket)
				require.Equal(t, "us-west-2", cfg.S3.Region)
				require.Equal(t, "https://s3.example.com", cfg.S3.Endpoint)
				require.Equal(t, "backups/", cfg.S3.KeyPrefix)
				require.True(t, cfg.S3.UseEnvCredentials)
			},
		},
		{
			name: "s3 with minimal fields",
			input: map[string]string{
				"type":      "s3",
				"s3-bucket": "minimal-bucket",
				"s3-region": "eu-west-1",
			},
			baseDir:  "/base",
			wantType: "s3",
			checkFunc: func(t *testing.T, cfg BackupConfig) {
				require.Nil(t, cfg.Local)
				require.NotNil(t, cfg.S3)
				require.Equal(t, "minimal-bucket", cfg.S3.Bucket)
				require.Equal(t, "eu-west-1", cfg.S3.Region)
				require.Empty(t, cfg.S3.Endpoint)
				require.Empty(t, cfg.S3.KeyPrefix)
				require.False(t, cfg.S3.UseEnvCredentials)
			},
		},
		{
			name:     "default to local when type empty",
			input:    map[string]string{},
			baseDir:  "/base",
			wantType: "local",
			checkFunc: func(t *testing.T, cfg BackupConfig) {
				require.NotNil(t, cfg.Local)
				require.Equal(t, "/base/data/backups", cfg.Local.Path)
				require.Nil(t, cfg.S3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildBackupConfig(tt.input, tt.baseDir)
			require.Equal(t, tt.wantType, result.Type)
			tt.checkFunc(t, result)
		})
	}
}

func TestBackupConfig_YAML_Serialization(t *testing.T) {
	tests := []struct {
		name     string
		config   BackupConfig
		wantYAML string
	}{
		{
			name: "s3 config",
			config: BackupConfig{
				Type: "s3",
				S3: &S3Backup{
					Bucket: "my-bucket",
					Region: "us-east-1",
				},
			},
			wantYAML: `type: s3
s3:
    bucket: my-bucket
    region: us-east-1
`,
		},
		{
			name: "s3 config with all fields",
			config: BackupConfig{
				Type: "s3",
				S3: &S3Backup{
					Bucket:            "my-bucket",
					Region:            "us-west-2",
					Endpoint:          "https://s3.example.com",
					KeyPrefix:         "backups/",
					UseEnvCredentials: true,
				},
			},
			wantYAML: `type: s3
s3:
    bucket: my-bucket
    region: us-west-2
    endpoint: https://s3.example.com
    key-prefix: backups/
    use-env-credentials: true
`,
		},
		{
			name: "local config",
			config: BackupConfig{
				Type: "local",
				Local: &LocalBackup{
					Path: "/path/to/backups",
				},
			},
			wantYAML: `type: local
local:
    path: /path/to/backups
`,
		},
		{
			name: "local config omits unused S3 fields",
			config: BackupConfig{
				Type: "local",
				Local: &LocalBackup{
					Path: "/backups",
				},
			},
			wantYAML: `type: local
local:
    path: /backups
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to YAML
			data, err := yaml.Marshal(tt.config)
			require.NoError(t, err)
			require.Equal(t, tt.wantYAML, string(data))

			// Unmarshal back and verify
			var parsed BackupConfig
			err = yaml.Unmarshal(data, &parsed)
			require.NoError(t, err)
			require.Equal(t, tt.config.Type, parsed.Type)

			if tt.config.S3 != nil {
				require.NotNil(t, parsed.S3)
				require.Equal(t, tt.config.S3.Bucket, parsed.S3.Bucket)
				require.Equal(t, tt.config.S3.Region, parsed.S3.Region)
				require.Equal(t, tt.config.S3.Endpoint, parsed.S3.Endpoint)
				require.Equal(t, tt.config.S3.KeyPrefix, parsed.S3.KeyPrefix)
				require.Equal(t, tt.config.S3.UseEnvCredentials, parsed.S3.UseEnvCredentials)
			}

			if tt.config.Local != nil {
				require.NotNil(t, parsed.Local)
				require.Equal(t, tt.config.Local.Path, parsed.Local.Path)
			}
		})
	}
}
