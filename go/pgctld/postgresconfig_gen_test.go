/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pgctld

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPostgresServerConfig(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()
	originalPoolerDir := poolerDir
	defer func() { poolerDir = originalPoolerDir }()
	poolerDir = tempDir

	tests := []struct {
		name          string
		poolerId      string
		port          int
		wantPort      int
		wantCluster   string
		wantDataDir   string
		wantSocketDir string
	}{
		{
			name:          "basic config creation",
			poolerId:      "test-pooler-1",
			port:          5432,
			wantPort:      5432,
			wantCluster:   "main",
			wantDataDir:   tempDir + "/pg_data",
			wantSocketDir: tempDir + "/pg_sockets",
		},
		{
			name:          "custom port",
			poolerId:      "pooler-2",
			port:          5433,
			wantPort:      5433,
			wantCluster:   "main",
			wantDataDir:   tempDir + "/pg_data",
			wantSocketDir: tempDir + "/pg_sockets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := GeneratePostgresServerConfig(tempDir, tt.port, "postgres")
			require.NoError(t, err, "GeneratePostgresServerConfig should not return error")

			assert.Equal(t, tt.wantPort, config.Port, "Port should match expected value")

			assert.Equal(t, tt.wantCluster, config.ClusterName, "ClusterName should match expected value")

			assert.Equal(t, tt.wantDataDir, config.DataDir, "DataDir should match expected value")

			assert.Equal(t, "localhost", config.ListenAddresses, "ListenAddresses should be localhost")

			assert.Equal(t, tt.wantSocketDir, config.UnixSocketDirectories, "UnixSocketDirectories should match expected value")
		})
	}
}

func TestPostgresBaseDir(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()
	originalPoolerDir := poolerDir
	defer func() { poolerDir = originalPoolerDir }()
	poolerDir = tempDir

	expected := tempDir + "/pg_data"
	result := PostgresDataDir(tempDir)

	assert.Equal(t, expected, result, "PostgresDataDir should return expected path")
}

func TestPostgresConfigFile(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()
	originalPoolerDir := poolerDir
	defer func() { poolerDir = originalPoolerDir }()
	poolerDir = tempDir

	expected := tempDir + "/pg_data/postgresql.conf"
	result := PostgresConfigFile(tempDir)

	assert.Equal(t, expected, result, "PostgresConfigFile should return expected path")
}

func TestMakePostgresConf(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()
	originalPoolerDir := poolerDir
	defer func() { poolerDir = originalPoolerDir }()
	poolerDir = tempDir

	config, err := GeneratePostgresServerConfig(tempDir, 5432, "postgres")
	require.NoError(t, err, "GeneratePostgresServerConfig should not return error")

	tests := []struct {
		name     string
		template string
		want     []string
		wantNot  []string
	}{
		{
			name:     "port template",
			template: "port = {{.Port}}",
			want:     []string{"port = 5432"},
		},
		{
			name:     "cluster name template",
			template: "cluster_name = '{{.ClusterName}}'",
			want:     []string{"cluster_name = 'main'"},
		},
		{
			name:     "data directory template",
			template: "data_directory = '{{.DataDir}}'",
			want:     []string{"data_directory = '" + tempDir + "/pg_data'"},
		},
		{
			name:     "max connections template",
			template: "max_connections = {{.MaxConnections}}",
			want:     []string{"max_connections = 60"},
		},
		{
			name:     "listen addresses template",
			template: "listen_addresses = '{{.ListenAddresses}}'",
			want:     []string{"listen_addresses = 'localhost'"},
		},
		{
			name:     "unix socket directories template",
			template: "unix_socket_directories = '{{.UnixSocketDirectories}}'",
			want:     []string{"unix_socket_directories = '" + tempDir + "/pg_sockets'"},
		},
		{
			name: "complex template",
			template: `# PostgreSQL Configuration
port = {{.Port}}
max_connections = {{.MaxConnections}}
listen_addresses = '{{.ListenAddresses}}'
data_directory = '{{.DataDir}}'
cluster_name = '{{.ClusterName}}'
unix_socket_directories = '{{.UnixSocketDirectories}}'`,
			want: []string{
				"port = 5432",
				"max_connections = 60",
				"listen_addresses = 'localhost'",
				"data_directory = '" + tempDir + "/pg_data'",
				"cluster_name = 'main'",
				"unix_socket_directories = '" + tempDir + "/pg_sockets'",
				"# PostgreSQL Configuration",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := config.MakePostgresConf(tt.template)
			require.NoError(t, err, "MakePostgresConf should not return error")

			// Check that wanted strings are present
			for _, want := range tt.want {
				assert.Contains(t, result, want, "Result should contain expected string")
			}

			// Check that unwanted strings are not present
			for _, wantNot := range tt.wantNot {
				assert.NotContains(t, result, wantNot, "Result should not contain unwanted string")
			}
		})
	}
}

func TestMakePostgresConfInvalidTemplate(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()
	originalPoolerDir := poolerDir
	defer func() { poolerDir = originalPoolerDir }()
	poolerDir = tempDir

	config, err := GeneratePostgresServerConfig(tempDir, 5432, "postgres")
	require.NoError(t, err, "GeneratePostgresServerConfig should not return error")

	tests := []struct {
		name     string
		template string
	}{
		{
			name:     "invalid template syntax",
			template: "port = {{.Port",
		},
		{
			name:     "unknown field",
			template: "unknown = {{.UnknownField}}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.MakePostgresConf(tt.template)
			assert.Error(t, err, "MakePostgresConf should return error for invalid template")
		})
	}
}

func TestGetPoolerDir(t *testing.T) {
	// Set up poolerDir for testing using temporary directory
	tempDir := t.TempDir()
	originalPoolerDir := poolerDir
	defer func() { poolerDir = originalPoolerDir }()

	poolerDir = tempDir

	result := GetPoolerDir()
	assert.Equal(t, tempDir, result, "GetPoolerDir should return configured directory")

	// Test empty case
	poolerDir = ""
	result = GetPoolerDir()
	assert.Equal(t, "", result, "GetPoolerDir should return empty string when not configured")
}
