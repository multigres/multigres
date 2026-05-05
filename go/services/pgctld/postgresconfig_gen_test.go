// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
)

func TestNewPostgresServerConfig(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	tests := []struct {
		name        string
		poolerId    string
		wantCluster string
		wantDataDir string
	}{
		{
			name:        "basic config creation",
			poolerId:    "test-pooler-1",
			wantCluster: "main",
			wantDataDir: tempDir + "/pg_data",
		},
		{
			name:        "second pooler",
			poolerId:    "pooler-2",
			wantCluster: "main",
			wantDataDir: tempDir + "/pg_data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := GeneratePostgresServerConfig(tempDir, "postgres", []string{})
			require.NoError(t, err, "GeneratePostgresServerConfig should not return error")

			assert.Equal(t, tt.wantCluster, config.ClusterName, "ClusterName should match expected value")
			assert.Equal(t, tt.wantDataDir, config.DataDir, "DataDir should match expected value")
		})
	}
}

func TestPostgresBaseDir(t *testing.T) {
	tempDir := t.TempDir()
	expected := tempDir + "/pg_data"
	t.Setenv(constants.PgDataDirEnvVar, expected)

	result := PostgresDataDir()

	assert.Equal(t, expected, result, "PostgresDataDir should return expected path")
}

func TestPostgresConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	expected := tempDir + "/pg_data/postgresql.conf"
	result := PostgresConfigFile()

	assert.Equal(t, expected, result, "PostgresConfigFile should return expected path")
}

func TestMakePostgresConf(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	config, err := GeneratePostgresServerConfig(tempDir, "postgres", []string{})
	require.NoError(t, err, "GeneratePostgresServerConfig should not return error")

	tests := []struct {
		name     string
		template string
		want     []string
		wantNot  []string
	}{
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
			name: "complex template",
			template: `# PostgreSQL Configuration
max_connections = {{.MaxConnections}}
data_directory = '{{.DataDir}}'
cluster_name = '{{.ClusterName}}'`,
			want: []string{
				"max_connections = 60",
				"data_directory = '" + tempDir + "/pg_data'",
				"cluster_name = 'main'",
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
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	config, err := GeneratePostgresServerConfig(tempDir, "postgres", []string{})
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

func TestGeneratePostgresServerConfigExtraConfFiles(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	extraDir := t.TempDir()
	extraA := filepath.Join(extraDir, "a.conf")
	extraB := filepath.Join(extraDir, "b.conf")

	// Extra A overrides shared_buffers; extra B overrides it again so the last
	// file wins. Also adds a setting not in the template at all.
	require.NoError(t, os.WriteFile(extraA, []byte("shared_buffers = 128MB\n"), 0o644))
	require.NoError(t, os.WriteFile(extraB, []byte("shared_buffers = 256MB\nlog_min_duration_statement = 500\n"), 0o644))

	cfg, err := GeneratePostgresServerConfig(tempDir, "postgres", []string{extraA, extraB})
	require.NoError(t, err)

	raw, err := os.ReadFile(cfg.Path)
	require.NoError(t, err)
	content := string(raw)

	// Source-attribution headers are present in append order.
	idxA := strings.Index(content, "## "+extraA)
	idxB := strings.Index(content, "## "+extraB)
	require.GreaterOrEqual(t, idxA, 0, "extra A header should be present")
	require.GreaterOrEqual(t, idxB, 0, "extra B header should be present")
	assert.Greater(t, idxB, idxA, "extra B should be appended after extra A")

	// Last-write-wins: the in-memory struct reflects the value postgres will see.
	assert.Equal(t, "256MB", cfg.SharedBuffers)

	// Settings absent from the template still land in the file.
	assert.Contains(t, content, "log_min_duration_statement = 500")
}

func TestGeneratePostgresServerConfigExtraConfFollowsInclude(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	extraDir := t.TempDir()
	overrides := filepath.Join(extraDir, "overrides.conf")
	extraFile := filepath.Join(extraDir, "extra.conf")

	require.NoError(t, os.WriteFile(overrides, []byte("shared_buffers = 512MB\n"), 0o644))
	require.NoError(t, os.WriteFile(extraFile, []byte("include '"+overrides+"'\n"), 0o644))

	cfg, err := GeneratePostgresServerConfig(tempDir, "postgres", []string{extraFile})
	require.NoError(t, err)

	// Postgres follows include directives at runtime, so our in-memory struct
	// must too — otherwise it disagrees with what the server actually loads.
	assert.Equal(t, "512MB", cfg.SharedBuffers)
}

func TestGeneratePostgresServerConfigExtraConfFileMissing(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv(constants.PgDataDirEnvVar, tempDir+"/pg_data")

	_, err := GeneratePostgresServerConfig(tempDir, "postgres", []string{"/does/not/exist.conf"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "/does/not/exist.conf")
}
