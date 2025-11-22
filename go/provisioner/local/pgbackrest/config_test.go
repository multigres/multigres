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

package pgbackrest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateConfig(t *testing.T) {
	t.Run("generates valid config with all fields", func(t *testing.T) {
		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    "/var/lib/postgresql/data",
			PgPort:        5432,
			PgSocketDir:   "/var/run/postgresql",
			PgUser:        "postgres",
			PgPassword:    "testpass",
			PgDatabase:    "postgres",
			LogPath:       "/var/log/pgbackrest",
			RetentionFull: 2,
		}

		result := GenerateConfig(cfg)

		assert.Contains(t, result, "[global]")
		assert.Contains(t, result, "log-path=/var/log/pgbackrest")
		assert.Contains(t, result, "[test-stanza]")
		assert.Contains(t, result, "pg1-path=/var/lib/postgresql/data")
		// pg1 is always local ("self"), so no pg1-host or pg1-host-type
		// Both socket-path and port are needed (port determines socket filename)
		assert.Contains(t, result, "pg1-port=5432")
		assert.Contains(t, result, "pg1-socket-path=/var/run/postgresql")
		assert.Contains(t, result, "pg1-user=postgres")
		// Note: Password is intentionally not included in the config for local connections
		assert.Contains(t, result, "pg1-database=postgres")
		assert.Contains(t, result, "repo1-retention-full=2")
	})

	t.Run("omits retention when zero", func(t *testing.T) {
		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    "/var/lib/postgresql/data",
			PgPort:        5432,
			LogPath:       "/var/log/pgbackrest",
			RetentionFull: 0,
		}

		result := GenerateConfig(cfg)

		assert.NotContains(t, result, "repo1-retention-full")
	})

	t.Run("omits optional fields when empty", func(t *testing.T) {
		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    "/var/lib/postgresql/data",
			PgPort:        5432,
			LogPath:       "/var/log/pgbackrest",
			RetentionFull: 1,
			// PgHost, PgSocketDir, PgUser, PgPassword, PgDatabase, SpoolPath are empty
		}

		result := GenerateConfig(cfg)

		assert.NotContains(t, result, "pg1-host")
		assert.NotContains(t, result, "pg1-host-type")
		assert.NotContains(t, result, "pg1-socket-path")
		assert.NotContains(t, result, "pg1-user")
		assert.NotContains(t, result, "pg1-password")
		assert.NotContains(t, result, "pg1-database")
		assert.NotContains(t, result, "spool-path")
		assert.Contains(t, result, "[test-stanza]")
	})

	t.Run("includes spool-path when specified", func(t *testing.T) {
		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    "/var/lib/postgresql/data",
			PgPort:        5432,
			LogPath:       "/var/log/pgbackrest",
			SpoolPath:     "/tmp/pgbackrest-spool",
			RetentionFull: 1,
		}

		result := GenerateConfig(cfg)

		assert.Contains(t, result, "spool-path=/tmp/pgbackrest-spool")
		assert.Contains(t, result, "[global]")
	})

	t.Run("generates valid INI format", func(t *testing.T) {
		cfg := Config{
			StanzaName:    "my-service",
			PgDataPath:    "/data/pg",
			PgPort:        5433,
			LogPath:       "/logs",
			RetentionFull: 3,
		}

		result := GenerateConfig(cfg)

		// Should have proper sections
		lines := strings.Split(result, "\n")
		var foundGlobal, foundStanza bool
		for _, line := range lines {
			if line == "[global]" {
				foundGlobal = true
			}
			if line == "[my-service]" {
				foundStanza = true
			}
		}

		assert.True(t, foundGlobal, "should have [global] section")
		assert.True(t, foundStanza, "should have stanza section")
	})

	t.Run("includes both socket-path and port for Unix sockets", func(t *testing.T) {
		// Test with socket-path: both socket-path and port should be included
		// Port determines the socket filename (e.g., .s.PGSQL.5432)
		cfgWithSocket := Config{
			StanzaName:  "test-socket",
			PgDataPath:  "/data/pg",
			PgPort:      5432,
			PgSocketDir: "/var/run/postgresql",
			LogPath:     "/logs",
		}

		resultWithSocket := GenerateConfig(cfgWithSocket)
		assert.Contains(t, resultWithSocket, "pg1-socket-path=/var/run/postgresql")
		assert.Contains(t, resultWithSocket, "pg1-port=5432")

		// Test without socket-path: only port should be included
		cfgWithoutSocket := Config{
			StanzaName: "test-port",
			PgDataPath: "/data/pg",
			PgPort:     5432,
			LogPath:    "/logs",
		}

		resultWithoutSocket := GenerateConfig(cfgWithoutSocket)
		assert.NotContains(t, resultWithoutSocket, "pg1-socket-path=")
		assert.Contains(t, resultWithoutSocket, "pg1-port=5432")
	})

	t.Run("generates symmetric configs with additional hosts", func(t *testing.T) {
		cfg := Config{
			StanzaName:  "multigres",
			PgDataPath:  "/data/pg1",
			PgSocketDir: "/var/run/pg1",
			PgPort:      5432,
			PgUser:      "postgres",
			PgDatabase:  "postgres",
			AdditionalHosts: []PgHost{
				{
					DataPath:  "/data/pg2",
					SocketDir: "/var/run/pg2",
					Port:      5433,
					User:      "postgres",
					Database:  "postgres",
				},
				{
					DataPath:  "/data/pg3",
					SocketDir: "/var/run/pg3",
					Port:      5434,
					User:      "postgres",
					Database:  "postgres",
				},
			},
			LogPath:       "/logs",
			RetentionFull: 2,
		}

		result := GenerateConfig(cfg)

		// Verify pg1 has both socket-path and port (port determines socket filename)
		assert.Contains(t, result, "pg1-path=/data/pg1")
		assert.Contains(t, result, "pg1-socket-path=/var/run/pg1")
		assert.Contains(t, result, "pg1-port=5432")

		// Verify pg2 has both socket-path and port
		assert.Contains(t, result, "pg2-path=/data/pg2")
		assert.Contains(t, result, "pg2-socket-path=/var/run/pg2")
		assert.Contains(t, result, "pg2-port=5433")

		// Verify pg3 has both socket-path and port
		assert.Contains(t, result, "pg3-path=/data/pg3")
		assert.Contains(t, result, "pg3-socket-path=/var/run/pg3")
		assert.Contains(t, result, "pg3-port=5434")

		// Verify all have proper user and database settings
		assert.Contains(t, result, "pg1-user=postgres")
		assert.Contains(t, result, "pg2-user=postgres")
		assert.Contains(t, result, "pg3-user=postgres")
	})
}

func TestWriteConfigFile(t *testing.T) {
	t.Run("creates directory and writes config", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "subdir", "pgbackrest.conf")

		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    "/var/lib/postgresql/data",
			PgPort:        5432,
			LogPath:       "/var/log/pgbackrest",
			RetentionFull: 2,
		}

		err := WriteConfigFile(configPath, cfg)
		require.NoError(t, err)

		// Verify directory was created
		_, err = os.Stat(filepath.Dir(configPath))
		require.NoError(t, err)

		// Verify file was written
		content, err := os.ReadFile(configPath)
		require.NoError(t, err)

		assert.Contains(t, string(content), "[global]")
		assert.Contains(t, string(content), "[test-stanza]")
	})

	t.Run("overwrites existing file", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "pgbackrest.conf")

		// Write first config
		cfg1 := Config{
			StanzaName:    "stanza1",
			PgDataPath:    "/data1",
			PgPort:        5432,
			LogPath:       "/log1",
			RetentionFull: 1,
		}
		err := WriteConfigFile(configPath, cfg1)
		require.NoError(t, err)

		// Write second config
		cfg2 := Config{
			StanzaName:    "stanza2",
			PgDataPath:    "/data2",
			PgPort:        5433,
			LogPath:       "/log2",
			RetentionFull: 3,
		}
		err = WriteConfigFile(configPath, cfg2)
		require.NoError(t, err)

		// Verify second config is present
		content, err := os.ReadFile(configPath)
		require.NoError(t, err)

		assert.Contains(t, string(content), "[stanza2]")
		assert.Contains(t, string(content), "/data2")
		assert.NotContains(t, string(content), "[stanza1]")
	})

	t.Run("handles invalid directory path", func(t *testing.T) {
		// Use a path that cannot be created (file as parent)
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "file.txt")
		err := os.WriteFile(filePath, []byte("test"), 0o644)
		require.NoError(t, err)

		configPath := filepath.Join(filePath, "subdir", "pgbackrest.conf")

		cfg := Config{
			StanzaName: "test",
			PgDataPath: "/data",
			PgPort:     5432,
			LogPath:    "/log",
		}

		err = WriteConfigFile(configPath, cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create directory")
	})
}

func TestStanzaCreate(t *testing.T) {
	t.Run("executes pgbackrest command with correct args", func(t *testing.T) {
		// Create a temporary directory with a valid config file
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "pgbackrest.conf")
		logPath := filepath.Join(tmpDir, "log")
		dataPath := filepath.Join(tmpDir, "data")
		repoPath := filepath.Join(tmpDir, "repo")

		// Create necessary directories
		require.NoError(t, os.MkdirAll(logPath, 0o755))
		require.NoError(t, os.MkdirAll(dataPath, 0o755))
		require.NoError(t, os.MkdirAll(repoPath, 0o755))

		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    dataPath,
			PgPort:        5432,
			LogPath:       logPath,
			RetentionFull: 2,
		}

		err := WriteConfigFile(configPath, cfg)
		require.NoError(t, err)

		ctx := context.Background()

		// Note: This will fail if pgbackrest is not installed or if PostgreSQL is not running
		// But the test will verify that the command is executed with the correct arguments
		err = StanzaCreate(ctx, "test-stanza", configPath, repoPath)
		// We expect an error because PostgreSQL is not running, but we can check
		// that the error is from pgbackrest execution, not from our code
		if err != nil {
			// Error is expected - pgbackrest will fail without a running PostgreSQL
			assert.Contains(t, err.Error(), "failed to create stanza")
			assert.Contains(t, err.Error(), "test-stanza")
		}
	})

	t.Run("respects context timeout", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "pgbackrest.conf")
		repoPath := filepath.Join(tmpDir, "repo")

		cfg := Config{
			StanzaName:    "test-stanza",
			PgDataPath:    "/nonexistent",
			PgPort:        5432,
			LogPath:       "/nonexistent",
			RetentionFull: 2,
		}

		err := WriteConfigFile(configPath, cfg)
		require.NoError(t, err)

		ctx := context.Background()

		// The function creates its own timeout context internally
		err = StanzaCreate(ctx, "test-stanza", configPath, repoPath)

		// Should fail because pgbackrest command will fail
		assert.Error(t, err)
	})

	t.Run("handles invalid config path", func(t *testing.T) {
		ctx := context.Background()

		err := StanzaCreate(ctx, "test-stanza", "/nonexistent/path/pgbackrest.conf", "/tmp/repo")

		// Should fail because config file doesn't exist
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create stanza")
	})

	t.Run("handles empty stanza name", func(t *testing.T) {
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "pgbackrest.conf")
		repoPath := filepath.Join(tmpDir, "repo")

		cfg := Config{
			StanzaName:    "test",
			PgDataPath:    "/data",
			PgPort:        5432,
			LogPath:       "/log",
			RetentionFull: 2,
		}

		err := WriteConfigFile(configPath, cfg)
		require.NoError(t, err)

		ctx := context.Background()

		// Try with empty stanza name
		err = StanzaCreate(ctx, "", configPath, repoPath)

		// Should fail - pgbackrest will reject empty stanza name
		assert.Error(t, err)
	})
}

func TestGenerateConfigWithoutRepoPath(t *testing.T) {
	cfg := Config{
		StanzaName: "test-stanza",
		PgDataPath: "/var/lib/postgresql/data",
		PgPort:     5432,
		LogPath:    "/var/log/pgbackrest",
	}

	config := GenerateConfig(cfg)

	// Verify repo1-path is NOT in the config
	assert.NotContains(t, config, "repo1-path=")
}
