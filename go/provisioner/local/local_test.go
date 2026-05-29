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

package local

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateUnixSocketPathLength(t *testing.T) {
	provisioner := &localProvisioner{}

	tests := []struct {
		name        string
		rootDir     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "short path should pass",
			rootDir:     "/tmp/mt",
			expectError: false,
		},
		{
			name:        "medium path should pass",
			rootDir:     "/home/user/multigres",
			expectError: false,
		},
		{
			name:        "very long path should fail",
			rootDir:     "/Users/very/long/path/that/will/definitely/exceed/the/unix/socket/path/limit/for/postgresql/sockets",
			expectError: true,
			errorMsg:    "unix socket path would exceed system limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &LocalProvisionerConfig{
				RootWorkingDir: tt.rootDir,
			}

			err := provisioner.validateUnixSocketPathLength(config)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}

				// For path length errors, verify the error message contains helpful guidance
				if strings.Contains(tt.errorMsg, "exceed system limit") {
					assert.Contains(t, err.Error(), "To fix this issue:")
					assert.Contains(t, err.Error(), "Initialize multigres from a directory with a shorter path")
					assert.Contains(t, err.Error(), "Provide config-path to multigres")
					assert.Contains(t, err.Error(), "Better:  multigres cluster init --config-path /tmp/mt/")
					assert.Contains(t, err.Error(), "This will generate socket paths like:")
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateUnixSocketPathLengthWithWorkingDirectory(t *testing.T) {
	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp/", "socket")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Change to the subdirectory
	originalDir, err := os.Getwd()
	require.NoError(t, err)
	defer func() {
		_ = os.Chdir(originalDir)
	}()

	require.NoError(t, os.Chdir(tempDir))

	provisioner := &localProvisioner{}
	config := &LocalProvisionerConfig{
		RootWorkingDir: "./relative_path", // This should be converted to absolute
	}

	err = provisioner.validateUnixSocketPathLength(config)
	require.NoError(t, err)
}

func TestCheckPgBackrestVersion_Parse(t *testing.T) {
	p := &localProvisioner{}
	tests := []struct {
		name    string
		out     string
		wantErr string
	}{
		{"meets minimum", "pgBackRest 2.57.0\n", ""},
		{"newer patch", "pgBackRest 2.57.3\n", ""},
		{"newer minor", "pgBackRest 2.60.1\n", ""},
		{"too old", "pgBackRest 2.55.1\n", "too old"},
		{"way too old", "pgBackRest 1.99.99\n", "too old"},
		{"unparsable", "garbage\n", "could not parse"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := p.checkPgBackrestVersionFromOutput(tc.out)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestCheckPostgresVersion_Parse(t *testing.T) {
	p := &localProvisioner{}
	tests := []struct {
		name    string
		out     string
		wantErr string
	}{
		{"17.0", "postgres (PostgreSQL) 17.0\n", ""},
		{"17.2 homebrew", "postgres (PostgreSQL) 17.2 (Homebrew)\n", ""},
		{"14.20", "postgres (PostgreSQL) 14.20\n", "unsupported"},
		{"18 beta", "postgres (PostgreSQL) 18beta1\n", "unsupported"},
		{"16.5", "postgres (PostgreSQL) 16.5\n", "unsupported"},
		{"unparsable", "garbage\n", "could not parse"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := p.checkPostgresVersionFromOutput(tc.out)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// writeFakeBinary writes a tiny shell script to dir/name that prints stdout and exits 0,
// or exits nonzero when exitNonZero is true. Returns the binary path.
func writeFakeBinary(t *testing.T, dir, name, stdout string, exitNonZero bool) string {
	t.Helper()
	exitLine := ""
	if exitNonZero {
		exitLine = "exit 1\n"
	}
	script := "#!/bin/sh\nprintf %s " + shellQuote(stdout) + "\n" + exitLine
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func TestCheckPgBackrestVersion_ExecsBinary(t *testing.T) {
	p := &localProvisioner{}
	dir := t.TempDir()

	t.Run("good version", func(t *testing.T) {
		path := writeFakeBinary(t, dir, "pgbackrest_good", "pgBackRest 2.57.0\n", false)
		require.NoError(t, p.checkPgBackrestVersion(path))
	})

	t.Run("too old", func(t *testing.T) {
		path := writeFakeBinary(t, dir, "pgbackrest_old", "pgBackRest 2.55.1\n", false)
		err := p.checkPgBackrestVersion(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too old")
	})

	t.Run("binary exits nonzero", func(t *testing.T) {
		path := writeFakeBinary(t, dir, "pgbackrest_fail", "", true)
		err := p.checkPgBackrestVersion(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get pgbackrest version")
	})
}

func TestCheckPostgresVersion_ExecsBinary(t *testing.T) {
	p := &localProvisioner{}
	dir := t.TempDir()

	t.Run("good version", func(t *testing.T) {
		path := writeFakeBinary(t, dir, "postgres_good", "postgres (PostgreSQL) 17.2 (Homebrew)\n", false)
		require.NoError(t, p.checkPostgresVersion(path))
	})

	t.Run("wrong major", func(t *testing.T) {
		path := writeFakeBinary(t, dir, "postgres_old", "postgres (PostgreSQL) 14.20\n", false)
		err := p.checkPostgresVersion(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported")
	})

	t.Run("binary exits nonzero", func(t *testing.T) {
		path := writeFakeBinary(t, dir, "postgres_fail", "", true)
		err := p.checkPostgresVersion(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get postgres version")
	})
}

func TestValidateSystemBinaries(t *testing.T) {
	p := &localProvisioner{}

	t.Run("missing binaries reported", func(t *testing.T) {
		// Point PATH at an empty temp dir so every required binary is missing.
		dir := t.TempDir()
		t.Setenv("PATH", dir)
		err := p.validateSystemBinaries()
		require.Error(t, err)
		// All required binaries should be flagged.
		for _, b := range []string{"etcd", "pg_ctl", "postgres", "pg_isready", "pgbackrest"} {
			assert.Contains(t, err.Error(), b)
		}
	})

	t.Run("all present succeeds", func(t *testing.T) {
		dir := t.TempDir()
		// Provide a no-op binary for each required tool.
		for _, b := range []string{"etcd", "pg_ctl", "postgres", "pg_isready", "pgbackrest"} {
			writeFakeBinary(t, dir, b, "", false)
		}
		t.Setenv("PATH", dir)
		require.NoError(t, p.validateSystemBinaries())
	})
}

func TestCheckRequiredBinaryVersions(t *testing.T) {
	const (
		goodPgbackrest = "pgBackRest 2.57.0\n"
		oldPgbackrest  = "pgBackRest 2.55.1\n"
		goodPostgres   = "postgres (PostgreSQL) 17.2 (Homebrew)\n"
		oldPostgres    = "postgres (PostgreSQL) 14.20\n"
	)

	tests := []struct {
		name          string
		omit          map[string]bool   // binaries to omit from PATH
		stdouts       map[string]string // binary name -> --version stdout (defaults to goodX)
		exitNonZero   map[string]bool   // binary name -> shim exits nonzero
		wantErrSubstr string
	}{
		{
			name: "all good",
		},
		{
			name:          "pgbackrest missing from PATH",
			omit:          map[string]bool{"pgbackrest": true},
			wantErrSubstr: "pgbackrest lookup failed",
		},
		{
			name:          "postgres missing from PATH",
			omit:          map[string]bool{"postgres": true},
			wantErrSubstr: "postgres lookup failed",
		},
		{
			name:          "pgbackrest too old",
			stdouts:       map[string]string{"pgbackrest": oldPgbackrest},
			wantErrSubstr: "too old",
		},
		{
			name:          "postgres wrong major",
			stdouts:       map[string]string{"postgres": oldPostgres},
			wantErrSubstr: "unsupported",
		},
		{
			name:          "pgbackrest --version errors",
			exitNonZero:   map[string]bool{"pgbackrest": true},
			wantErrSubstr: "failed to get pgbackrest version",
		},
		{
			name:          "postgres --version errors",
			exitNonZero:   map[string]bool{"postgres": true},
			wantErrSubstr: "failed to get postgres version",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stdouts := map[string]string{
				"pgbackrest": goodPgbackrest,
				"postgres":   goodPostgres,
			}
			maps.Copy(stdouts, tc.stdouts)
			for _, b := range []string{"pgbackrest", "postgres"} {
				if tc.omit[b] {
					continue
				}
				writeFakeBinary(t, dir, b, stdouts[b], tc.exitNonZero[b])
			}
			t.Setenv("PATH", dir)

			p := &localProvisioner{}
			err := p.checkRequiredBinaryVersions()
			if tc.wantErrSubstr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErrSubstr)
		})
	}
}
