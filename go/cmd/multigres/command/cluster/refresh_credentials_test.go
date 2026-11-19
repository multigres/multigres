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

package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/test/utils"
)

func TestRefreshCredentials(t *testing.T) {
	tests := []struct {
		name           string
		setupConfig    func(t *testing.T, dir string)
		envVars        map[string]string
		wantErr        bool
		wantErrContain string
		// wantUntouched is a file, relative to the cluster directory, that a
		// failed refresh must leave byte for byte as it was.
		wantUntouched string
	}{
		{
			name: "refresh credentials with session token",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WithEndpoint("https://localhost:9443/").
					WriteToDir(t, dir)

				stoppedSidecar(t, dir, "pooler_001")
				stoppedSidecar(t, dir, "pooler_002")
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "ASIANEWACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "newsecretkey",
				"AWS_SESSION_TOKEN":     "newsessiontoken",
			},
			wantErr: false,
		},
		{
			name: "refresh credentials without session token",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, dir)

				stoppedSidecar(t, dir, "pooler_001")
				stoppedSidecar(t, dir, "pooler_002")
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIANEWACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "newsecretkey",
			},
			wantErr: false,
		},
		{
			// A cluster directory from before the sidecar holds the keys in the
			// pooler's own pgbackrest.conf, which nothing reads any more.
			name: "error when no pooler has a sidecar",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, dir)
				utils.WritePgBackrestConfig(t, dir, utils.PgBackrestOpts{
					AccessKey: "AKIAOLDACCESSKEY",
					SecretKey: "oldsecretkey",
					Bucket:    "test-bucket",
				})
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIANEWACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "newsecretkey",
			},
			wantErr:        true,
			wantErrContain: "not refreshed for 1 of 1 poolers (pooler_001)",
			wantUntouched:  filepath.Join("data", "pooler_001", "pgbackrest", "pgbackrest.conf"),
		},
		{
			// One pooler has its sidecar directory and one does not: the second
			// keeps its old keys, so the refresh as a whole failed.
			name: "error when one pooler has no sidecar directory",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, dir)
				stoppedSidecar(t, dir, "pooler_001")
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "data", "pooler_002"), 0o755))
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIANEWACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "newsecretkey",
			},
			wantErr:        true,
			wantErrContain: "not refreshed for 1 of 2 poolers (pooler_002)",
		},
		{
			name: "error when one sidecar cannot be refreshed",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, dir)
				stoppedSidecar(t, dir, "pooler_001")
				broken := stoppedSidecar(t, dir, "pooler_002")
				require.NoError(t, os.WriteFile(filepath.Join(broken, "owner.json"), []byte("{not json"), 0o600))
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIANEWACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "newsecretkey",
			},
			wantErr:        true,
			wantErrContain: "not refreshed for 1 of 2 poolers (pooler_002)",
		},
		{
			name: "error when no config file exists",
			setupConfig: func(t *testing.T, dir string) {
				// Don't create config file
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "secretkey",
			},
			wantErr:        true,
			wantErrContain: "multigres.yaml not found",
		},
		{
			name: "error when backup is not S3",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithLocalBackup("/tmp/backups").
					WriteToDir(t, dir)
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "secretkey",
			},
			wantErr:        true,
			wantErrContain: "not configured for S3 backups",
		},
		{
			name: "error when UseEnvCredentials is false",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WithEnvCredentials(false).
					WriteToDir(t, dir)
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID":     "AKIAACCESSKEY",
				"AWS_SECRET_ACCESS_KEY": "secretkey",
			},
			wantErr:        true,
			wantErrContain: "not configured to use environment credentials",
		},
		{
			name: "error when AWS_ACCESS_KEY_ID is not set",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, dir)
			},
			envVars: map[string]string{
				"AWS_SECRET_ACCESS_KEY": "secretkey",
			},
			wantErr:        true,
			wantErrContain: "AWS_ACCESS_KEY_ID",
		},
		{
			name: "error when AWS_SECRET_ACCESS_KEY is not set",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, dir)
			},
			envVars: map[string]string{
				"AWS_ACCESS_KEY_ID": "AKIAACCESSKEY",
			},
			wantErr:        true,
			wantErrContain: "AWS_SECRET_ACCESS_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory
			tmpDir := t.TempDir()

			// Setup config
			tt.setupConfig(t, tmpDir)

			// Start from an environment without any of the three, then set the
			// ones this case names.
			for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"} {
				t.Setenv(k, "")
				require.NoError(t, os.Unsetenv(k))
			}
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}
			var before []byte
			if tt.wantUntouched != "" {
				var err error
				before, err = os.ReadFile(filepath.Join(tmpDir, tt.wantUntouched))
				require.NoError(t, err)
			}

			// Create command with config-path pointing to temp dir
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			cmd.Flags().StringSlice("config-path", []string{tmpDir}, "config paths")

			// Run refresh credentials
			rcmd := &refreshCredentialsCmd{}
			err := rcmd.runRefreshCredentials(cmd, nil)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrContain != "" {
					require.ErrorContains(t, err, tt.wantErrContain)
				}
				if tt.wantUntouched != "" {
					after, err := os.ReadFile(filepath.Join(tmpDir, tt.wantUntouched))
					require.NoError(t, err)
					require.Equal(t, string(before), string(after))
				}
				return
			}
			require.NoError(t, err)

			// Every sidecar's key directory holds the new keys as one bundle.
			bundles, err := filepath.Glob(filepath.Join(tmpDir, "data", "pgbackrestd-pooler_*", "s3-keys", "credentials.json"))
			require.NoError(t, err)
			require.Len(t, bundles, 2)
			for _, bundle := range bundles {
				raw, err := os.ReadFile(bundle)
				require.NoError(t, err)
				var keys map[string]string
				require.NoError(t, json.Unmarshal(raw, &keys))
				require.Equal(t, map[string]string{
					"access_key_id":     tt.envVars["AWS_ACCESS_KEY_ID"],
					"secret_access_key": tt.envVars["AWS_SECRET_ACCESS_KEY"],
					"session_token":     tt.envVars["AWS_SESSION_TOKEN"],
				}, keys)
				info, err := os.Stat(bundle)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
			}
		})
	}
}

// stoppedSidecar lays out a pooler directory and the directory of its
// pgbackrestd sidecar, with no sidecar process, and returns the latter.
func stoppedSidecar(t *testing.T, clusterDir, pooler string) string {
	t.Helper()
	poolerDir := filepath.Join(clusterDir, "data", pooler)
	require.NoError(t, os.MkdirAll(poolerDir, 0o755))
	sidecarDir := local.NewPgBackRestdPaths(poolerDir).Dir
	require.NoError(t, os.MkdirAll(sidecarDir, 0o700))
	return sidecarDir
}

// A pooler directory the cluster's configuration does not name is a leftover:
// without a sidecar directory it is skipped. One the configuration names is a
// pooler of this cluster, and the same gap fails the refresh.
func TestRefreshCredentials_ConfiguredPoolersMustAllHaveASidecar(t *testing.T) {
	run := func(t *testing.T, named ...string) error {
		t.Helper()
		dir := t.TempDir()
		utils.NewTestConfig().WithS3Backup("test-bucket", "us-east-1").WriteToDir(t, dir)
		stoppedSidecar(t, dir, "pooler_001")
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "data", "pooler_old"), 0o755))

		configFile := filepath.Join(dir, "multigres.yaml")
		raw, err := os.ReadFile(configFile)
		require.NoError(t, err)
		var config map[string]any
		require.NoError(t, yaml.Unmarshal(raw, &config))
		cells := map[string]any{}
		for i, name := range named {
			cells[fmt.Sprintf("zone%d", i+1)] = map[string]any{
				"pgctld": map[string]any{"pooler-dir": filepath.Join(dir, "data", name)},
			}
		}
		config["provisioner-config"].(map[string]any)["cells"] = cells
		raw, err = yaml.Marshal(config)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(configFile, raw, 0o644))

		t.Setenv("AWS_ACCESS_KEY_ID", "AKIANEWACCESSKEY")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "newsecretkey")
		t.Setenv("AWS_SESSION_TOKEN", "")
		cmd := &cobra.Command{}
		cmd.SetContext(t.Context())
		cmd.Flags().StringSlice("config-path", []string{dir}, "config paths")
		return (&refreshCredentialsCmd{}).runRefreshCredentials(cmd, nil)
	}

	t.Run("a leftover directory is skipped", func(t *testing.T) {
		require.NoError(t, run(t, "pooler_001"))
	})
	t.Run("a configured pooler without a sidecar fails the refresh", func(t *testing.T) {
		require.ErrorContains(t, run(t, "pooler_001", "pooler_old"), "not refreshed for 1 of 2 poolers (pooler_old)")
	})
}

// Nothing of a key, a secret or a token reaches the terminal: not a short one
// in full, and not the ends of a long one.
func TestRefreshCredentials_PrintsNoCredentialMaterial(t *testing.T) {
	for name, creds := range map[string][3]string{
		"short": {"AKSHORT1", "s3cr3t", "tok3n"},
		"long":  {"AKQXZZZZQQQQXXXXYYJW", "Zq7pabcdefghijklmnopqrstuvwxyz0123456789Kx9v", "Yw3mabcdefghijklmnopqrstuvwxyz0123456789Jt5b"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			utils.NewTestConfig().WithS3Backup("test-bucket", "us-east-1").WriteToDir(t, dir)
			stoppedSidecar(t, dir, "pooler_001")
			t.Setenv("AWS_ACCESS_KEY_ID", creds[0])
			t.Setenv("AWS_SECRET_ACCESS_KEY", creds[1])
			t.Setenv("AWS_SESSION_TOKEN", creds[2])

			read, write, err := os.Pipe()
			require.NoError(t, err)
			stdout := os.Stdout
			os.Stdout = write
			cmd := &cobra.Command{}
			cmd.SetContext(t.Context())
			cmd.Flags().StringSlice("config-path", []string{dir}, "config paths")
			runErr := (&refreshCredentialsCmd{}).runRefreshCredentials(cmd, nil)
			os.Stdout = stdout
			require.NoError(t, write.Close())
			out, err := io.ReadAll(read)
			require.NoError(t, err)
			require.NoError(t, runErr)

			printed := string(out)
			require.Contains(t, printed, "AWS_SECRET_ACCESS_KEY: set")
			for _, secret := range creds {
				for _, part := range []string{secret, secret[:4], secret[len(secret)-4:]} {
					require.NotContains(t, printed, part, "the output shows part of a credential")
				}
			}
		})
	}
}
