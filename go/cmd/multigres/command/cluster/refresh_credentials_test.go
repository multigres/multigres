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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/test/utils"
)

func TestRefreshCredentials(t *testing.T) {
	tests := []struct {
		name           string
		setupConfig    func(t *testing.T, dir string)
		envVars        map[string]string
		wantErr        bool
		wantErrContain string
	}{
		{
			name: "refresh credentials with session token",
			setupConfig: func(t *testing.T, dir string) {
				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WithEndpoint("https://localhost:9443/").
					WriteToDir(t, dir)

				utils.WritePgBackrestConfig(t, dir, utils.PgBackrestOpts{
					AccessKey:    "ASIAOLDACCESSKEY",
					SecretKey:    "oldsecretkey",
					SessionToken: "oldsessiontoken",
					Bucket:       "test-bucket",
				})
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
			wantErr: false,
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

			// Set environment variables
			for k, v := range tt.envVars {
				t.Setenv(k, v)
			}

			// Create command with config-path pointing to temp dir
			cmd := &cobra.Command{}
			cmd.Flags().StringSlice("config-path", []string{tmpDir}, "config paths")

			// Run refresh credentials
			rcmd := &refreshCredentialsCmd{}
			err := rcmd.runRefreshCredentials(cmd, nil)

			// Check error
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.wantErrContain != "" && !strings.Contains(err.Error(), tt.wantErrContain) {
					t.Errorf("error = %v, want to contain %q", err, tt.wantErrContain)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify pgbackrest.conf files were updated in all pooler directories
			poolerDirs, err := filepath.Glob(filepath.Join(tmpDir, "data", "pooler_*"))
			if err != nil {
				t.Fatalf("failed to find pooler directories: %v", err)
			}

			if len(poolerDirs) == 0 {
				t.Fatal("no pooler directories found")
			}

			for _, poolerDir := range poolerDirs {
				pgbackrestConf := filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf")
				content, err := os.ReadFile(pgbackrestConf)
				if err != nil {
					t.Fatalf("failed to read pgbackrest.conf: %v", err)
				}

				contentStr := string(content)

				// Verify new credentials are present
				if accessKey := tt.envVars["AWS_ACCESS_KEY_ID"]; accessKey != "" {
					if !strings.Contains(contentStr, "repo1-s3-key="+accessKey) {
						t.Errorf("pgbackrest.conf missing new access key: %s", accessKey)
					}
				}

				if secretKey := tt.envVars["AWS_SECRET_ACCESS_KEY"]; secretKey != "" {
					if !strings.Contains(contentStr, "repo1-s3-key-secret="+secretKey) {
						t.Errorf("pgbackrest.conf missing new secret key")
					}
				}

				if sessionToken := tt.envVars["AWS_SESSION_TOKEN"]; sessionToken != "" {
					if !strings.Contains(contentStr, "repo1-s3-token="+sessionToken) {
						t.Errorf("pgbackrest.conf missing new session token")
					}
				}

				// Verify permissions are still restrictive (0600)
				info, err := os.Stat(pgbackrestConf)
				if err != nil {
					t.Fatalf("failed to stat pgbackrest.conf: %v", err)
				}

				if info.Mode().Perm() != 0o600 {
					t.Errorf("pgbackrest.conf permissions = %o, want 0600", info.Mode().Perm())
				}
			}
		})
	}
}

func TestMaskCredential(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "long credential",
			input: "AKIAIOSFODNN7EXAMPLE",
			want:  "AKIA************MPLE",
		},
		{
			name:  "short credential (8 chars)",
			input: "12345678",
			want:  "12345678",
		},
		{
			name:  "credential with 9 chars",
			input: "123456789",
			want:  "1234*6789",
		},
		{
			name:  "credential with 10 chars",
			input: "1234567890",
			want:  "1234**7890",
		},
		{
			name:  "very short credential (4 chars)",
			input: "1234",
			want:  "1234",
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := maskCredential(tt.input)
			if got != tt.want {
				t.Errorf("maskCredential(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
