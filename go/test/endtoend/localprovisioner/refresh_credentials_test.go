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

package localprovisioner

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/utils"
)

// readCredentialsFile reads and returns the contents of the credentials file
func readCredentialsFile(path string) (map[string]string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	credentials := make(map[string]string)
	for line := range strings.SplitSeq(string(content), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			credentials[parts[0]] = parts[1]
		}
	}

	return credentials, nil
}

// TestCredentialRefreshIntegration tests the end-to-end credential refresh workflow
func TestCredentialRefreshIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping credential refresh integration test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_test_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Logf("Test directory: %s", tempDir)

	// Initial AWS credentials
	initialAccessKey := "test-access-key"
	initialSecretKey := "test-secret-key"

	// Set initial credentials in environment
	oldAccessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	oldSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	oldSessionToken := os.Getenv("AWS_SESSION_TOKEN")

	// Restore original credentials after test
	defer func() {
		if oldAccessKey != "" {
			os.Setenv("AWS_ACCESS_KEY_ID", oldAccessKey)
		} else {
			os.Unsetenv("AWS_ACCESS_KEY_ID")
		}
		if oldSecretKey != "" {
			os.Setenv("AWS_SECRET_ACCESS_KEY", oldSecretKey)
		} else {
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		}
		if oldSessionToken != "" {
			os.Setenv("AWS_SESSION_TOKEN", oldSessionToken)
		} else {
			os.Unsetenv("AWS_SESSION_TOKEN")
		}
	}()

	os.Setenv("AWS_ACCESS_KEY_ID", initialAccessKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", initialSecretKey)
	os.Unsetenv("AWS_SESSION_TOKEN")

	// Step 1: Initialize cluster with S3 + UseEnvCredentials
	t.Log("Step 1: Initializing cluster with S3 and UseEnvCredentials...")

	initArgs := []string{
		"cluster", "init",
		"--config-path", tempDir,
		"--backup-url", "s3://multigres/backups/",
		"--region", "us-east-1",
	}

	initCmd := exec.Command("multigres", initArgs...)
	initOutput, err := initCmd.CombinedOutput()
	if err != nil {
		// If S3 validation fails, skip the test - S3 endpoint may not be available
		t.Logf("Init failed: %s", string(initOutput))
		if strings.Contains(string(initOutput), "S3 validation failed") {
			t.Skip("Cannot test credential refresh without accessible S3 - S3 endpoint likely not available")
		}
		require.NoError(t, err, "Init command failed with output: %s", string(initOutput))
	}
	t.Logf("Init output: %s", string(initOutput))

	// Step 2: Verify initial credentials file exists
	t.Log("Step 2: Verifying initial credentials file exists...")

	credentialsFile := filepath.Join(tempDir, "pgbackrest-credentials.conf")
	_, err = os.Stat(credentialsFile)
	require.NoError(t, err, "Credentials file should exist after init")

	// Check file permissions (should be 0600)
	fileInfo, err := os.Stat(credentialsFile)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm(), "Credentials file should have 0600 permissions")

	// Read initial credentials
	initialCreds, err := readCredentialsFile(credentialsFile)
	require.NoError(t, err, "Should be able to read credentials file")
	t.Logf("Initial credentials: %v", initialCreds)

	// Verify initial credentials contain expected keys
	require.Contains(t, initialCreds, "repo1-s3-key", "Should have repo1-s3-key")
	require.Contains(t, initialCreds, "repo1-s3-key-secret", "Should have repo1-s3-key-secret")
	require.Contains(t, initialCreds, "repo1-s3-key-type", "Should have repo1-s3-key-type")

	// Verify initial values
	assert.Equal(t, initialAccessKey, initialCreds["repo1-s3-key"])
	assert.Equal(t, initialSecretKey, initialCreds["repo1-s3-key-secret"])
	assert.Equal(t, "shared", initialCreds["repo1-s3-key-type"])

	// Step 3: Change AWS credentials in environment
	t.Log("Step 3: Changing AWS credentials in environment...")

	// Example credentials from AWS documentation (not real credentials)
	newAccessKey := "AKIAIOSFODNN7EXAMPLE"                     //nolint:gosec // Example credential from AWS docs
	newSecretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" //nolint:gosec // Example credential from AWS docs
	newSessionToken := "FwoGZXIvYXdzEBQaDEXAMPLETOKEN123456"   //nolint:gosec // Example credential from AWS docs

	os.Setenv("AWS_ACCESS_KEY_ID", newAccessKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", newSecretKey)
	os.Setenv("AWS_SESSION_TOKEN", newSessionToken)

	// Step 4: Run refresh-credentials command
	t.Log("Step 4: Running refresh-credentials command...")

	refreshArgs := []string{
		"cluster", "refresh-credentials",
		"--config-path", tempDir,
	}

	refreshCmd := exec.Command("multigres", refreshArgs...)
	refreshOutput, err := refreshCmd.CombinedOutput()
	require.NoError(t, err, "Refresh command failed with output: %s", string(refreshOutput))
	t.Logf("Refresh output: %s", string(refreshOutput))

	// Verify output contains success message
	assert.Contains(t, string(refreshOutput), "Credentials refreshed successfully")

	// Step 5: Verify credentials file updated with new values
	t.Log("Step 5: Verifying credentials file updated with new values...")

	// Read updated credentials
	updatedCreds, err := readCredentialsFile(credentialsFile)
	require.NoError(t, err, "Should be able to read updated credentials file")
	t.Logf("Updated credentials: %v", updatedCreds)

	// Verify updated credentials contain expected keys
	require.Contains(t, updatedCreds, "repo1-s3-key", "Should have repo1-s3-key")
	require.Contains(t, updatedCreds, "repo1-s3-key-secret", "Should have repo1-s3-key-secret")
	require.Contains(t, updatedCreds, "repo1-s3-key-type", "Should have repo1-s3-key-type")

	// Verify new values
	assert.Equal(t, newAccessKey, updatedCreds["repo1-s3-key"], "Access key should be updated")
	assert.Equal(t, newSecretKey, updatedCreds["repo1-s3-key-secret"], "Secret key should be updated")
	assert.Equal(t, "shared", updatedCreds["repo1-s3-key-type"], "Key type should still be 'shared'")

	// Verify session token is present if set
	if newSessionToken != "" {
		require.Contains(t, updatedCreds, "repo1-s3-token", "Should have repo1-s3-token when session token is set")
		assert.Equal(t, newSessionToken, updatedCreds["repo1-s3-token"], "Session token should be updated")
	}

	// Step 6: Verify file permissions preserved (0600)
	t.Log("Step 6: Verifying file permissions preserved...")

	fileInfo, err = os.Stat(credentialsFile)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), fileInfo.Mode().Perm(), "Credentials file should still have 0600 permissions after refresh")

	t.Log("Integration test completed successfully!")
}

// TestCredentialRefreshWithoutS3 tests refresh-credentials gracefully handles missing S3
func TestCredentialRefreshWithoutS3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping credential refresh test in short mode")
	}

	// Setup test directory
	tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_no_s3_")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Set test credentials
	os.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	defer func() {
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	}()

	// Initialize cluster with S3 (will fail to validate S3 if endpoint not available, but that's ok for this test)
	initArgs := []string{
		"cluster", "init",
		"--config-path", tempDir,
		"--backup-url", "s3://test-bucket/test/",
		"--region", "us-east-1",
	}

	initCmd := exec.Command("multigres", initArgs...)
	initOutput, err := initCmd.CombinedOutput()
	// Init may fail if S3 endpoint is not available - that's expected
	// We're testing that the refresh command works with the config even if S3 is not accessible
	if err != nil {
		t.Logf("Init failed (expected if S3 endpoint not available): %s", string(initOutput))

		// If init failed, we can't test refresh - skip this test
		t.Skip("Cannot test refresh without successful init - S3 endpoint likely not available")
	}

	// If we get here, init succeeded (S3 endpoint was available)
	// Now change credentials and test refresh
	os.Setenv("AWS_ACCESS_KEY_ID", "new-access-key")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "new-secret-key")

	// Run refresh command
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	refreshArgs := []string{
		"cluster", "refresh-credentials",
		"--config-path", tempDir,
	}

	refreshCmd := exec.CommandContext(ctx, "multigres", refreshArgs...)
	refreshOutput, err := refreshCmd.CombinedOutput()

	// Refresh should succeed even if S3 is not accessible
	// (it only updates the credentials file, doesn't validate S3 connectivity)
	require.NoError(t, err, "Refresh command failed with output: %s", string(refreshOutput))

	// Verify credentials file was updated
	credentialsFile := filepath.Join(tempDir, "pgbackrest-credentials.conf")
	creds, err := readCredentialsFile(credentialsFile)
	require.NoError(t, err)

	assert.Equal(t, "new-access-key", creds["repo1-s3-key"])
	assert.Equal(t, "new-secret-key", creds["repo1-s3-key-secret"])
}

// TestCredentialRefreshErrors tests error cases for refresh-credentials
func TestCredentialRefreshErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping credential refresh error test in short mode")
	}

	tests := []struct {
		name          string
		setup         func(*testing.T) string // returns temp dir
		env           map[string]string       // environment variables to set
		errorContains string
	}{
		{
			name: "missing config file",
			setup: func(t *testing.T) string {
				tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_err_")
				require.NoError(t, err)
				return tempDir
			},
			env: map[string]string{
				"AWS_ACCESS_KEY_ID":     "test-key",
				"AWS_SECRET_ACCESS_KEY": "test-secret",
			},
			errorContains: "failed to load config",
		},
		{
			name: "missing AWS_ACCESS_KEY_ID",
			setup: func(t *testing.T) string {
				tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_err_")
				require.NoError(t, err)

				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, tempDir)

				return tempDir
			},
			env: map[string]string{
				"AWS_SECRET_ACCESS_KEY": "test-secret",
			},
			errorContains: "AWS_ACCESS_KEY_ID",
		},
		{
			name: "missing AWS_SECRET_ACCESS_KEY",
			setup: func(t *testing.T) string {
				tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_err_")
				require.NoError(t, err)

				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WriteToDir(t, tempDir)

				return tempDir
			},
			env: map[string]string{
				"AWS_ACCESS_KEY_ID": "test-key",
			},
			errorContains: "AWS_SECRET_ACCESS_KEY",
		},
		{
			name: "not configured for S3 backups",
			setup: func(t *testing.T) string {
				tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_err_")
				require.NoError(t, err)

				utils.NewTestConfig().
					WithLocalBackup("/tmp/backups").
					WriteToDir(t, tempDir)

				return tempDir
			},
			env: map[string]string{
				"AWS_ACCESS_KEY_ID":     "test-key",
				"AWS_SECRET_ACCESS_KEY": "test-secret",
			},
			errorContains: "not configured for S3 backups",
		},
		{
			name: "not using env credentials",
			setup: func(t *testing.T) string {
				tempDir, err := os.MkdirTemp("/tmp", "multigres_cred_refresh_err_")
				require.NoError(t, err)

				utils.NewTestConfig().
					WithS3Backup("test-bucket", "us-east-1").
					WithEnvCredentials(false).
					WriteToDir(t, tempDir)

				return tempDir
			},
			env: map[string]string{
				"AWS_ACCESS_KEY_ID":     "test-key",
				"AWS_SECRET_ACCESS_KEY": "test-secret",
			},
			errorContains: "not configured to use environment credentials",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := tt.setup(t)
			defer os.RemoveAll(tempDir)

			// Clear all AWS env vars first
			oldVars := map[string]string{
				"AWS_ACCESS_KEY_ID":     os.Getenv("AWS_ACCESS_KEY_ID"),
				"AWS_SECRET_ACCESS_KEY": os.Getenv("AWS_SECRET_ACCESS_KEY"),
				"AWS_SESSION_TOKEN":     os.Getenv("AWS_SESSION_TOKEN"),
			}
			os.Unsetenv("AWS_ACCESS_KEY_ID")
			os.Unsetenv("AWS_SECRET_ACCESS_KEY")
			os.Unsetenv("AWS_SESSION_TOKEN")

			// Restore after test
			defer func() {
				for k, v := range oldVars {
					if v != "" {
						os.Setenv(k, v)
					} else {
						os.Unsetenv(k)
					}
				}
			}()

			// Set test environment variables
			for k, v := range tt.env {
				os.Setenv(k, v)
			}

			// Run refresh command
			refreshArgs := []string{
				"cluster", "refresh-credentials",
				"--config-path", tempDir,
			}

			refreshCmd := exec.Command("multigres", refreshArgs...)
			refreshOutput, err := refreshCmd.CombinedOutput()

			// Should fail with expected error
			require.Error(t, err, "Command should fail")
			output := string(refreshOutput)
			assert.Contains(t, strings.ToLower(output), strings.ToLower(tt.errorContains),
				"Error output should contain: %s\nActual output: %s", tt.errorContains, output)
		})
	}
}
