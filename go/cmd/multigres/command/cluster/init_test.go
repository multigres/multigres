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
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestInitCommandLocalBackup(t *testing.T) {
	// Create temporary directory with short path to avoid Unix socket path length issues
	tmpDir := filepath.Join("/tmp", "mt-test-local")
	configPath := filepath.Join(tmpDir, "config")

	// Clean up before and after test
	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	// Create root command and add init command
	rootCmd := &cobra.Command{Use: "test"}
	// Add config-path flag to root command (normally done by viperutil)
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")

	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddInitCommand(clusterCmd)

	// Set args for local backup (default)
	rootCmd.SetArgs([]string{"cluster", "init", "--config-path", configPath})

	// Execute command
	err := rootCmd.Execute()
	require.NoError(t, err)

	// Verify config file was created
	configFile := filepath.Join(configPath, "multigres.yaml")
	require.FileExists(t, configFile)

	// Read and parse config
	data, err := os.ReadFile(configFile)
	require.NoError(t, err)

	var config MultigresConfig
	err = yaml.Unmarshal(data, &config)
	require.NoError(t, err)

	// Verify provisioner
	assert.Equal(t, "local", config.Provisioner)

	// Extract backup config
	provConfig := config.ProvisionerConfig
	require.NotNil(t, provConfig)

	backupConfig, ok := provConfig["backup"].(map[string]any)
	require.True(t, ok, "backup config should exist")

	// Verify backup type is local
	assert.Equal(t, "local", backupConfig["type"])

	// Verify local backup config with path
	localConfig, ok := backupConfig["local"].(map[string]any)
	require.True(t, ok, "local backup config should exist")
	path, ok := localConfig["path"].(string)
	require.True(t, ok, "path should be a string")
	assert.Contains(t, path, "data/backups")
}

func TestInitCommandS3BackupURL(t *testing.T) {
	// Skip if no AWS credentials (test will attempt S3 validation)
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Skip("Skipping S3 test - AWS credentials not set")
	}

	// Create temporary directory
	tmpDir := filepath.Join("/tmp", "mt-test-s3-url")
	configPath := filepath.Join(tmpDir, "config")

	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")

	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddInitCommand(clusterCmd)

	// Use test bucket from environment or skip
	testBucket := os.Getenv("TEST_S3_BUCKET")
	if testBucket == "" {
		t.Skip("Skipping S3 test - TEST_S3_BUCKET not set")
	}

	rootCmd.SetArgs([]string{
		"cluster", "init",
		"--config-path", configPath,
		"--backup-url", "s3://" + testBucket + "/test-prefix/",
		"--region", "us-east-1",
	})

	err := rootCmd.Execute()
	require.NoError(t, err)

	// Verify config file
	configFile := filepath.Join(configPath, "multigres.yaml")
	require.FileExists(t, configFile)

	data, err := os.ReadFile(configFile)
	require.NoError(t, err)

	var config MultigresConfig
	err = yaml.Unmarshal(data, &config)
	require.NoError(t, err)

	backupConfig, ok := config.ProvisionerConfig["backup"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "s3", backupConfig["type"])
	assert.Equal(t, testBucket, backupConfig["s3-bucket"])
	assert.Equal(t, "us-east-1", backupConfig["s3-region"])

	// Verify timestamped prefix was generated
	prefix, ok := backupConfig["s3-key-prefix"].(string)
	require.True(t, ok)
	assert.Contains(t, prefix, "test-prefix/")
	assert.Regexp(t, `test-prefix/\d{8}-\d{6}/`, prefix)
}

func TestInitCommandS3BackupMissingRegion(t *testing.T) {
	// Test error when backup-url provided but region is missing
	tmpDir := filepath.Join("/tmp", "mt-test-s3-no-region")
	configPath := filepath.Join(tmpDir, "config")

	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")

	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddInitCommand(clusterCmd)

	// Set args for S3 backup without region
	rootCmd.SetArgs([]string{
		"cluster", "init",
		"--config-path", configPath,
		"--backup-url", "s3://test-bucket/prefix/",
	})

	// Execute command - should fail without region
	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--region required")
}

func TestInitCommandS3BackupInvalidBucketName(t *testing.T) {
	// Test error with invalid bucket name in URL
	tmpDir := filepath.Join("/tmp", "mt-test-s3-invalid")
	configPath := filepath.Join(tmpDir, "config")

	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")

	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddInitCommand(clusterCmd)

	// Set args with invalid bucket name (uppercase not allowed)
	rootCmd.SetArgs([]string{
		"cluster", "init",
		"--config-path", configPath,
		"--backup-url", "s3://Invalid-Bucket/prefix/",
		"--region", "us-east-1",
	})

	// Execute command - should fail with bucket validation error
	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket name must contain only lowercase")
}
