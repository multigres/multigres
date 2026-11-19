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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/test/s3mock"
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

func TestInitCommandMinigresTopology(t *testing.T) {
	tmpDir := filepath.Join("/tmp", "mt-test-minigres-topology")
	configPath := filepath.Join(tmpDir, "config")

	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")

	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddInitCommand(clusterCmd)

	rootCmd.SetArgs([]string{"cluster", "init", "--config-path", configPath, "--topology", "minigres"})

	err := rootCmd.Execute()
	require.NoError(t, err)

	configFile := filepath.Join(configPath, "multigres.yaml")
	data, err := os.ReadFile(configFile)
	require.NoError(t, err)

	var config MultigresConfig
	require.NoError(t, yaml.Unmarshal(data, &config))

	cells, ok := config.ProvisionerConfig["cells"].(map[string]any)
	require.True(t, ok, "cells config should exist")
	require.Len(t, cells, 1, "minigres topology should generate a single cell")

	zone1, ok := cells["zone1"].(map[string]any)
	require.True(t, ok, "zone1 cell should exist")

	minigres, ok := zone1["minigres"].(map[string]any)
	require.True(t, ok, "zone1 should have a minigres service configured")
	assert.NotEmpty(t, minigres["path"])

	// The classic triad must not be present in a minigres cell.
	_, hasMultigateway := zone1["multigateway"]
	_, hasMultipooler := zone1["multipooler"]
	_, hasMultiorch := zone1["multiorch"]
	assert.False(t, hasMultigateway, "minigres cell should not configure multigateway")
	assert.False(t, hasMultipooler, "minigres cell should not configure multipooler")
	assert.False(t, hasMultiorch, "minigres cell should not configure multiorch")
}

func TestInitCommandInvalidTopology(t *testing.T) {
	tmpDir := filepath.Join("/tmp", "mt-test-invalid-topology")
	configPath := filepath.Join(tmpDir, "config")

	_ = os.RemoveAll(tmpDir)
	defer os.RemoveAll(tmpDir)

	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")

	clusterCmd := &cobra.Command{Use: "cluster"}
	rootCmd.AddCommand(clusterCmd)
	AddInitCommand(clusterCmd)

	rootCmd.SetArgs([]string{"cluster", "init", "--config-path", configPath, "--topology", "bogus"})

	err := rootCmd.Execute()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown --topology")
	assert.NoFileExists(t, filepath.Join(configPath, "multigres.yaml"))
}

// A self-hosted S3 service with a self-signed certificate: init validates the
// bucket with the certificate verified against --s3-ca-file, the same way the
// sidecar will, and records the endpoint and the CA in the config. Without the
// CA the certificate is not trusted, local endpoint or not, and verification
// goes off only with the explicit flag, which is recorded and warned about.
func TestInitCommandSelfHostedS3(t *testing.T) {
	s3Server, err := s3mock.NewServer(0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s3Server.Stop() })
	require.NoError(t, s3Server.CreateBucket("self-hosted"))
	caFile := filepath.Join(t.TempDir(), "s3-ca.pem")
	require.NoError(t, os.WriteFile(caFile, s3Server.CertPEM(), 0o600))
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "")

	initWith := func(t *testing.T, extra ...string) (map[string]any, error) {
		t.Helper()
		// A short directory: the cluster's socket paths hang off it.
		configPath, err := os.MkdirTemp("/tmp", "mt-init-s3-")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(configPath) })
		rootCmd := &cobra.Command{Use: "test", SilenceUsage: true, SilenceErrors: true}
		rootCmd.PersistentFlags().StringSlice("config-path", []string{}, "config paths")
		clusterCmd := &cobra.Command{Use: "cluster"}
		rootCmd.AddCommand(clusterCmd)
		AddInitCommand(clusterCmd)
		rootCmd.SetArgs(append([]string{
			"cluster", "init", "--config-path", configPath,
			"--backup-url", "s3://self-hosted/backups/", "--region", "us-east-1",
			"--s3-endpoint", s3Server.Endpoint(),
		}, extra...))
		if err := rootCmd.Execute(); err != nil {
			return nil, err
		}
		data, err := os.ReadFile(filepath.Join(configPath, "multigres.yaml"))
		require.NoError(t, err)
		var config MultigresConfig
		require.NoError(t, yaml.Unmarshal(data, &config))
		backupConfig, ok := config.ProvisionerConfig["backup"].(map[string]any)
		require.True(t, ok)
		s3Config, ok := backupConfig["s3"].(map[string]any)
		require.True(t, ok, "backup config: %v", backupConfig)
		return s3Config, nil
	}

	t.Run("a plaintext endpoint is refused", func(t *testing.T) {
		plain := "http://" + strings.TrimPrefix(s3Server.Endpoint(), "https://")
		for _, extra := range [][]string{{"--s3-endpoint", plain}, {"--s3-endpoint", plain, "--s3-skip-tls-verify"}} {
			_, err := initWith(t, extra...)
			require.ErrorContains(t, err, "--s3-endpoint")
			require.ErrorContains(t, err, "must be an https URL")
		}
	})

	t.Run("with the CA file", func(t *testing.T) {
		s3Config, err := initWith(t, "--s3-ca-file", caFile)
		require.NoError(t, err)
		assert.Equal(t, s3Server.Endpoint(), s3Config["endpoint"])
		assert.Equal(t, caFile, s3Config["ca-file"])
		assert.NotContains(t, s3Config, "skip-tls-verify", "verification stays on")
	})
	t.Run("without it the certificate is not trusted", func(t *testing.T) {
		_, err := initWith(t)
		require.ErrorContains(t, err, "certificate")
	})
	t.Run("verification off only when asked", func(t *testing.T) {
		s3Config, err := initWith(t, "--s3-skip-tls-verify")
		require.NoError(t, err)
		assert.Equal(t, true, s3Config["skip-tls-verify"])
		assert.NotContains(t, s3Config, "ca-file")
	})
	t.Run("a CA and verification off together are refused", func(t *testing.T) {
		_, err := initWith(t, "--s3-ca-file", caFile, "--s3-skip-tls-verify")
		require.ErrorContains(t, err, "cannot be combined")
	})
}
