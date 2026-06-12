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
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/provisioner"
	s3tools "github.com/multigres/multigres/go/tools/s3"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// initCmd holds the init command configuration
type initCmd struct {
	provisioner viperutil.Value[string]
	backupPath  viperutil.Value[string]
	backupURL   viperutil.Value[string]
	region      viperutil.Value[string]
}

// getConfigPaths returns the list of config paths.
func getConfigPaths(cmd *cobra.Command) ([]string, error) {
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return nil, fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		return nil, errors.New("no config paths specified")
	}

	return configPaths, nil
}

// buildConfigFromFlags creates a MultigresConfig based on command flags
func (icmd *initCmd) buildConfigFromFlags(cmd *cobra.Command, configPaths []string) (*MultigresConfig, error) {
	provisionerName := icmd.provisioner.Get()

	// Parse and validate backup configuration
	backupConfig, err := icmd.buildBackupConfig(configPaths)
	if err != nil {
		return nil, err
	}

	// Get the provisioner instance
	p, err := provisioner.GetProvisioner(provisionerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get provisioner '%s': %w", provisionerName, err)
	}

	defaultConfig := p.DefaultConfig(configPaths, backupConfig)

	return &MultigresConfig{
		Provisioner:       provisionerName,
		ProvisionerConfig: defaultConfig,
	}, nil
}

// buildBackupConfig reads backup flags and builds config map
func (icmd *initCmd) buildBackupConfig(configPaths []string) (map[string]string, error) {
	backupURL := icmd.backupURL.Get()

	// If no backup URL, use local backups
	if backupURL == "" {
		config := map[string]string{"type": "local"}
		backupPath := icmd.backupPath.Get()
		if backupPath != "" {
			config["path"] = backupPath
		}
		return config, nil
	}

	// Parse S3 URL
	bucket, keyPrefix, err := parseBackupURL(backupURL)
	if err != nil {
		return nil, err
	}

	region := icmd.region.Get()

	// Validate region requirement
	if region == "" {
		return nil, fmt.Errorf("--region required for S3 backups\nExample: --backup-url=%s --region=us-east-1", backupURL)
	}

	// Generate timestamped prefix
	timestamp := time.Now().UTC().Format("20060102-150405")
	if keyPrefix == "" {
		keyPrefix = "backups/"
	}
	timestampedPrefix := keyPrefix + timestamp + "/"

	config := map[string]string{
		"type":                   "s3",
		"s3-bucket":              bucket,
		"s3-region":              region,
		"s3-key-prefix":          timestampedPrefix,
		"s3-use-env-credentials": "true",
	}

	fmt.Printf("Generated S3 backup prefix: %s\n", timestampedPrefix)

	// Validate credentials and S3 access
	creds, err := s3tools.ReadCredentialsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("AWS credentials not found in environment variables: %w\nSet these environment variables:\n  export AWS_ACCESS_KEY_ID=your-key-id\n  export AWS_SECRET_ACCESS_KEY=your-secret-key\n  export AWS_SESSION_TOKEN=your-token  # Optional", err)
	}
	accessKey := creds.AccessKey
	secretKey := creds.SecretKey
	sessionToken := creds.SessionToken

	fmt.Println("AWS credentials detected in environment ✓")
	fmt.Println("Validating S3 access...")

	//nolint:gocritic // CLI entry point, no parent context available
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = s3tools.ValidateAccess(ctx, s3tools.ValidationConfig{
		Bucket:       bucket,
		Region:       region,
		Endpoint:     "",
		KeyPrefix:    timestampedPrefix,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SessionToken: sessionToken,
	})
	if err != nil {
		return nil, fmt.Errorf("S3 validation failed: %w\n\nFix the issue and try again, or run 'multigres cluster check-backup-config' to diagnose", err)
	}

	fmt.Println("✓ S3 backup configuration validated successfully")

	return config, nil
}

// createConfigFile creates and writes the multigres configuration file
func createConfigFile(config *MultigresConfig, configPaths []string) (string, error) {
	// Validate the configuration before writing it
	if err := validateConfig(config); err != nil {
		return "", fmt.Errorf("configuration validation failed: %w", err)
	}

	// Marshal to YAML
	yamlData, err := yaml.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	// Determine config file path - use the first config path
	configDir := configPaths[0]
	configFile := filepath.Join(configDir, "multigres.yaml")

	// Check if config file already exists
	if _, err := os.Stat(configFile); err == nil {
		return "", fmt.Errorf("config file already exists: %s", configFile)
	}

	// Check if config directory exists
	if _, err := os.Stat(configDir); err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Creating config directory %s...\n", configDir)
			if err := os.MkdirAll(configDir, 0o755); err != nil {
				return "", fmt.Errorf("failed to create config directory %s: %w", configDir, err)
			}
		} else {
			return "", fmt.Errorf("failed to access config directory %s: %w", configDir, err)
		}
	}

	// Print the generated configuration
	fmt.Println("\nGenerated configuration:")
	fmt.Println("======================")
	fmt.Print(string(yamlData))

	// Write config file
	if err := os.WriteFile(configFile, yamlData, 0o644); err != nil {
		return "", fmt.Errorf("failed to write config file %s: %w", configFile, err)
	}

	return configFile, nil
}

// runInit handles the initialization of a multigres cluster configuration
func (icmd *initCmd) runInit(cmd *cobra.Command, args []string) error {
	configPaths, err := getConfigPaths(cmd)
	if err != nil {
		return err
	}

	fmt.Println("Initializing Multigres cluster configuration...")

	// Build configuration from flags
	config, err := icmd.buildConfigFromFlags(cmd, configPaths)
	if err != nil {
		return err
	}

	// Create and write config file
	configFile, err := createConfigFile(config, configPaths)
	if err != nil {
		return err
	}

	fmt.Printf("Created configuration file: %s\n", configFile)
	fmt.Println("Cluster configuration created successfully!")
	return nil
}

// validateConfig validates the configuration using the appropriate provisioner
func validateConfig(config *MultigresConfig) error {
	// Get the provisioner instance
	p, err := provisioner.GetProvisioner(config.Provisioner)
	if err != nil {
		return fmt.Errorf("failed to get provisioner '%s': %w", config.Provisioner, err)
	}

	// Validate the provisioner-specific configuration
	if err := p.ValidateConfig(config.ProvisionerConfig); err != nil {
		return fmt.Errorf("provisioner validation failed: %w", err)
	}

	return nil
}

// AddInitCommand adds the init subcommand to the cluster command
func AddInitCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()

	icmd := &initCmd{
		provisioner: viperutil.Configure(reg, "provisioner", viperutil.Options[string]{
			Default:  "local",
			FlagName: "provisioner",
			Dynamic:  false,
		}),
		backupPath: viperutil.Configure(reg, "backup-path", viperutil.Options[string]{
			Default:  "",
			FlagName: "backup-path",
			Dynamic:  false,
		}),
		backupURL: viperutil.Configure(reg, "backup-url", viperutil.Options[string]{
			Default:  "",
			FlagName: "backup-url",
			Dynamic:  false,
		}),
		region: viperutil.Configure(reg, "region", viperutil.Options[string]{
			Default:  "",
			FlagName: "region",
			Dynamic:  false,
		}),
	}

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a local cluster configuration",
		Long: `Initialize a new Multigres cluster configuration for local development.

Currently, only the 'local' provisioner is supported. This creates a multi-cell
cluster configuration on a single machine that can be started with 'multigres cluster up'.

The cluster can be configured with either local filesystem backups or S3-compatible
backups (including AWS S3, s3mock for testing, etc.).

S3 Backup Examples:
  # AWS S3
  multigres cluster init --backup-url=s3://my-bucket/backups/ --region=us-east-1`,
		RunE: icmd.runInit,
	}

	cmd.Flags().String("provisioner", icmd.provisioner.Default(), "Provisioner to use (only 'local' is supported)")
	cmd.Flags().String("backup-path", icmd.backupPath.Default(), "Path for local backups (defaults to {configDir}/data/backups)")
	cmd.Flags().String("backup-url", icmd.backupURL.Default(), "S3 backup URL (format: s3://bucket/prefix)")
	cmd.Flags().String("region", icmd.region.Default(), "AWS region (required for S3 backups)")

	viperutil.BindFlags(cmd.Flags(), icmd.provisioner, icmd.backupPath,
		icmd.backupURL, icmd.region)

	clusterCmd.AddCommand(cmd)
}
