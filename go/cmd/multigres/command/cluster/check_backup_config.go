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
	"strings"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/common/backup"
	s3tools "github.com/multigres/multigres/go/tools/s3"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// checkBackupConfigCmd holds the check-backup-config command configuration
type checkBackupConfigCmd struct {
	backupURL viperutil.Value[string]
	region    viperutil.Value[string]
}

// AddCheckBackupConfigCommand adds the check-backup-config subcommand
func AddCheckBackupConfigCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()

	ccmd := &checkBackupConfigCmd{
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
		Use:   "check-backup-config",
		Short: "Validate S3 backup configuration",
		Long: `Validates S3 backup configuration by checking credentials, bucket access, and permissions.

By default, reads configuration from multigres.yaml. Use flags to test configuration before
running 'cluster init'.

Examples:
  # Validate existing configuration
  multigres cluster check-backup-config

  # Test AWS S3 configuration before init
  multigres cluster check-backup-config \
    --backup-url=s3://my-bucket/backups/ \
    --region=us-east-1`,
		RunE: ccmd.runCheckBackupConfig,
	}

	cmd.Flags().String("backup-url", ccmd.backupURL.Default(), "S3 backup URL (format: s3://bucket/prefix)")
	cmd.Flags().String("region", ccmd.region.Default(), "AWS region")

	clusterCmd.AddCommand(cmd)
}

func (ccmd *checkBackupConfigCmd) runCheckBackupConfig(cmd *cobra.Command, args []string) error {
	fmt.Println("Checking S3 backup configuration...")
	fmt.Println()

	var bucket, region, endpoint, keyPrefix string
	var useConfig bool

	// Check if --backup-url flag was explicitly provided on command line
	backupURLFlagChanged := cmd.Flags().Changed("backup-url")

	if backupURLFlagChanged {
		// Use flags - parse backup URL
		backupURL := ccmd.backupURL.Get()
		var err error
		bucket, keyPrefix, err = parseBackupURL(backupURL)
		if err != nil {
			return err
		}

		region = ccmd.region.Get()

		// Validate region requirement
		if region == "" {
			return fmt.Errorf("--region required for S3 backups\nExample: --backup-url=%s --region=us-east-1", backupURL)
		}

		useConfig = false
	} else {
		// Load from config file
		useConfig = true
		configPaths, err := getConfigPaths(cmd)
		if err != nil {
			return err
		}

		config, _, err := LoadConfig(configPaths)
		if err != nil {
			return fmt.Errorf("failed to load config: %w\n\nUse flags to test configuration before init", err)
		}

		// Get backup config from provisioner-config
		provisionerConfig, ok := config.ProvisionerConfig["backup"].(map[string]any)
		if !ok {
			return errors.New("backup configuration not found in multigres.yaml")
		}

		backupType, _ := provisionerConfig["type"].(string)
		if backupType == "local" {
			fmt.Println("Backup type is 'local', skipping S3 validation.")
			return nil
		}

		if backupType != "s3" {
			return fmt.Errorf("unknown backup type: %s", backupType)
		}

		// Get S3 config
		s3Config, ok := provisionerConfig["s3"].(map[string]any)
		if !ok {
			return errors.New("S3 configuration not found in backup config")
		}

		// Extract S3 config
		bucket, _ = s3Config["bucket"].(string)
		region, _ = s3Config["region"].(string)
		endpoint, _ = s3Config["endpoint"].(string)
		keyPrefix, _ = s3Config["key-prefix"].(string)

		// Check if using env credentials (can be string or bool)
		useEnvCreds := false
		if val, ok := s3Config["use-env-credentials"].(bool); ok {
			useEnvCreds = val
		} else if val, ok := s3Config["use-env-credentials"].(string); ok {
			useEnvCreds = (val == "true")
		}

		if !useEnvCreds {
			fmt.Println("S3 backup not configured with use_env_credentials, skipping validation.")
			return nil
		}
	}

	// Read AWS credentials from environment
	creds, err := s3tools.ReadCredentialsFromEnv()
	if err != nil {
		fmt.Println("✗ AWS_ACCESS_KEY_ID: not set")
		fmt.Println("✗ AWS_SECRET_ACCESS_KEY: not set")
		fmt.Println()
		return errors.New("S3 configured with use_env_credentials but credentials not in environment\nSet AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY before running this command.")
	}

	// Check credentials
	fmt.Println("✓ AWS_ACCESS_KEY_ID: set")
	fmt.Println("✓ AWS_SECRET_ACCESS_KEY: set")

	// Validate S3 access
	ctx, cancel := context.WithTimeout(cmd.Context(), backup.ValidationTimeout)
	defer cancel()

	err = s3tools.ValidateAccess(ctx, s3tools.ValidationConfig{
		Bucket:       bucket,
		Region:       region,
		Endpoint:     endpoint,
		KeyPrefix:    keyPrefix,
		AccessKey:    creds.AccessKey,
		SecretKey:    creds.SecretKey,
		SessionToken: creds.SessionToken,
	})
	if err != nil {
		fmt.Printf("✗ S3 validation failed: %v\n", err)
		fmt.Println()

		// Provide helpful error messages based on error type
		errStr := err.Error()
		if strings.Contains(errStr, "NotFound") || strings.Contains(errStr, "NoSuchBucket") {
			fmt.Printf("Error: S3 bucket not found. Create it with:\n")
			if endpoint != "" {
				fmt.Printf("  aws --endpoint-url=%s s3 mb s3://%s --region %s\n", endpoint, bucket, region)
			} else {
				fmt.Printf("  aws s3 mb s3://%s --region %s\n", bucket, region)
			}
		} else if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "connection") {
			fmt.Println("Error: Cannot reach S3 endpoint. Check:")
			fmt.Println("  - Network connectivity")
			fmt.Println("  - Endpoint URL is correct")
			fmt.Println("  - Firewall rules allow access")
		} else if strings.Contains(errStr, "AccessDenied") || strings.Contains(errStr, "Forbidden") {
			fmt.Println("Error: Credentials lack write permissions to bucket.")
			fmt.Println("Check IAM policy allows s3:PutObject and s3:DeleteObject.")
		} else {
			fmt.Println("Error: S3 validation failed. Check your configuration and credentials.")
		}

		return err
	}

	fmt.Println("✓ Bucket exists and is accessible")
	fmt.Println("✓ Write access verified")
	fmt.Println("✓ Cleanup successful")
	fmt.Println()
	fmt.Println("S3 backup configuration is valid.")

	if useConfig {
		fmt.Println()
		fmt.Println("Configuration loaded from multigres.yaml")
	}

	return nil
}
