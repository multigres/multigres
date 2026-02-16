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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/common/backup"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/tools/fileutil"
	s3tools "github.com/multigres/multigres/go/tools/s3"
)

// refreshCredentialsCmd holds the refresh-credentials command configuration
type refreshCredentialsCmd struct{}

// AddRefreshCredentialsCommand adds the refresh-credentials subcommand
func AddRefreshCredentialsCommand(clusterCmd *cobra.Command) {
	rcmd := &refreshCredentialsCmd{}

	cmd := &cobra.Command{
		Use:   "refresh-credentials",
		Short: "Refresh AWS credentials for S3 backups",
		Long: `Refreshes AWS credentials for S3 backups after obtaining new temporary credentials.

This command reads fresh credentials from environment variables (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) and updates the credentials in each pooler's
pgbackrest.conf file.

Run this command after refreshing your AWS credentials (e.g., via 'aws sso login') to
ensure backup operations continue to work with the new credentials.

Requirements:
  - Must be run from the cluster directory (where multigres.yaml exists)
  - Cluster must be configured for S3 backups with UseEnvCredentials=true
  - Required environment variables must be set:
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN (optional, but required for temporary credentials)

Example:
  # Refresh credentials after AWS SSO login
  export AWS_ACCESS_KEY_ID=<new-key>
  export AWS_SECRET_ACCESS_KEY=<new-secret>
  export AWS_SESSION_TOKEN=<new-token>
  multigres cluster refresh-credentials`,
		RunE: rcmd.runRefreshCredentials,
	}

	clusterCmd.AddCommand(cmd)
}

func (rcmd *refreshCredentialsCmd) runRefreshCredentials(cmd *cobra.Command, args []string) error {
	fmt.Println("Refreshing AWS credentials for S3 backups...")
	fmt.Println()

	// Get config paths
	configPaths, err := getConfigPaths(cmd)
	if err != nil {
		return err
	}

	// Load cluster config
	config, configFile, err := LoadConfig(configPaths)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	fmt.Printf("Loaded configuration from: %s\n", configFile)
	fmt.Println()

	// Get backup config from provisioner-config
	provisionerConfig, ok := config.ProvisionerConfig["backup"].(map[string]any)
	if !ok {
		return errors.New("backup configuration not found in multigres.yaml")
	}

	backupType, _ := provisionerConfig["type"].(string)
	if backupType != "s3" {
		return fmt.Errorf("cluster is not configured for S3 backups (backup type: %s)", backupType)
	}

	// Get S3 config
	s3Config, ok := provisionerConfig["s3"].(map[string]any)
	if !ok {
		return errors.New("S3 configuration not found in backup config")
	}

	// Check if using env credentials (can be string or bool)
	useEnvCreds := false
	if val, ok := s3Config["use-env-credentials"].(bool); ok {
		useEnvCreds = val
	} else if val, ok := s3Config["use-env-credentials"].(string); ok {
		useEnvCreds = (val == "true")
	}

	if !useEnvCreds {
		return errors.New("cluster is not configured to use environment credentials (s3-use-env-credentials: false)")
	}

	// Read AWS credentials from environment
	creds, err := s3tools.ReadCredentialsFromEnv()
	if err != nil {
		return err
	}

	fmt.Println("Found credentials in environment:")
	fmt.Printf("  AWS_ACCESS_KEY_ID: %s\n", maskCredential(creds.AccessKey))
	fmt.Printf("  AWS_SECRET_ACCESS_KEY: %s\n", maskCredential(creds.SecretKey))
	if creds.SessionToken != "" {
		fmt.Printf("  AWS_SESSION_TOKEN: %s\n", maskCredential(creds.SessionToken))
	}
	fmt.Println()

	// Create backup config to get credentials
	bucket, _ := s3Config["bucket"].(string)
	region, _ := s3Config["region"].(string)
	endpoint, _ := s3Config["endpoint"].(string)
	keyPrefix, _ := s3Config["key-prefix"].(string)

	backupConfig, err := backup.NewConfig(&clustermetadatapb.BackupLocation{
		Location: &clustermetadatapb.BackupLocation_S3{
			S3: &clustermetadatapb.S3Backup{
				Bucket:            bucket,
				Region:            region,
				Endpoint:          endpoint,
				KeyPrefix:         keyPrefix,
				UseEnvCredentials: true,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create backup config: %w", err)
	}

	// Get new credentials
	newCreds, err := backupConfig.PgBackRestCredentials()
	if err != nil {
		return fmt.Errorf("failed to get credentials: %w", err)
	}

	// Find all pooler directories
	clusterDir := filepath.Dir(configFile)
	dataDir := filepath.Join(clusterDir, "data")
	poolerDirs, err := filepath.Glob(filepath.Join(dataDir, "pooler_*"))
	if err != nil {
		return fmt.Errorf("failed to find pooler directories: %w", err)
	}

	if len(poolerDirs) == 0 {
		return fmt.Errorf("no pooler directories found in %s", dataDir)
	}

	// Update pgbackrest.conf in each pooler directory
	updatedFiles := []string{}
	for _, poolerDir := range poolerDirs {
		pgbackrestConf := filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf")

		// Read current config
		currentContent, err := os.ReadFile(pgbackrestConf)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("⚠ Skipping %s (does not exist)\n", pgbackrestConf)
				continue
			}
			return fmt.Errorf("failed to read %s: %w", pgbackrestConf, err)
		}

		// Update credentials in config
		updatedContent, err := backup.UpdateCredentialsInConfig(string(currentContent), newCreds)
		if err != nil {
			return fmt.Errorf("failed to update credentials in %s: %w", pgbackrestConf, err)
		}

		// Write back atomically
		if err := fileutil.AtomicWriteFile(pgbackrestConf, []byte(updatedContent), 0o600); err != nil {
			return fmt.Errorf("failed to write %s: %w", pgbackrestConf, err)
		}

		updatedFiles = append(updatedFiles, pgbackrestConf)
	}

	fmt.Printf("✓ Credentials refreshed successfully\n")
	for _, f := range updatedFiles {
		fmt.Printf("  Updated: %s\n", f)
	}
	fmt.Println()
	fmt.Println("AWS credentials have been updated. Backup operations will use the new credentials.")

	return nil
}

// maskCredential masks all but the first 4 and last 4 characters of a credential
func maskCredential(cred string) string {
	if len(cred) <= 8 {
		return cred
	}

	first4 := cred[:4]
	last4 := cred[len(cred)-4:]
	middle := strings.Repeat("*", len(cred)-8)

	return first4 + middle + last4
}
