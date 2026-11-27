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
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/provisioner/local"
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
AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN), writes them to the key directory of each
pooler's pgbackrestd sidecar, and makes every running sidecar reload them. It fails if
any sidecar could not be refreshed.

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

	// Only that each one is there: no part of a key, secret or token is
	// printed, whatever its length.
	fmt.Println("Found credentials in environment:")
	fmt.Println("  AWS_ACCESS_KEY_ID: set")
	fmt.Println("  AWS_SECRET_ACCESS_KEY: set")
	if creds.SessionToken != "" {
		fmt.Println("  AWS_SESSION_TOKEN: set")
	}
	fmt.Println()

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

	// Each pooler has its own pgbackrestd, which is the only process that holds
	// the keys. Give every one the new keys and report each outcome: a partial
	// refresh leaves some poolers archiving with keys that are about to expire.
	//
	// A pooler directory without a sidecar directory is a pooler that was not
	// refreshed, and counts as a failure. The exception is a directory the
	// cluster's configuration does not name: a leftover that no pooler of this
	// cluster uses.
	configured := configuredPoolerDirs(config.ProvisionerConfig)
	var reloaded, staged, failed []string
	for _, poolerDir := range poolerDirs {
		name := filepath.Base(poolerDir)
		outcome, err := local.RefreshSidecarS3Keys(cmd.Context(), poolerDir)
		switch {
		case errors.Is(err, local.ErrNoSidecar) && len(configured) > 0 && !configured[filepath.Clean(poolerDir)]:
			fmt.Printf("⚠ Skipping %s (not a pooler of this cluster's configuration, and it has no pgbackrestd sidecar directory)\n", name)
		case errors.Is(err, local.ErrNoSidecar):
			fmt.Printf("✗ %s: no pgbackrestd sidecar directory\n", name)
			failed = append(failed, name)
		case err != nil:
			fmt.Printf("✗ %s: %v\n", name, err)
			failed = append(failed, name)
		case outcome == local.S3KeysReloaded:
			fmt.Printf("✓ %s: running sidecar reloaded the new credentials\n", name)
			reloaded = append(reloaded, name)
		default:
			fmt.Printf("✓ %s: credentials written; the sidecar reads them when it is next configured\n", name)
			staged = append(staged, name)
		}
	}
	fmt.Println()

	if len(failed) > 0 {
		return fmt.Errorf("credentials were not refreshed for %d of %d poolers (%s); fix the cause and run the command again",
			len(failed), len(poolerDirs), strings.Join(failed, ", "))
	}
	if len(reloaded)+len(staged) == 0 {
		return fmt.Errorf("no pgbackrestd sidecar found beside the pooler directories in %s; nothing was refreshed", dataDir)
	}

	fmt.Printf("✓ Credentials refreshed successfully\n")
	if len(reloaded) > 0 {
		fmt.Println("pgBackRest commands started from now on use the new credentials. A command that is already running keeps the ones it started with.")
	}
	if len(reloaded) == 0 {
		fmt.Println("No sidecar is running with a configured repository; the new credentials take effect when the cluster starts.")
	}

	return nil
}

// configuredPoolerDirs returns the pooler directories the cluster's
// configuration names (cells.<cell>.pgctld.pooler-dir), cleaned. It is empty
// when the configuration names none.
func configuredPoolerDirs(provisionerConfig map[string]any) map[string]bool {
	dirs := map[string]bool{}
	cells, _ := provisionerConfig["cells"].(map[string]any)
	for _, cell := range cells {
		services, _ := cell.(map[string]any)
		pgctld, _ := services["pgctld"].(map[string]any)
		if dir, _ := pgctld["pooler-dir"].(string); dir != "" {
			dirs[filepath.Clean(dir)] = true
		}
	}
	return dirs
}
