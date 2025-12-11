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

package topo

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/multigres/multigres/go/provisioner/local"
)

// GetAdminServerAddress resolves the admin server address from flags or config
func GetAdminServerAddress(cmd *cobra.Command) (string, error) {
	// Check if admin-server flag is provided
	adminServer, _ := cmd.Flags().GetString("admin-server")
	if adminServer != "" {
		return adminServer, nil
	}

	// Fall back to config file
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return "", fmt.Errorf("failed to get config-path flag: %w", err)
	}

	if len(configPaths) == 0 {
		return "", fmt.Errorf("either --admin-server flag or --config-path must be provided")
	}

	// Load config and extract multiadmin address
	adminServerFromConfig, err := getAdminServerFromConfig(configPaths)
	if err != nil {
		return "", fmt.Errorf("failed to get admin server from config: %w", err)
	}

	if adminServerFromConfig == "" {
		return "", fmt.Errorf("either --admin-server flag or --config-path with multiadmin configuration must be provided")
	}

	return adminServerFromConfig, nil
}

// getAdminServerFromConfig extracts the multiadmin server address from config
func getAdminServerFromConfig(configPaths []string) (string, error) {
	// Find the config file
	var configFile string
	for _, path := range configPaths {
		candidate := filepath.Join(path, "multigres.yaml")
		if _, err := os.Stat(candidate); err == nil {
			configFile = candidate
			break
		}
	}

	if configFile == "" {
		return "", fmt.Errorf("multigres.yaml not found in any of the provided paths")
	}

	// Read the config file directly
	data, err := os.ReadFile(configFile)
	if err != nil {
		return "", fmt.Errorf("failed to read config file %s: %w", configFile, err)
	}

	// Parse the config structure
	var config struct {
		Provisioner       string         `yaml:"provisioner"`
		ProvisionerConfig map[string]any `yaml:"provisioner-config"`
	}

	if err := yaml.Unmarshal(data, &config); err != nil {
		return "", fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Extract multiadmin config for local provisioner
	if config.Provisioner == "local" {
		// Convert the map to YAML and then to typed config
		yamlData, err := yaml.Marshal(config.ProvisionerConfig)
		if err != nil {
			return "", fmt.Errorf("failed to marshal provisioner config: %w", err)
		}

		var localConfig local.LocalProvisionerConfig
		if err := yaml.Unmarshal(yamlData, &localConfig); err != nil {
			return "", fmt.Errorf("failed to unmarshal local provisioner config: %w", err)
		}

		// Build admin server address from config
		return fmt.Sprintf("localhost:%d", localConfig.Multiadmin.GrpcPort), nil
	}

	return "", fmt.Errorf("unsupported provisioner: %s", config.Provisioner)
}
