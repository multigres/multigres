// Copyright 2025 The Multigres Authors.
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
	"fmt"
	"os"
	"path/filepath"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/provisioner"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// getAvailableTopoImplementations returns a list of registered topo implementations
func getAvailableTopoImplementations() []string {
	return topo.GetAvailableImplementations()
}

// validateConfigPaths validates that the provided config paths exist and are directories
func validateConfigPaths(cmd *cobra.Command) ([]string, error) {
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return nil, fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		return nil, fmt.Errorf("no config paths specified")
	}

	for _, configPath := range configPaths {
		absPath, err := filepath.Abs(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve config path %s: %w", configPath, err)
		}

		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("config path does not exist: %s", absPath)
		} else if err != nil {
			return nil, fmt.Errorf("failed to access config path %s: %w", absPath, err)
		}

		// Check if it's a directory
		if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
			return nil, fmt.Errorf("config path is not a directory: %s", absPath)
		}
	}

	return configPaths, nil
}

// buildConfigFromFlags creates a MultigresConfig based on command flags
func buildConfigFromFlags(cmd *cobra.Command) (*MultigresConfig, error) {
	// Get config paths to substitute in provisioner config
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return nil, fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		return nil, fmt.Errorf("no config paths specified")
	}

	// Get provisioner name from flags or use default
	provisionerName, _ := cmd.Flags().GetString("provisioner")
	if provisionerName == "" {
		provisionerName = "local" // default provisioner
	}

	// Create default configuration for the specified provisioner
	config, err := createDefaultConfig(provisionerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create default config: %w", err)
	}

	return config, nil
}

// createConfigFile creates and writes the multigres configuration file
func createConfigFile(cmd *cobra.Command, configPaths []string) (string, error) {
	// Build configuration from flags
	config, err := buildConfigFromFlags(cmd)
	if err != nil {
		return "", err
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

	// Print the generated configuration
	fmt.Println("\nGenerated configuration:")
	fmt.Println("======================")
	fmt.Print(string(yamlData))

	// Write config file
	if err := os.WriteFile(configFile, yamlData, 0644); err != nil {
		return "", fmt.Errorf("failed to write config file %s: %w", configFile, err)
	}

	return configFile, nil
}

// runInit handles the initialization of a multigres cluster configuration
func runInit(cmd *cobra.Command, args []string) error {
	// Validate config paths
	configPaths, err := validateConfigPaths(cmd)
	if err != nil {
		return err
	}

	fmt.Println("Initializing Multigres cluster configuration...")

	// Create config file
	configFile, err := createConfigFile(cmd, configPaths)
	if err != nil {
		return err
	}

	fmt.Printf("Created configuration file: %s\n", configFile)
	fmt.Println("Cluster configuration created successfully!")
	return nil
}

// createDefaultConfig creates a default configuration for the specified provisioner
func createDefaultConfig(provisionerName string) (*MultigresConfig, error) {
	// Get default config from the provisioner
	p, err := provisioner.GetProvisioner(provisionerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get provisioner '%s': %w", provisionerName, err)
	}

	defaultConfig := p.DefaultConfig()

	return &MultigresConfig{
		Provisioner:       provisionerName,
		ProvisionerConfig: defaultConfig,
	}, nil
}

var InitCommand = &cobra.Command{
	Use:   "init",
	Short: "Create a local cluster configuration",
	Long:  "Initialize a new local Multigres cluster configuration that can be used with 'multigres cluster up'.",
	RunE:  runInit,
}

func init() {
	InitCommand.Flags().String("provisioner", "local", "Provisioner to use (only 'local' is supported)")
}
