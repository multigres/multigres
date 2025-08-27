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
	"strings"

	"github.com/multigres/multigres/go/clustermetadata/topo"

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
			cmd.SilenceUsage = true
			return nil, fmt.Errorf("failed to resolve config path %s: %w", configPath, err)
		}

		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			cmd.SilenceUsage = true
			return nil, fmt.Errorf("config path does not exist: %s", absPath)
		} else if err != nil {
			cmd.SilenceUsage = true
			return nil, fmt.Errorf("failed to access config path %s: %w", absPath, err)
		}

		// Check if it's a directory
		if info, err := os.Stat(absPath); err == nil && !info.IsDir() {
			cmd.SilenceUsage = true
			return nil, fmt.Errorf("config path is not a directory: %s", absPath)
		}
	}

	return configPaths, nil
}

// buildConfigFromFlags creates a MultigressConfig based on command flags
func buildConfigFromFlags(cmd *cobra.Command) (*MultigressConfig, error) {
	// Start with default config
	config := DefaultConfig()

	// Override with flag values if provided
	if provisioner, _ := cmd.Flags().GetString("provisioner"); provisioner != "" {
		config.Provisioner = provisioner
	}

	if topoBackend, _ := cmd.Flags().GetString("topo-backend"); topoBackend != "" {
		config.Topology.Backend = topoBackend
	}

	if globalRootPath, _ := cmd.Flags().GetString("topo-global-root-path"); globalRootPath != "" {
		config.Topology.GlobalRootPath = globalRootPath
	}

	if defaultCellName, _ := cmd.Flags().GetString("topo-default-cell-name"); defaultCellName != "" {
		config.Topology.DefaultCellName = defaultCellName
	}

	if defaultCellRootPath, _ := cmd.Flags().GetString("topo-default-cell-root-path"); defaultCellRootPath != "" {
		config.Topology.DefaultCellRootPath = defaultCellRootPath
	}

	if etcdDefaultAddress, _ := cmd.Flags().GetString("topo-etcd-default-address"); etcdDefaultAddress != "" {
		config.Topology.EtcdDefaultAddress = etcdDefaultAddress
	}

	return config, nil
}

// validateConfig validates the configuration values
func validateConfig(cmd *cobra.Command, config *MultigressConfig) error {
	// Validate provisioner
	if config.Provisioner != "local" {
		cmd.SilenceUsage = true
		return fmt.Errorf("invalid provisioner: %s (only 'local' is supported)", config.Provisioner)
	}

	// Validate topo backend
	availableBackends := getAvailableTopoImplementations()
	validBackend := false
	for _, backend := range availableBackends {
		if config.Topology.Backend == backend {
			validBackend = true
			break
		}
	}
	if !validBackend {
		cmd.SilenceUsage = true
		return fmt.Errorf("invalid topo backend: %s (available: %v)", config.Topology.Backend, availableBackends)
	}

	return nil
}

// createConfigFile creates and writes the multigres configuration file
func createConfigFile(cmd *cobra.Command, configPaths []string) (string, error) {
	// Build configuration from flags
	config, err := buildConfigFromFlags(cmd)
	if err != nil {
		return "", err
	}

	// Validate configuration
	if err := validateConfig(cmd, config); err != nil {
		return "", err
	}

	// Marshal to YAML
	yamlData, err := yaml.Marshal(config)
	if err != nil {
		cmd.SilenceUsage = true
		return "", fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	// Determine config file path - use the first config path
	configDir := configPaths[0]
	configFile := filepath.Join(configDir, "multigres.yaml")

	// Check if config file already exists
	if _, err := os.Stat(configFile); err == nil {
		cmd.SilenceUsage = true
		return "", fmt.Errorf("config file already exists: %s", configFile)
	}

	// Write config file
	if err := os.WriteFile(configFile, yamlData, 0644); err != nil {
		cmd.SilenceUsage = true
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

var InitCommand = &cobra.Command{
	Use:   "init",
	Short: "Create a local cluster configuration",
	Long:  "Initialize a new local Multigres cluster configuration that can be used with 'multigres cluster up'.",
	RunE:  runInit,
}

func init() {
	// Add flags for configuration options
	availableBackends := getAvailableTopoImplementations()
	backendsStr := strings.Join(availableBackends, ", ")

	InitCommand.Flags().String("provisioner", "local", "Provisioner to use (only 'local' is supported)")
	InitCommand.Flags().String("topo-backend", "etcd2", fmt.Sprintf("Topology backend to use (available: %s)", backendsStr))
	InitCommand.Flags().String("topo-global-root-path", "/multigres/global", "Global topology root path")
	InitCommand.Flags().String("topo-default-cell-name", "zone1", "Default cell name")
	InitCommand.Flags().String("topo-default-cell-root-path", "/multigres/zone1", "Default cell root path")
	InitCommand.Flags().String("topo-etcd-default-address", "localhost:2379", "Default etcd address with port")
}
