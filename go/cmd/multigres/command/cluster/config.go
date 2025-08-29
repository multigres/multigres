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

	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/provisioner/local"

	"github.com/spf13/cobra"

	"gopkg.in/yaml.v3"
)

// MultigressConfig represents the structure of the multigres configuration file
type MultigressConfig struct {
	Provisioner       string                 `yaml:"provisioner"`
	ProvisionerConfig map[string]interface{} `yaml:"provisioner-config,omitempty"`
}

// LoadConfig loads the multigres configuration from the specified paths
func LoadConfig(configPaths []string) (*MultigressConfig, string, error) {
	// Try to find the config file in the provided paths
	for _, configPath := range configPaths {
		configFile := filepath.Join(configPath, "multigres.yaml")
		if _, err := os.Stat(configFile); err == nil {
			data, err := os.ReadFile(configFile)
			if err != nil {
				return nil, "", fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}

			var config MultigressConfig
			if err := yaml.Unmarshal(data, &config); err != nil {
				return nil, "", fmt.Errorf("failed to parse config file %s: %w", configFile, err)
			}

			// Validate that provisioner is specified
			if config.Provisioner == "" {
				return nil, "", fmt.Errorf("provisioner not specified in config file %s", configFile)
			}

			return &config, configFile, nil
		}
	}

	return nil, "", fmt.Errorf("multigres.yaml not found in any of the provided paths: %v", configPaths)
}

// CreateDefaultConfig creates a default configuration for the specified provisioner
func CreateDefaultConfig(provisionerName string) (*MultigressConfig, error) {
	// Get default config from the provisioner
	p, err := provisioner.GetProvisioner(provisionerName)
	if err != nil {
		return nil, fmt.Errorf("failed to get provisioner '%s': %w", provisionerName, err)
	}

	defaultConfig := p.DefaultConfig()

	return &MultigressConfig{
		Provisioner:       provisionerName,
		ProvisionerConfig: defaultConfig,
	}, nil
}

// GetTypedLocalConfig returns a typed local provisioner configuration if the provisioner is "local"
func (c *MultigressConfig) GetTypedLocalConfig() (*local.LocalProvisionerConfig, error) {
	if c.Provisioner != "local" {
		return nil, fmt.Errorf("provisioner is not 'local', got: %s", c.Provisioner)
	}

	// Convert map[string]interface{} to typed config via YAML marshaling
	yamlData, err := yaml.Marshal(c.ProvisionerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal provisioner config: %w", err)
	}

	var typedConfig local.LocalProvisionerConfig
	if err := yaml.Unmarshal(yamlData, &typedConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal provisioner config: %w", err)
	}

	return &typedConfig, nil
}

// RegisterSubCommands registers all cluster subcommands with the given parent command
func RegisterSubCommands(parent *cobra.Command) {
	parent.AddCommand(InitCommand)
	parent.AddCommand(UpCommand)
	parent.AddCommand(DownCommand)
	parent.AddCommand(StatusCommand)
}
