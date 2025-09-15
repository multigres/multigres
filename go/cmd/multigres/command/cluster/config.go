// Copyright 2025 The Supabase, Inc.
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

	"gopkg.in/yaml.v3"
)

// MultigresConfig represents the structure of the multigres configuration file
type MultigresConfig struct {
	Provisioner       string         `yaml:"provisioner"`
	ProvisionerConfig map[string]any `yaml:"provisioner-config,omitempty"`
}

// LoadConfig loads the multigres configuration from the specified paths
func LoadConfig(configPaths []string) (*MultigresConfig, string, error) {
	// Try to find the config file in the provided paths
	for _, configPath := range configPaths {
		configFile := filepath.Join(configPath, "multigres.yaml")
		if _, err := os.Stat(configFile); err == nil {
			data, err := os.ReadFile(configFile)
			if err != nil {
				return nil, "", fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}

			var config MultigresConfig
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
