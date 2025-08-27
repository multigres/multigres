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

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// TopologyConfig holds the configuration for cluster topology
type TopologyConfig struct {
	Backend             string `yaml:"backend"`
	GlobalRootPath      string `yaml:"global-root-path"`
	DefaultCellName     string `yaml:"default-cell-name"`
	DefaultCellRootPath string `yaml:"default-cell-root-path"`
	EtcdDefaultAddress  string `yaml:"etcd-default-address"`
}

// MultigressConfig represents the structure of the multigres configuration file
type MultigressConfig struct {
	Provisioner string         `yaml:"provisioner"`
	Topology    TopologyConfig `yaml:"topology"`
}

// DefaultConfig returns a MultigressConfig with default values
func DefaultConfig() *MultigressConfig {
	return &MultigressConfig{
		Provisioner: "local",
		Topology: TopologyConfig{
			Backend:             "etcd2",
			GlobalRootPath:      "/multigres/global",
			DefaultCellName:     "zone1",
			DefaultCellRootPath: "/multigres/zone1",
			EtcdDefaultAddress:  "localhost:2379",
		},
	}
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

			return &config, configFile, nil
		}
	}

	return nil, "", fmt.Errorf("multigres.yaml not found in any of the provided paths: %v", configPaths)
}

// RegisterCommands registers all cluster subcommands with the given parent command
func RegisterCommands(parent *cobra.Command) {
	parent.AddCommand(InitCommand)
	parent.AddCommand(UpCommand)
	parent.AddCommand(DownCommand)
	parent.AddCommand(StatusCommand)
}
