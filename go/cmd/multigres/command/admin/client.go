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

package admin

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"

	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/provisioner/local"
	"github.com/multigres/multigres/go/tools/grpccommon"
)

// Conn wraps a gRPC connection to the multiadmin server.
type Conn struct {
	multiadminpb.MultiadminServiceClient
	conn *grpc.ClientConn
}

// Close closes the underlying gRPC connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// NewClient creates a connection to the multiadmin server.
func NewClient(cmd *cobra.Command) (*Conn, error) {
	addr, err := GetServerAddress(cmd)
	if err != nil {
		return nil, err
	}
	conn, err := grpccommon.NewClient(addr, grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to admin server at %s: %w", addr, err)
	}
	return &Conn{
		MultiadminServiceClient: multiadminpb.NewMultiadminServiceClient(conn),
		conn:                    conn,
	}, nil
}

// GetServerAddress resolves the admin server address from flags or config
func GetServerAddress(cmd *cobra.Command) (string, error) {
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
		return "", errors.New("either --admin-server flag or --config-path must be provided")
	}

	// Load config and extract multiadmin address
	adminServerFromConfig, err := getServerFromConfig(configPaths)
	if err != nil {
		return "", fmt.Errorf("failed to get admin server from config: %w", err)
	}

	if adminServerFromConfig == "" {
		return "", errors.New("either --admin-server flag or --config-path with multiadmin configuration must be provided")
	}

	return adminServerFromConfig, nil
}

// getServerFromConfig extracts the multiadmin server address from config
func getServerFromConfig(configPaths []string) (string, error) {
	localConfig, err := parseLocalConfig(configPaths)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("localhost:%d", localConfig.Multiadmin.GrpcPort), nil
}

// GetLocalConfig parses multigres.yaml and returns the typed local provisioner config.
// Returns an error if the provisioner is not "local".
func GetLocalConfig(configPaths []string) (*local.LocalProvisionerConfig, error) {
	return parseLocalConfig(configPaths)
}

// parseLocalConfig reads multigres.yaml from the given paths and returns the typed local config.
func parseLocalConfig(configPaths []string) (*local.LocalProvisionerConfig, error) {
	var configFile string
	for _, path := range configPaths {
		candidate := filepath.Join(path, "multigres.yaml")
		if _, err := os.Stat(candidate); err == nil {
			configFile = candidate
			break
		}
	}
	if configFile == "" {
		return nil, errors.New("multigres.yaml not found in any of the provided paths")
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configFile, err)
	}

	var raw struct {
		Provisioner       string         `yaml:"provisioner"`
		ProvisionerConfig map[string]any `yaml:"provisioner-config"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}
	if raw.Provisioner != "local" {
		return nil, fmt.Errorf("unsupported provisioner: %s", raw.Provisioner)
	}

	yamlData, err := yaml.Marshal(raw.ProvisionerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal provisioner config: %w", err)
	}

	var localConfig local.LocalProvisionerConfig
	if err := yaml.Unmarshal(yamlData, &localConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal local provisioner config: %w", err)
	}
	return &localConfig, nil
}
