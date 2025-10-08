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

package cluster

import (
	"context"
	"fmt"

	"github.com/multigres/multigres/go/provisioner"

	"github.com/spf13/cobra"
)

// teardownAllServices stops all provisioned services using the provisioner's Teardown method
func teardownAllServices(ctx context.Context, provisionerName string, configPaths []string, clean bool) error {
	// Create provisioner instance
	p, err := provisioner.GetProvisioner(provisionerName)
	if err != nil {
		return fmt.Errorf("failed to create provisioner '%s': %w", provisionerName, err)
	}

	// Let provisioner load its own configuration
	if err := p.LoadConfig(configPaths); err != nil {
		return fmt.Errorf("failed to load provisioner config: %w", err)
	}

	// Use the provisioner's teardown method
	if err := p.Teardown(ctx, clean); err != nil {
		return fmt.Errorf("failed to teardown services: %w", err)
	}

	return nil
}

// down handles the cluster down command
func down(cmd *cobra.Command, args []string) error {
	fmt.Println("Stopping Multigres cluster...")

	// Get the clean flag
	clean, err := cmd.Flags().GetBool("clean")
	if err != nil {
		return fmt.Errorf("failed to get clean flag: %w", err)
	}

	if clean {
		fmt.Println("Warning: clean mode, all data for this local cluster will be deleted")
	}

	// Get config paths from flags
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		configPaths = []string{"."}
	}

	// Load configuration to determine provisioner type
	config, configFile, err := LoadConfig(configPaths)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w. Run 'multigres cluster init' first", err)
	}

	fmt.Printf("Using configuration from: %s\n", configFile)
	fmt.Printf("Stopping cluster with provisioner: %s\n", config.Provisioner)

	ctx := context.Background()

	// Teardown all services using the provisioner
	if err := teardownAllServices(ctx, config.Provisioner, configPaths, clean); err != nil {
		return fmt.Errorf("failed to teardown services: %w", err)
	}

	fmt.Println("Multigres cluster stopped successfully!")
	return nil
}

// AddStopCommand adds the stop subcommand to the cluster command
func AddStopCommand(clusterCmd *cobra.Command) {
	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop local cluster",
		Long:  "Stop the local Multigres cluster. Use --clean to fully tear down all resources.",
		RunE:  down,
	}

	stopCmd.Flags().Bool("clean", false, "Fully tear down all cluster resources")

	clusterCmd.AddCommand(stopCmd)
}
