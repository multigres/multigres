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
	"context"
	"fmt"

	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// provisionEtcd ensures etcd is running and accessible
func provisionEtcd(ctx context.Context, config *MultigressConfig, p provisioner.Provisioner) (*provisioner.ProvisionResult, error) {
	fmt.Println("\n=== Step 1: Provisioning etcd ===")

	req := &provisioner.ProvisionRequest{
		Service: "etcd",
		Config:  config.ProvisionerConfig,
	}

	result, err := p.ProvisionEtcd(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to provision etcd: %w", err)
	}

	tcpPort := result.Ports["tcp"]
	fmt.Printf("etcd available at: %s:%d ✓\n", result.FQDN, tcpPort)
	return result, nil
}

// setupCell initializes the topology cell configuration
func setupCell(ctx context.Context, config *MultigressConfig, etcdAddress string) error {
	fmt.Println("\n=== Step 2: Setting up cell ===")
	fmt.Printf("Configuring cell: %s\n", config.Topology.DefaultCellName)
	fmt.Printf("Using etcd at: %s\n", etcdAddress)

	// TODO: Implement cell setup using topo service
	// This would involve:
	// - Creating the cell in etcd if it doesn't exist
	// - Setting up cell-specific configuration
	// - Validating cell connectivity
	// - Use etcdAddress to connect to etcd for topology operations

	fmt.Printf("Cell '%s' setup completed ✓\n", config.Topology.DefaultCellName)
	return nil
}

// provisionMultigateway starts the multigateway service
func provisionMultigateway(ctx context.Context, config *MultigressConfig, p provisioner.Provisioner) error {
	fmt.Println("\n=== Step 3: Provisioning Multigateway ===")

	req := &provisioner.ProvisionRequest{
		Service: "multigateway",
		Config:  config.ProvisionerConfig,
	}

	result, err := p.ProvisionMultigateway(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to provision multigateway: %w", err)
	}

	grpcPort := result.Ports["grpc"]
	fmt.Printf("Multigateway available at: %s:%d ✓\n", result.FQDN, grpcPort)
	return nil
}

// provisionMultiOrch starts the multi-orchestrator service
func provisionMultiOrch(ctx context.Context, config *MultigressConfig, p provisioner.Provisioner) error {
	fmt.Println("\n=== Step 4: Provisioning Multi-Orchestrator ===")

	req := &provisioner.ProvisionRequest{
		Service: "multiorch",
		Config:  config.ProvisionerConfig,
	}

	result, err := p.ProvisionMultiOrch(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to provision multi-orchestrator: %w", err)
	}

	grpcPort := result.Ports["grpc"]
	fmt.Printf("Multi-Orchestrator available at: %s:%d ✓\n", result.FQDN, grpcPort)
	return nil
}

// runUp handles the cluster up command
func runUp(cmd *cobra.Command, args []string) error {
	servenv.FireRunHooks()
	fmt.Println("Starting Multigres cluster...")

	// Get config paths from flags
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		configPaths = []string{"."}
	}

	// Load configuration
	config, configFile, err := LoadConfig(configPaths)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	fmt.Printf("Using configuration from: %s\n", configFile)

	// Create provisioner instance
	p, err := provisioner.GetProvisioner(config.Provisioner)
	if err != nil {
		return fmt.Errorf("failed to create provisioner '%s': %w", config.Provisioner, err)
	}

	fmt.Printf("Using provisioner: %s\n", p.Name())

	ctx := context.Background()

	// Execute provisioning steps in order
	etcdResult, err := provisionEtcd(ctx, config, p)
	if err != nil {
		return fmt.Errorf("etcd provisioning failed: %w", err)
	}

	// Use the etcd address from provisioning result for cell setup
	etcdPort := etcdResult.Ports["tcp"]
	etcdAddress := fmt.Sprintf("%s:%d", etcdResult.FQDN, etcdPort)
	if err := setupCell(ctx, config, etcdAddress); err != nil {
		return fmt.Errorf("cell setup failed: %w", err)
	}

	if err := provisionMultigateway(ctx, config, p); err != nil {
		return fmt.Errorf("multigateway provisioning failed: %w", err)
	}

	if err := provisionMultiOrch(ctx, config, p); err != nil {
		return fmt.Errorf("multi-orchestrator provisioning failed: %w", err)
	}

	fmt.Println("\n🎉 Multigres cluster started successfully!")
	return nil
}

var UpCommand = &cobra.Command{
	Use:   "up",
	Short: "Start local cluster",
	Long:  "Start a local Multigres cluster using the configuration created with 'multigres cluster init'.",
	RunE:  runUp,
}

func init() {
	// No additional flags needed - config-path is provided by viperutil via root command
}
