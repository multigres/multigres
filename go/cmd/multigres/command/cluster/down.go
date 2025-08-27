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
	"os/exec"
	"strings"

	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// stopMultigresContainers stops all containers with the multigres project label
func stopMultigresContainers(clean bool) error {
	// Check if docker is available
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker not found in PATH: %w", err)
	}

	// Find all containers with the multigres project label
	listCmd := exec.Command("docker", "ps", "-q", "--filter", "label=com.docker.compose.project=multigres")
	output, err := listCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to list multigres containers: %w", err)
	}

	containerIDs := strings.Fields(strings.TrimSpace(string(output)))
	if len(containerIDs) == 0 {
		fmt.Println("No multigres containers are currently running")
		return nil
	}

	fmt.Printf("Found %d multigres container(s) to stop\n", len(containerIDs))

	// Stop the containers
	for _, containerID := range containerIDs {
		// Get container name for better user feedback
		nameCmd := exec.Command("docker", "inspect", containerID, "--format", "{{.Name}}")
		nameOutput, err := nameCmd.Output()
		containerName := strings.TrimSpace(strings.TrimPrefix(string(nameOutput), "/"))
		if err != nil {
			containerName = containerID[:12] // fallback to short ID
		}

		fmt.Printf("Stopping container: %s\n", containerName)
		stopCmd := exec.Command("docker", "stop", containerID)
		if err := stopCmd.Run(); err != nil {
			fmt.Printf("Warning: failed to stop container %s: %v\n", containerName, err)
		}
	}

	// If clean flag is set, also remove containers and clean up resources
	if clean {
		fmt.Println("Cleaning up containers and resources...")

		// Remove stopped containers
		for _, containerID := range containerIDs {
			removeCmd := exec.Command("docker", "rm", containerID)
			if err := removeCmd.Run(); err != nil {
				fmt.Printf("Warning: failed to remove container %s: %v\n", containerID[:12], err)
			}
		}

		// Remove the multigres network if it exists
		networkName := "multigres-stack"
		fmt.Printf("Removing network: %s\n", networkName)
		removeNetworkCmd := exec.Command("docker", "network", "rm", networkName)
		if err := removeNetworkCmd.Run(); err != nil {
			fmt.Printf("Warning: failed to remove network %s: %v\n", networkName, err)
		}

		// Optionally remove volumes (commented out for safety - user data)
		// fmt.Println("Removing named volumes...")
		// removeVolumeCmd := exec.Command("docker", "volume", "rm", "multigres-etcd-data")
		// if err := removeVolumeCmd.Run(); err != nil {
		//     fmt.Printf("Warning: failed to remove volume: %v\n", err)
		// }

		fmt.Println("Clean up completed (volumes preserved)")
	}

	return nil
}

// runDown handles the cluster down command
func runDown(cmd *cobra.Command, args []string) error {
	servenv.FireRunHooks()
	fmt.Println("Stopping Multigres cluster...")

	// Check if Docker is available early
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker not found in PATH: %w", err)
	}

	// Get the clean flag
	clean, err := cmd.Flags().GetBool("clean")
	if err != nil {
		return fmt.Errorf("failed to get clean flag: %w", err)
	}

	if clean {
		fmt.Println("Clean mode: will remove containers and networks")
	}

	// Get config paths from flags (for future use if needed)
	configPaths, err := cmd.Flags().GetStringSlice("config-path")
	if err != nil {
		return fmt.Errorf("failed to get config-path flag: %w", err)
	}
	if len(configPaths) == 0 {
		configPaths = []string{"."}
	}

	// Try to load configuration for context, but don't fail if it's not found
	config, configFile, err := LoadConfig(configPaths)
	if err == nil {
		fmt.Printf("Using configuration from: %s\n", configFile)
		fmt.Printf("Stopping cluster with etcd at: %s\n", config.Topology.EtcdDefaultAddress)
	} else {
		fmt.Println("No configuration found, stopping all multigres containers")
	}

	// Stop multigres containers
	if err := stopMultigresContainers(clean); err != nil {
		return fmt.Errorf("failed to stop containers: %w", err)
	}

	fmt.Println("Multigres cluster stopped successfully!")
	return nil
}

var DownCommand = &cobra.Command{
	Use:   "down",
	Short: "Stop local cluster",
	Long:  "Stop the local Multigres cluster. Use --clean to fully tear down all resources.",
	RunE:  runDown,
}

func init() {
	DownCommand.Flags().Bool("clean", false, "Fully tear down all cluster resources")
	// config-path is provided by viperutil via root command
}
