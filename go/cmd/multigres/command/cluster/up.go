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
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// checkEtcdConnectivity checks if etcd is reachable at the given address
func checkEtcdConnectivity(address string) error {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to etcd at %s: %w", address, err)
	}
	defer conn.Close()
	return nil
}

// startEtcdContainer starts an etcd container using Docker
func startEtcdContainer(address string) error {
	fmt.Printf("Starting etcd container for address: %s\n", address)

	// Extract port from address (assuming format like "localhost:2379")
	parts := strings.Split(address, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid address format: %s (expected host:port)", address)
	}
	port := parts[1]

	// Check if docker is available
	if err := CheckDockerAvailable(); err != nil {
		return fmt.Errorf("docker not available: %w", err)
	}

	// Create Multigres network for better container orchestration
	if err := CreateMultigresNetwork(); err != nil {
		return fmt.Errorf("failed to create network: %w", err)
	}

	containerName := EtcdContainerName

	// Check if container already exists
	if IsContainerRunning(containerName) {
		fmt.Printf("etcd container '%s' is already running\n", containerName)
		return nil
	}

	// Remove any stopped container with the same name
	if err := RemoveContainer(containerName); err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}

	// Calculate peer port (client port + 1 for convention)
	clientPort := port
	peerPort := fmt.Sprintf("%d", 2380) // Keep peer port standard for now

	// Build Docker command with proper grouping labels
	args := []string{
		"run", "-d",
		"--name", containerName,
		"--network", NetworkName,
		"-p", fmt.Sprintf("%s:%s", clientPort, clientPort), // Map configured port to same port inside container
		"-p", fmt.Sprintf("%s:%s", peerPort, peerPort), // Peer port mapping
		"--restart", "unless-stopped",
		"--health-cmd", "etcdctl endpoint health",
		"--health-interval", "10s",
		"--health-timeout", "5s",
		"--health-retries", "3",
		// Multigres grouping labels
		"--label", "com.docker.compose.project=multigres",
		"--label", "com.docker.compose.service=etcd",
		"--label", "multigres.component=etcd",
		"--label", "multigres.stack=local",
	}

	// Add environment variables and volume
	args = append(args,
		"--env", "ALLOW_NONE_AUTHENTICATION=yes",
		"--env", fmt.Sprintf("ETCD_ADVERTISE_CLIENT_URLS=http://0.0.0.0:%s", clientPort),
		"--env", fmt.Sprintf("ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:%s", clientPort),
		"--env", fmt.Sprintf("ETCD_INITIAL_ADVERTISE_PEER_URLS=http://0.0.0.0:%s", peerPort),
		"--env", fmt.Sprintf("ETCD_LISTEN_PEER_URLS=http://0.0.0.0:%s", peerPort),
		"--env", fmt.Sprintf("ETCD_INITIAL_CLUSTER=default=http://0.0.0.0:%s", peerPort),
		"--env", "ETCD_NAME=default",
		"--env", "ETCD_DATA_DIR=/etcd-data",
		"--volume", fmt.Sprintf("%s:/etcd-data", EtcdDataVolume),
		"quay.io/coreos/etcd:v3.5.9",
	)

	etcdCmd := exec.Command("docker", args...)

	fmt.Printf("Starting etcd container (mapping host port %s to container port %s)\n", clientPort, clientPort)

	if output, err := etcdCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to start etcd container: %w\nOutput: %s", err, string(output))
	}

	fmt.Printf("etcd container started successfully on port %s\n", clientPort)

	// Wait for etcd to be ready with better feedback
	fmt.Print("Waiting for etcd to be ready")
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		if err := checkEtcdConnectivity(address); err == nil {
			fmt.Println(" ✓")
			return nil
		}
		fmt.Print(".")
		time.Sleep(1 * time.Second)
	}

	// If we get here, etcd isn't responding - show container logs for debugging
	fmt.Println("\nFailed to connect to etcd. Container logs:")
	logsCmd := exec.Command("docker", "logs", "--tail", "20", containerName)
	if logs, err := logsCmd.Output(); err == nil {
		fmt.Printf("%s\n", string(logs))
	}

	return fmt.Errorf("etcd container started but is not responding after %d seconds", maxRetries)
}

// runUp handles the cluster up command
func runUp(cmd *cobra.Command, args []string) error {
	servenv.FireRunHooks()
	fmt.Println("Starting Multigres cluster...")

	// Check if Docker is available early
	if err := CheckDockerAvailable(); err != nil {
		return fmt.Errorf("docker not available: %w", err)
	}

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

	// Check etcd connectivity
	etcdAddress := config.Topology.EtcdDefaultAddress
	fmt.Printf("Checking etcd connectivity at: %s\n", etcdAddress)

	if err := checkEtcdConnectivity(etcdAddress); err != nil {
		fmt.Printf("etcd not reachable: %v\n", err)
		fmt.Println("Starting etcd container...")

		if err := startEtcdContainer(etcdAddress); err != nil {
			return fmt.Errorf("failed to start etcd container: %w", err)
		}
	} else {
		fmt.Printf("etcd is already running at: %s ✓\n", etcdAddress)
	}

	fmt.Println("Multigres cluster started successfully!")
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
