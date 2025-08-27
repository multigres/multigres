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
)

const (
	// NetworkName is the name of the Docker network used by Multigres
	NetworkName = "multigres-stack"
	// EtcdContainerName is the name of the etcd container
	EtcdContainerName = "multigres-etcd"
	// EtcdDataVolume is the name of the etcd data volume
	EtcdDataVolume = "multigres-etcd-data"
)

// HasDocker checks if Docker is available in the PATH
func HasDocker() bool {
	_, err := exec.LookPath("docker")
	return err == nil
}

// CheckDockerAvailable checks if Docker is available and returns an error if not
func CheckDockerAvailable() error {
	if !HasDocker() {
		return fmt.Errorf("docker not found in PATH")
	}
	return nil
}

// CreateMultigresNetwork creates a Docker network for Multigres components
func CreateMultigresNetwork() error {
	// Check if network already exists
	checkCmd := exec.Command("docker", "network", "ls", "--filter", fmt.Sprintf("name=%s", NetworkName), "--format", "{{.Name}}")
	output, err := checkCmd.Output()
	if err == nil && strings.TrimSpace(string(output)) == NetworkName {
		fmt.Printf("Network '%s' already exists\n", NetworkName)
		return nil
	}

	// Create network
	createCmd := exec.Command("docker", "network", "create", NetworkName)
	if err := createCmd.Run(); err != nil {
		return fmt.Errorf("failed to create network %s: %w", NetworkName, err)
	}

	fmt.Printf("Created network: %s\n", NetworkName)
	return nil
}

// RemoveMultigresNetwork removes the Multigres Docker network
func RemoveMultigresNetwork() error {
	fmt.Printf("Removing network: %s\n", NetworkName)
	removeNetworkCmd := exec.Command("docker", "network", "rm", NetworkName)
	if err := removeNetworkCmd.Run(); err != nil {
		return fmt.Errorf("failed to remove network %s: %w", NetworkName, err)
	}
	return nil
}

// IsContainerRunning checks if a container with the given name is currently running
func IsContainerRunning(containerName string) bool {
	checkCmd := exec.Command("docker", "ps", "-q", "-f", fmt.Sprintf("name=%s", containerName))
	output, err := checkCmd.Output()
	return err == nil && len(strings.TrimSpace(string(output))) > 0
}

// StopContainer stops a running container by name
func StopContainer(containerName string) error {
	// First check if it's running
	if !IsContainerRunning(containerName) {
		fmt.Printf("Container '%s' is not running\n", containerName)
		return nil
	}

	// Get container ID
	checkCmd := exec.Command("docker", "ps", "-q", "-f", fmt.Sprintf("name=%s", containerName))
	output, err := checkCmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get container ID for %s: %w", containerName, err)
	}

	containerID := strings.TrimSpace(string(output))
	if containerID == "" {
		return fmt.Errorf("container %s not found", containerName)
	}

	// Stop the container
	fmt.Printf("Stopping container: %s\n", containerName)
	stopCmd := exec.Command("docker", "stop", containerID)
	if err := stopCmd.Run(); err != nil {
		return fmt.Errorf("failed to stop container %s: %w", containerName, err)
	}

	return nil
}

// RemoveContainer removes a container by name
func RemoveContainer(containerName string) error {
	fmt.Printf("Removing container: %s\n", containerName)
	removeCmd := exec.Command("docker", "rm", "-f", containerName)
	if err := removeCmd.Run(); err != nil {
		return fmt.Errorf("failed to remove container %s: %w", containerName, err)
	}
	return nil
}

// RemoveVolume removes a named volume
func RemoveVolume(volumeName string) error {
	fmt.Printf("Removing volume: %s\n", volumeName)
	removeVolumeCmd := exec.Command("docker", "volume", "rm", volumeName)
	if err := removeVolumeCmd.Run(); err != nil {
		return fmt.Errorf("failed to remove volume %s: %w", volumeName, err)
	}
	return nil
}

// GetMultigresContainers returns a list of container IDs for containers with the multigres project label
func GetMultigresContainers() ([]string, error) {
	listCmd := exec.Command("docker", "ps", "-q", "--filter", "label=com.docker.compose.project=multigres")
	output, err := listCmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list multigres containers: %w", err)
	}

	containerIDs := strings.Fields(strings.TrimSpace(string(output)))
	return containerIDs, nil
}

// GetContainerName returns the name of a container by ID
func GetContainerName(containerID string) string {
	nameCmd := exec.Command("docker", "inspect", containerID, "--format", "{{.Name}}")
	nameOutput, err := nameCmd.Output()
	if err != nil {
		return containerID[:12] // fallback to short ID
	}
	return strings.TrimSpace(strings.TrimPrefix(string(nameOutput), "/"))
}
