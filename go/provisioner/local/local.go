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

package local

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"time"

	"github.com/multigres/multigres/go/provisioner"
)

// LocalProvisioner implements the Provisioner interface for local Docker-based provisioning
type LocalProvisioner struct {
	config map[string]interface{}
}

// Name returns the name of this provisioner
func (p *LocalProvisioner) Name() string {
	return "local"
}

// DefaultConfig returns the default configuration for the local provisioner
func (p *LocalProvisioner) DefaultConfig() map[string]interface{} {
	return map[string]interface{}{
		"network": "multigres-stack",
		"labels": map[string]interface{}{
			"com.docker.compose.project": "multigres",
			"multigres.stack":            "local",
		},
		"runtime": "binary", // Options: "binary" or "docker"
		"etcd": map[string]interface{}{
			"runtime": "docker", // etcd always uses docker for local provisioner
			"image":   "quay.io/coreos/etcd:v3.5.9",
			"datadir": "/etcd-data",
			"volume":  "multigres-etcd-data",
		},
		"multigateway": map[string]interface{}{
			"runtime":   "binary", // Use binary by default for multigres components
			"binary":    "bin/multigateway",
			"image":     "multigres/multigateway:latest",
			"port":      15991,
			"grpc_port": 15992,
		},
		"multiorch": map[string]interface{}{
			"runtime":   "binary", // Use binary by default for multigres components
			"binary":    "bin/multiorch",
			"image":     "multigres/multiorch:latest",
			"port":      15999,
			"grpc_port": 16000,
		},
	}
}

// checkEtcdConnectivity checks if etcd is reachable at the given address
func checkEtcdConnectivity(address string) error {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to etcd at %s: %w", address, err)
	}
	defer conn.Close()
	return nil
}

// ProvisionEtcd provisions etcd using Docker containers
func (p *LocalProvisioner) ProvisionEtcd(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Get etcd config
	etcdConfig := p.getServiceConfig("etcd")

	// Extract port from default address (assuming format like "localhost:2379")
	// For now, we'll use default port 2379
	port := 2379
	address := fmt.Sprintf("localhost:%d", port)

	// Check if etcd is already running
	if err := checkEtcdConnectivity(address); err == nil {
		fmt.Printf("etcd is already running at: %s ✓\n", address)
		return &provisioner.ProvisionResult{
			FQDN: "localhost",
			Ports: map[string]int{
				"tcp": port,
			},
			Metadata: map[string]interface{}{
				"container_name":  EtcdContainerName,
				"runtime":         "docker",
				"already_running": true,
			},
		}, nil
	}

	// Check if Docker is available
	if err := CheckDockerAvailable(); err != nil {
		return nil, fmt.Errorf("docker not available: %w", err)
	}

	// Create Multigres network for better container orchestration
	if err := CreateMultigresNetwork(); err != nil {
		return nil, fmt.Errorf("failed to create network: %w", err)
	}

	containerName := EtcdContainerName

	// Check if container already exists
	if IsContainerRunning(containerName) {
		fmt.Printf("etcd container '%s' is already running\n", containerName)
		return &provisioner.ProvisionResult{
			FQDN: "localhost",
			Ports: map[string]int{
				"tcp": port,
			},
			Metadata: map[string]interface{}{
				"container_name":  containerName,
				"runtime":         "docker",
				"already_running": true,
			},
		}, nil
	}

	// Remove any stopped container with the same name
	if err := RemoveContainer(containerName); err != nil {
		return nil, fmt.Errorf("failed to remove container: %w", err)
	}

	// Get image from config
	image := "quay.io/coreos/etcd:v3.5.9" // default
	if img, ok := etcdConfig["image"].(string); ok {
		image = img
	}

	// Calculate peer port (client port + 1 for convention)
	clientPort := fmt.Sprintf("%d", port)
	peerPort := "2380" // Keep peer port standard

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
		image,
	)

	etcdCmd := exec.Command("docker", args...)

	fmt.Printf("Starting etcd container (mapping host port %s to container port %s)\n", clientPort, clientPort)

	if output, err := etcdCmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("failed to start etcd container: %w\nOutput: %s", err, string(output))
	}

	fmt.Printf("etcd container started successfully on port %s\n", clientPort)

	// Wait for etcd to be ready with better feedback
	fmt.Print("Waiting for etcd to be ready")
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		if err := checkEtcdConnectivity(address); err == nil {
			fmt.Println(" ✓")
			return &provisioner.ProvisionResult{
				FQDN: "localhost",
				Ports: map[string]int{
					"tcp": port,
				},
				Metadata: map[string]interface{}{
					"container_name": containerName,
					"runtime":        "docker",
					"newly_started":  true,
				},
			}, nil
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

	return nil, fmt.Errorf("etcd container started but is not responding after %d seconds", maxRetries)
}

// ProvisionMultigateway provisions multigateway using either binaries or Docker containers
func (p *LocalProvisioner) ProvisionMultigateway(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Get runtime preference from config
	multigatewayConfig := p.getServiceConfig("multigateway")
	runtime := p.getRuntime(multigatewayConfig)

	port := 15991
	if p, ok := multigatewayConfig["port"].(int); ok {
		port = p
	}

	// TODO: Implement actual multigateway startup (binary or docker based on config)
	fmt.Println("Note: Multigateway provisioning is not fully implemented yet")

	return &provisioner.ProvisionResult{
		FQDN: "localhost",
		Ports: map[string]int{
			"grpc": port,
		},
		Metadata: map[string]interface{}{
			"runtime": runtime,
			"status":  "placeholder",
		},
	}, nil
}

// ProvisionMultiOrch provisions multi-orchestrator using either binaries or Docker containers
func (p *LocalProvisioner) ProvisionMultiOrch(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Get runtime preference from config
	multiorchConfig := p.getServiceConfig("multiorch")
	runtime := p.getRuntime(multiorchConfig)

	port := 15999
	if p, ok := multiorchConfig["port"].(int); ok {
		port = p
	}

	// TODO: Implement actual multi-orchestrator startup (binary or docker based on config)
	fmt.Println("Note: Multi-Orchestrator provisioning is not fully implemented yet")

	return &provisioner.ProvisionResult{
		FQDN: "localhost",
		Ports: map[string]int{
			"grpc": port,
		},
		Metadata: map[string]interface{}{
			"runtime": runtime,
			"status":  "placeholder",
		},
	}, nil
}

// Deprovision removes/stops services
func (p *LocalProvisioner) Deprovision(ctx context.Context, req *provisioner.ProvisionRequest) error {
	// TODO: Move existing container stop logic here and add binary process stopping
	return fmt.Errorf("not yet implemented")
}

// ValidateConfig validates the local provisioner configuration
func (p *LocalProvisioner) ValidateConfig(config map[string]interface{}) error {
	// Validate network
	if network, ok := config["network"]; ok {
		if _, ok := network.(string); !ok {
			return fmt.Errorf("network must be a string")
		}
	}

	// Validate runtime options
	if runtime, ok := config["runtime"]; ok {
		runtimeStr, ok := runtime.(string)
		if !ok {
			return fmt.Errorf("runtime must be a string")
		}
		if runtimeStr != "binary" && runtimeStr != "docker" {
			return fmt.Errorf("runtime must be 'binary' or 'docker', got: %s", runtimeStr)
		}
	}

	return nil
}

// getServiceConfig gets the configuration for a specific service
func (p *LocalProvisioner) getServiceConfig(service string) map[string]interface{} {
	if serviceConfig, ok := p.config[service].(map[string]interface{}); ok {
		return serviceConfig
	}
	// Return default config for the service
	defaults := p.DefaultConfig()
	if serviceConfig, ok := defaults[service].(map[string]interface{}); ok {
		return serviceConfig
	}
	return map[string]interface{}{}
}

// getRuntime determines the runtime for a service (binary or docker)
func (p *LocalProvisioner) getRuntime(serviceConfig map[string]interface{}) string {
	// Check service-specific runtime first
	if runtime, ok := serviceConfig["runtime"].(string); ok {
		return runtime
	}
	// Fall back to global runtime
	if runtime, ok := p.config["runtime"].(string); ok {
		return runtime
	}
	// Default to binary
	return "binary"
}

// NewLocalProvisioner creates a new local provisioner instance
func NewLocalProvisioner() (provisioner.Provisioner, error) {
	p := &LocalProvisioner{
		config: map[string]interface{}{},
	}

	return p, nil
}

func init() {
	// Register the local provisioner
	provisioner.RegisterProvisioner("local", NewLocalProvisioner)
}
