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

package local

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/tools/semver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// waitForServiceReady waits for a service to become ready by checking appropriate endpoints
func (p *localProvisioner) waitForServiceReady(serviceName string, host string, servicePorts map[string]int, timeout time.Duration) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return fmt.Errorf("%s did not become ready within %v", serviceName, timeout)
		case <-ticker.C:
			// First check TCP connectivity on all advertised ports
			allPortsReady := true
			for _, port := range servicePorts {
				address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				conn, err := net.DialTimeout("tcp", address, 2*time.Second)
				if err != nil {
					allPortsReady = false
					break // This port not ready yet
				}
				conn.Close()
			}
			if !allPortsReady {
				continue // Not all ports ready yet
			}
			// For services with HTTP endpoints, check debug/config endpoint
			if err := p.checkMultigresServiceHealth(serviceName, host, servicePorts); err != nil {
				continue // HTTP endpoint not ready yet
			}
			return nil // Service is ready
		}
	}
}

// checkMultigresServiceHealth checks health for all supported service port types
func (p *localProvisioner) checkMultigresServiceHealth(serviceName string, host string, servicePorts map[string]int) error {
	// Iterate over service ports and run health checks for supported types
	for portType, port := range servicePorts {
		switch portType {
		case "http_port":
			// Run HTTP health check
			httpAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
			if err := p.checkDebugConfigEndpoint(httpAddress); err != nil {
				return err
			}
		case "grpc_port":
			// Run gRPC health check for pgctld
			if serviceName == "pgctld" {
				grpcAddress := net.JoinHostPort(host, fmt.Sprintf("%d", port))
				if err := p.checkPgctldGrpcHealth(grpcAddress); err != nil {
					return err
				}
			}
			// Future: Add other gRPC services
		default:
			// No health check implemented for this port type, skip
			continue
		}
	}
	return nil
}

// checkDebugConfigEndpoint checks if the debug/config endpoint returns 200 OK
func (p *localProvisioner) checkDebugConfigEndpoint(address string) error {
	url := fmt.Sprintf("http://%s/live", address)
	client := &http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("failed to reach debug endpoint: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("debug endpoint returned status %d", resp.StatusCode)
	}
	return nil
}

// checkPgctldGrpcHealth checks if pgctld gRPC server is healthy by calling Status
func (p *localProvisioner) checkPgctldGrpcHealth(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)
	_, err = client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("pgctld gRPC status call failed: %w", err)
	}

	return nil
}

// findBinary finds a binary by name, checking PATH first, then optional configured path
func (p *localProvisioner) findBinary(name string, serviceConfig map[string]any) (string, error) {
	// First try to find in PATH
	if binaryPath, err := exec.LookPath(name); err == nil {
		return binaryPath, nil
	}

	// Then try configured path if provided
	if pathConfig, ok := serviceConfig["path"].(string); ok && pathConfig != "" {
		// Check if it's an absolute path or relative path
		var fullPath string
		if filepath.IsAbs(pathConfig) {
			fullPath = pathConfig
		} else {
			// Make it relative to current directory
			fullPath = filepath.Join(".", pathConfig)
		}

		// Check if the binary exists and is executable
		if info, err := os.Stat(fullPath); err == nil && !info.IsDir() {
			return fullPath, nil
		}
	}

	return "", fmt.Errorf("binary '%s' not found in PATH or configured path", name)
}

// checkEtcdVersion verifies that the etcd binary major version matches expected version
func (p *localProvisioner) checkEtcdVersion(binaryPath, expectedVersion string) error {
	// Run etcd --version to get version info
	cmd := exec.Command(binaryPath, "--version")
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get etcd version: %w", err)
	}

	// Parse version from output - etcd version output format varies
	versionStr := string(output)

	// Try to extract version number from various etcd output formats
	versionRegex := regexp.MustCompile(`(?:etcd\s+Version:\s*|^|\s+)v?(\d+\.\d+\.\d+)`)
	matches := versionRegex.FindStringSubmatch(versionStr)
	if len(matches) < 2 {
		// If we can't parse version, just warn and continue
		fmt.Printf("Warning: could not parse etcd version from output: %s\n", strings.TrimSpace(versionStr))
		return nil
	}

	actualVersion := "v" + matches[1] // ensure v prefix for semver
	expectedVersionWithV := "v" + strings.TrimPrefix(expectedVersion, "v")

	// Use servenv semver to compare major versions
	actualMajor := semver.Major(actualVersion)
	expectedMajor := semver.Major(expectedVersionWithV)

	if actualMajor != expectedMajor {
		return fmt.Errorf("etcd major version mismatch: expected %s.x.x, found %s",
			strings.TrimPrefix(expectedMajor, "v"), strings.TrimPrefix(actualVersion, "v"))
	}

	fmt.Printf("🔍 - etcd %s found — version compatible ✓\n",
		strings.TrimPrefix(actualVersion, "v"))
	return nil
}

// readServiceLogs reads the last few lines from a service's log file for debugging
func (p *localProvisioner) readServiceLogs(logFile string, lines int) string {
	if logFile == "" {
		return "No log file available"
	}

	// Check if log file exists
	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		return fmt.Sprintf("Log file not found: %s", logFile)
	}

	// Read the file
	data, err := os.ReadFile(logFile)
	if err != nil {
		return fmt.Sprintf("Failed to read log file %s: %v", logFile, err)
	}

	// Get the last N lines
	logLines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(logLines) == 0 {
		return "Log file is empty"
	}

	// Return last 'lines' lines or all lines if fewer exist
	start := max(len(logLines)-lines, 0)

	result := strings.Join(logLines[start:], "\n")
	if result == "" {
		return "Log file is empty"
	}

	return result
}

// validateProcessRunning checks if a process with the given PID is still running
func (p *localProvisioner) validateProcessRunning(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("invalid PID: %d", pid)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process with PID %d not found: %w", pid, err)
	}

	// Send signal 0 to check if process exists without actually sending a signal
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return fmt.Errorf("process with PID %d is not running: %w", pid, err)
	}

	return nil
}

// stopProcessByPID stops a process by its PID
func (p *localProvisioner) stopProcessByPID(pid int) error {
	// Check if process exists
	process, err := os.FindProcess(pid)
	if err != nil {
		// Process not found, assume already cleaned up
		fmt.Printf("Process %d not found, assuming already stopped\n", pid)
		return nil
	}

	// Send SIGTERM to gracefully stop the process
	if err := process.Signal(syscall.SIGTERM); err != nil {
		// Process might already be dead, check errno
		if err.Error() == "no such process" || err.Error() == "process already finished" {
			fmt.Printf("Process %d already stopped\n", pid)
			return nil
		}

		// If SIGTERM fails for other reasons, try SIGKILL
		if err := process.Kill(); err != nil {
			// If kill also fails and it's because process doesn't exist, that's ok
			if err.Error() == "no such process" || err.Error() == "process already finished" {
				fmt.Printf("Process %d already stopped\n", pid)
				return nil
			}
			return fmt.Errorf("failed to kill process %d: %w", pid, err)
		}
	}

	// Wait a bit for the process to exit
	time.Sleep(2 * time.Second)

	fmt.Printf("Process %d stopped successfully\n", pid)
	return nil
}

// checkPortConflict checks if a port is already in use by another process
func (p *localProvisioner) checkPortConflict(port int, serviceName, portName string) error {
	if port <= 0 {
		return nil // Skip invalid ports
	}

	address := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		// Port is not in use, this is good
		return nil
	}
	conn.Close()

	// Port is in use by some process
	return fmt.Errorf("cannot start %s: port %d (%s) is already in use by another process. "+
		"There is no way to do a clean start. Please kill the process using port %d or change the configuration",
		serviceName, port, portName, port)
}

// getExpectedPortsForDbService returns expected ports for a DB-scoped service (per cell)
func (p *localProvisioner) getExpectedPortsForDbService(serviceName, cell string) map[string]int {
	ports := make(map[string]int)

	cellConfig, err := p.getCellServiceConfig(cell, serviceName)
	if err != nil {
		return ports
	}

	switch serviceName {
	case "multigateway":
		if httpPort, ok := cellConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case "multipooler":
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := cellConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case "multiorch":
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := cellConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	}

	return ports
}

// getExpectedPortsForService returns the expected ports for a service based on its configuration
func (p *localProvisioner) getExpectedPortsForService(serviceName string) map[string]int {
	serviceConfig := p.getServiceConfig(serviceName)
	ports := make(map[string]int)

	switch serviceName {
	case "multigateway":
		if httpPort, ok := serviceConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case "multipooler":
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := serviceConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case "multiorch":
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := serviceConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case "multiadmin":
		if httpPort, ok := serviceConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case "etcd":
		if port, ok := serviceConfig["port"].(int); ok {
			ports["tcp"] = port
		}
	}

	return ports
}

// findRunningDbService finds a running service by service name within a specific database and cell
func (p *localProvisioner) findRunningDbService(serviceName, databaseName, cell string) (*LocalProvisionedService, error) {
	services, err := p.loadDbProvisionedServices(databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to load service states for database %s: %w", databaseName, err)
	}

	for _, service := range services {
		if service.Service == serviceName {
			// Check if the service matches the cell
			if serviceCell, ok := service.Metadata["cell"].(string); ok && serviceCell == cell {
				// Check if the service is actually still running
				if service.PID > 0 {
					if err := p.validateProcessRunning(service.PID); err == nil {
						return service, nil
					}
				}
			}
		}
	}

	// Check for port conflicts with other processes using cell-specific config
	expectedPorts := p.getExpectedPortsForDbService(serviceName, cell)
	for portName, port := range expectedPorts {
		if err := p.checkPortConflict(port, serviceName, portName); err != nil {
			return nil, err
		}
	}

	return nil, nil // No running service found
}

// findRunningEtcdService finds a running etcd service
func (p *localProvisioner) findRunningEtcdService() (*LocalProvisionedService, error) {
	services, err := p.loadEtcdServices()
	if err != nil {
		return nil, fmt.Errorf("failed to load etcd service states: %w", err)
	}

	for _, service := range services {
		if service.Service == "etcd" {
			// Check if the service is actually still running
			if service.PID > 0 {
				if err := p.validateProcessRunning(service.PID); err == nil {
					return service, nil
				}
			}
		}
	}

	// Check for port conflicts with other processes
	expectedPorts := p.getExpectedPortsForService("etcd")
	for portName, port := range expectedPorts {
		if err := p.checkPortConflict(port, "etcd", portName); err != nil {
			return nil, err
		}
	}

	return nil, nil // No running etcd service found
}

// findRunningService finds a running service by service name (for global services like multiadmin)
func (p *localProvisioner) findRunningService(serviceName string) (*LocalProvisionedService, error) {
	// Load global services (e.g., multiadmin, etcd)
	services, err := p.loadGlobalServices()
	if err != nil {
		return nil, fmt.Errorf("failed to load global service states: %w", err)
	}

	for _, service := range services {
		if service.Service == serviceName {
			// Check if the service is actually still running
			if service.PID > 0 {
				if err := p.validateProcessRunning(service.PID); err == nil {
					return service, nil
				}
			}
		}
	}

	// Check for port conflicts with other processes
	expectedPorts := p.getExpectedPortsForService(serviceName)
	for portName, port := range expectedPorts {
		if err := p.checkPortConflict(port, serviceName, portName); err != nil {
			return nil, err
		}
	}

	return nil, nil // No running service found
}
