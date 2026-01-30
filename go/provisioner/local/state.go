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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/provisioner"
)

// LocalProvisionedService represents a service instance that has been provisioned
type LocalProvisionedService struct {
	ID         string         `json:"id"`                    // Unique instance ID
	Service    string         `json:"service"`               // Service name (etcd, multigateway, etc.)
	PID        int            `json:"pid,omitempty"`         // For binary processes
	BinaryPath string         `json:"binary-path,omitempty"` // Path to the binary
	DataDir    string         `json:"data-dir,omitempty"`    // Data directory
	LogFile    string         `json:"log-file,omitempty"`    // Path to log file
	Ports      map[string]int `json:"ports"`                 // Port mappings
	FQDN       string         `json:"fqdn"`                  // Hostname/FQDN
	Runtime    string         `json:"runtime"`               // "binary"
	StartedAt  time.Time      `json:"started-at"`            // When it was started
	Metadata   map[string]any `json:"metadata,omitempty"`    // Additional metadata
}

// getStateDir returns the path to the state directory
func (p *localProvisioner) getStateDir() string {
	return filepath.Join(p.getRootWorkingDir(), "state")
}

// getLogsDir returns the path to the logs directory
func (p *localProvisioner) getLogsDir() string {
	return filepath.Join(p.getRootWorkingDir(), "logs")
}

func (p *localProvisioner) getDataDir() string {
	return filepath.Join(p.getRootWorkingDir(), "data")
}

// createLogFile creates a log file path and ensures the directory exists
func (p *localProvisioner) createLogFile(serviceName, serviceID, databaseName string) (string, error) {
	logsDir := p.getLogsDir()
	var serviceLogDir string

	if databaseName != "" {
		// For database services: logs/dbs/dbname/servicename
		serviceLogDir = filepath.Join(logsDir, "dbs", databaseName, serviceName)
	} else {
		// For non-database services (like etcd): logs/servicename
		serviceLogDir = filepath.Join(logsDir, serviceName)
	}

	// Create the service-specific log directory
	if err := os.MkdirAll(serviceLogDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create log directory %s: %w", serviceLogDir, err)
	}

	// Create the log file path
	logFile := filepath.Join(serviceLogDir, serviceID+".log")
	return logFile, nil
}

// cleanupLogFile removes a log file if it exists
func (p *localProvisioner) cleanupLogFile(logFilePath string) error {
	if logFilePath == "" {
		return nil
	}

	// Check if log file exists
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to clean up
	}

	// Remove the log file
	if err := os.Remove(logFilePath); err != nil {
		return fmt.Errorf("failed to remove log file %s: %w", logFilePath, err)
	}

	fmt.Printf("Cleaned up log file: %s\n", logFilePath)
	return nil
}

// saveServiceState saves the provisioned service state to disk
func (p *localProvisioner) saveServiceState(service *LocalProvisionedService, databaseName string) error {
	stateDir := p.getStateDir()
	var targetDir string

	if databaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", databaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return fmt.Errorf("failed to create state directory %s: %w", targetDir, err)
	}

	// File name format: service_id.json (e.g., etcd_abc123.json)
	fileName := fmt.Sprintf("%s_%s.json", service.Service, service.ID)
	filePath := filepath.Join(targetDir, fileName)

	data, err := json.MarshalIndent(service, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal service state: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write service state file %s: %w", filePath, err)
	}

	return nil
}

// removeServiceState removes a service state file from disk
func (p *localProvisioner) removeServiceState(serviceID, serviceName, databaseName string) error {
	stateDir := p.getStateDir()
	var targetDir string

	if databaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", databaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	fileName := fmt.Sprintf("%s_%s.json", serviceName, serviceID)
	filePath := filepath.Join(targetDir, fileName)

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove state file %s: %w", filePath, err)
	}

	return nil
}

// loadDbProvisionedServices loads provisioned services for a specific database
func (p *localProvisioner) loadDbProvisionedServices(databaseName string) ([]*LocalProvisionedService, error) {
	if databaseName == "" {
		return nil, errors.New("database name is required")
	}

	stateDir := p.getStateDir()
	targetDir := filepath.Join(stateDir, "dbs", databaseName)

	// Check if target directory exists
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		return nil, nil // No services for this database
	}

	entries, err := os.ReadDir(targetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", targetDir, err)
	}

	var services []*LocalProvisionedService
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			// Parse filename: service_id.json
			name := strings.TrimSuffix(entry.Name(), ".json")
			parts := strings.SplitN(name, "_", 2)
			if len(parts) != 2 {
				// Skip invalid state files
				continue
			}

			serviceName := parts[0]
			serviceID := parts[1]

			req := &provisioner.DeprovisionRequest{
				Service:      serviceName,
				ServiceID:    serviceID,
				DatabaseName: databaseName,
			}
			service, err := p.loadServiceState(req)
			if err != nil {
				// Log warning but continue with other services
				fmt.Printf("Warning: failed to load state for %s service %s: %v\n", serviceName, serviceID, err)
				continue
			}

			if service != nil {
				services = append(services, service)
			}
		}
	}

	return services, nil
}

// loadEtcdServices loads etcd services from the top-level state directory
func (p *localProvisioner) loadEtcdServices() ([]*LocalProvisionedService, error) {
	stateDir := p.getStateDir()

	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil, nil // No state directory, no services running
	}

	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", stateDir, err)
	}

	var services []*LocalProvisionedService
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") && strings.HasPrefix(entry.Name(), "etcd_") {
			// Parse filename: etcd_serviceID.json
			name := strings.TrimSuffix(entry.Name(), ".json")
			parts := strings.SplitN(name, "_", 2)
			if len(parts) != 2 || parts[0] != "etcd" {
				continue
			}

			serviceID := parts[1]

			req := &provisioner.DeprovisionRequest{
				Service:      "etcd",
				ServiceID:    serviceID,
				DatabaseName: "", // etcd is a global service
			}
			service, err := p.loadServiceState(req)
			if err != nil {
				// Log warning but continue with other services
				fmt.Printf("Warning: failed to load state for etcd service %s: %v\n", serviceID, err)
				continue
			}

			if service != nil {
				services = append(services, service)
			}
		}
	}

	return services, nil
}

// loadGlobalServices loads all global services (non-database services) from state files
func (p *localProvisioner) loadGlobalServices() ([]*LocalProvisionedService, error) {
	stateDir := p.getStateDir()

	// Check if state directory exists
	if _, err := os.Stat(stateDir); os.IsNotExist(err) {
		return nil, nil // No state directory, no services running
	}

	entries, err := os.ReadDir(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", stateDir, err)
	}

	var services []*LocalProvisionedService
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			// Parse filename: serviceName_serviceID.json
			name := strings.TrimSuffix(entry.Name(), ".json")
			parts := strings.SplitN(name, "_", 2)
			if len(parts) != 2 {
				continue
			}

			serviceName := parts[0]
			serviceID := parts[1]

			// Load global services (non-etcd services can be included here)
			if serviceName == constants.ServiceMultiadmin || serviceName == "etcd" {
				req := &provisioner.DeprovisionRequest{
					Service:      serviceName,
					ServiceID:    serviceID,
					DatabaseName: "", // global services have no database name
				}

				service, err := p.loadServiceState(req)
				if err != nil {
					// Log warning but continue with other services
					fmt.Printf("Warning: failed to load state for global service %s: %v\n", serviceID, err)
					continue
				}

				if service != nil {
					services = append(services, service)
				}
			}
		}
	}

	return services, nil
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

// getExpectedPortsForDbService returns expected ports for a DB-scoped service (per cell)
func (p *localProvisioner) getExpectedPortsForDbService(serviceName, cell string) map[string]int {
	ports := make(map[string]int)

	cellConfig, err := p.getCellServiceConfig(cell, serviceName)
	if err != nil {
		return ports
	}

	switch serviceName {
	case constants.ServiceMultigateway:
		if httpPort, ok := cellConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case constants.ServiceMultipooler:
		if grpcPort, ok := cellConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := cellConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case constants.ServiceMultiorch:
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
	case constants.ServiceMultigateway:
		if httpPort, ok := serviceConfig["http_port"].(int); ok {
			ports["http"] = httpPort
		}
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
	case constants.ServiceMultipooler:
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := serviceConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case constants.ServiceMultiorch:
		if grpcPort, ok := serviceConfig["grpc_port"].(int); ok {
			ports["grpc"] = grpcPort
		}
		if httpPort, ok := serviceConfig["http_port"].(int); ok && httpPort > 0 {
			ports["http"] = httpPort
		}
	case constants.ServiceMultiadmin:
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
