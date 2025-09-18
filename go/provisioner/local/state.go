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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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

// loadServiceState loads a specific service state from disk
func (p *localProvisioner) loadServiceState(req *provisioner.DeprovisionRequest) (*LocalProvisionedService, error) {
	stateDir := p.getStateDir()
	var targetDir string

	if req.DatabaseName != "" {
		// For database services: state/dbs/dbname
		targetDir = filepath.Join(stateDir, "dbs", req.DatabaseName)
	} else {
		// For non-database services (like etcd): state/
		targetDir = stateDir
	}

	fileName := fmt.Sprintf("%s_%s.json", req.Service, req.ServiceID)
	filePath := filepath.Join(targetDir, fileName)

	// Check if state file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil // Service not found
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file %s: %w", filePath, err)
	}

	var service LocalProvisionedService
	if err := json.Unmarshal(data, &service); err != nil {
		return nil, fmt.Errorf("failed to parse state file %s: %w", filePath, err)
	}

	// Sanity check: ensure this method is called for the expected service type
	if req.Service != service.Service {
		return nil, fmt.Errorf("deprovision%s called for wrong service type: %s", service.Service, req.Service)
	}

	return &service, nil
}

// loadDbProvisionedServices loads provisioned services for a specific database
func (p *localProvisioner) loadDbProvisionedServices(databaseName string) ([]*LocalProvisionedService, error) {
	if databaseName == "" {
		return nil, fmt.Errorf("database name is required")
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
			if serviceName == "multiadmin" || serviceName == "etcd" {
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
