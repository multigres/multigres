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
	"os/exec"
	"time"

	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/tools/stringutil"
)

// provisionLocalproxy provisions the local proxy service by running "multigres local proxy"
func (p *localProvisioner) provisionLocalproxy(ctx context.Context, req *provisioner.ProvisionRequest) (*provisioner.ProvisionResult, error) {
	// Sanity check
	if req.Service != "localproxy" {
		return nil, fmt.Errorf("provisionLocalproxy called for wrong service type: %s", req.Service)
	}

	// Check if localproxy is already running
	existingService, err := p.findRunningService("localproxy")
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing localproxy service: %w", err)
	}

	if existingService != nil {
		fmt.Printf("localproxy is already running (PID %d) ✓\n", existingService.PID)
		return &provisioner.ProvisionResult{
			ServiceName: "localproxy",
			FQDN:        existingService.FQDN,
			Ports:       existingService.Ports,
			Metadata: map[string]any{
				"service_id": existingService.ID,
				"log_file":   existingService.LogFile,
			},
		}, nil
	}

	// Get localproxy config
	localproxyConfig := p.getServiceConfig("localproxy")

	// Get HTTP port from config
	httpPort := 15800
	if p, ok := localproxyConfig["http_port"].(int); ok && p > 0 {
		httpPort = p
	}

	// Get log level
	logLevel := "info"
	if level, ok := localproxyConfig["log_level"].(string); ok {
		logLevel = level
	}

	// Find multigres binary
	multigresBinary, err := p.findBinary("multigres", localproxyConfig)
	if err != nil {
		return nil, fmt.Errorf("multigres binary not found: %w", err)
	}

	// Generate unique ID for this service instance
	serviceID := stringutil.RandomString(8)

	// Create log file path
	logFile, err := p.createLogFile("localproxy", serviceID, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Build command arguments: multigres local proxy --http-port 8080 --log-level info --log-output <file>
	args := []string{
		"local", "proxy",
		"--http-port", fmt.Sprintf("%d", httpPort),
		"--log-level", logLevel,
		"--log-output", logFile,
	}

	// Add config paths if available
	for _, configPath := range p.configPaths {
		args = append(args, "--config-path", configPath)
	}

	// Start localproxy process
	localproxyCmd := exec.CommandContext(ctx, multigresBinary, args...)

	fmt.Printf("▶️  - Launching localproxy (HTTP:%d)...", httpPort)

	if err := localproxyCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start localproxy: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(localproxyCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("localproxy process validation failed: %w", err)
	}

	// Create provision state
	service := &LocalProvisionedService{
		ID:         serviceID,
		Service:    "localproxy",
		PID:        localproxyCmd.Process.Pid,
		BinaryPath: multigresBinary,
		Ports:      map[string]int{"http_port": httpPort},
		FQDN:       "localhost",
		LogFile:    logFile,
		StartedAt:  time.Now(),
	}

	// Save service state to disk
	if err := p.saveServiceState(service, ""); err != nil {
		fmt.Printf("Warning: failed to save service state: %v\n", err)
	}

	// Wait for localproxy to be ready
	servicePorts := map[string]int{"http_port": httpPort}
	if err := p.waitForServiceReady("localproxy", "localhost", servicePorts, 10*time.Second); err != nil {
		logs := p.readServiceLogs(logFile, 20)
		return nil, fmt.Errorf("localproxy readiness check failed: %w\n\nLast 20 lines from localproxy logs:\n%s", err, logs)
	}
	fmt.Printf(" ready ✓\n")

	return &provisioner.ProvisionResult{
		ServiceName: "localproxy",
		FQDN:        "localhost",
		Ports: map[string]int{
			"http_port": httpPort,
		},
		Metadata: map[string]any{
			"service_id": serviceID,
			"log_file":   logFile,
		},
	}, nil
}
