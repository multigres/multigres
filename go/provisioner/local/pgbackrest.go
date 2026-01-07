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
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/provisioner"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// PgbackRestProvisionResult contains the result of provisioning pgbackrest server
type PgbackRestProvisionResult struct {
	Port    int
	LogFile string
}

// certDir returns the directory where pgBackRest certificates are stored
func (p *localProvisioner) certDir() string {
	return filepath.Join(p.config.RootWorkingDir, "certs")
}

// PgBackRestCertPaths holds the paths to the generated pgBackRest certificates.
type PgBackRestCertPaths struct {
	CACertFile     string // ca.crt
	ServerCertFile string // pgbackrest.crt
	ServerKeyFile  string // pgbackrest.key
}

// GeneratePgBackRestCerts creates TLS certificates for pgBackRest server in the specified directory.
// This is a public function that can be reused by tests and other components.
// It creates:
//   - ca.crt and ca.key (CA certificate and key)
//   - pgbackrest.crt and pgbackrest.key (server certificate and key)
//
// Returns the paths to the generated certificates that are needed for pgBackRest configuration.
func GeneratePgBackRestCerts(certDir string) (*PgBackRestCertPaths, error) {
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create pgBackRest certificate directory: %w", err)
	}

	caCertFile := filepath.Join(certDir, "ca.crt")
	caKeyFile := filepath.Join(certDir, "ca.key")
	if err := generateCA(caCertFile, caKeyFile); err != nil {
		return nil, fmt.Errorf("failed to generate CA for pgBackRest: %w", err)
	}

	certFile := filepath.Join(certDir, "pgbackrest.crt")
	keyFile := filepath.Join(certDir, "pgbackrest.key")
	if err := generateCert(caCertFile, caKeyFile, certFile, keyFile, "pgbackrest", []string{"localhost", "pgbackrest"}); err != nil {
		return nil, fmt.Errorf("failed to generate certificate for pgBackRest: %w", err)
	}

	return &PgBackRestCertPaths{
		CACertFile:     caCertFile,
		ServerCertFile: certFile,
		ServerKeyFile:  keyFile,
	}, nil
}

// StartPgBackRestServer starts a pgBackRest server and waits for the config file to be generated.
// This is a public function that can be reused by tests and other components.
//
// Parameters:
//   - ctx: context for cancellation and timeout
//   - poolerDir: directory where multipooler will generate pgbackrest.conf
//   - configTimeout: how long to wait for the config file to be generated
//
// Returns the started command or an error.
// The caller is responsible for managing the process lifecycle (waiting, killing, etc.)
func StartPgBackRestServer(ctx context.Context, poolerDir string, configTimeout time.Duration) (*exec.Cmd, error) {
	// Find pgbackrest binary
	pgbackrestBinary, err := exec.LookPath("pgbackrest")
	if err != nil {
		return nil, fmt.Errorf("pgbackrest binary not found in PATH: %w", err)
	}

	// Wait for multipooler to generate the pgbackrest config file
	configFile := filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf")

	// Wait for config file with timeout
	waitCtx, cancel := context.WithTimeout(ctx, configTimeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			return nil, fmt.Errorf("timeout waiting for pgbackrest config file: %s", configFile)
		case <-ticker.C:
			if _, err := os.Stat(configFile); err == nil {
				goto ConfigReady
			}
		}
	}

ConfigReady:
	// Start pgbackrest server
	cmd := exec.CommandContext(ctx, pgbackrestBinary, "server")

	// Set environment variables
	cmd.Env = append(os.Environ(),
		"PGBACKREST_CONFIG="+configFile,
	)

	return cmd, nil
}

// provisionPgbackRestServer provisions a pgbackrest server for a cell
// It waits for the multipooler to generate the pgbackrest.conf file before starting the server
func (p *localProvisioner) provisionPgbackRestServer(ctx context.Context, dbName, cell string) (*PgbackRestProvisionResult, error) {
	// Create unique pgbackrest service ID for this cell
	pgbackrestServiceID := "pgbackrest-" + cell

	// Check if pgbackrest server is already running for this service combination
	existingService, err := p.findRunningDbService(constants.ServicePgbackrest, dbName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing pgbackrest service: %w", err)
	}

	// Check if the existing service matches our specific service ID
	if existingService != nil && existingService.ID == pgbackrestServiceID {
		fmt.Printf("pgbackrest server is already running (PID %d) ✓\n", existingService.PID)
		return &PgbackRestProvisionResult{
			Port:    existingService.Ports["pgbackrest_port"],
			LogFile: existingService.LogFile,
		}, nil
	}

	// Get cell-specific multipooler config to find pooler directory
	multipoolerConfig, err := p.getCellServiceConfig(cell, constants.ServiceMultipooler)
	if err != nil {
		return nil, fmt.Errorf("failed to get multipooler config for cell %s: %w", cell, err)
	}

	poolerDir, ok := multipoolerConfig["pooler_dir"].(string)
	if !ok || poolerDir == "" {
		return nil, fmt.Errorf("pooler_dir not found in multipooler config for cell %s", cell)
	}

	// Get cell-specific pgbackrest config
	pgbackrestConfig, err := p.getCellServiceConfig(cell, constants.ServicePgbackrest)
	if err != nil {
		return nil, fmt.Errorf("failed to get pgbackrest config for cell %s: %w", cell, err)
	}

	// Get pgbackrest port from config or use default
	pgbackrestPort := ports.DefaultPgbackRestPort
	if port, ok := pgbackrestConfig["port"].(int); ok && port > 0 {
		pgbackrestPort = port
	}

	// Create pgbackrest log file
	pgbackrestLogFile, err := p.createLogFile(constants.ServicePgbackrest, cell, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgbackrest log file: %w", err)
	}

	// Get cert directory
	certDir := p.certDir()

	// Wait for multipooler to generate config and start pgBackRest server
	configFile := filepath.Join(poolerDir, "pgbackrest", "pgbackrest.conf")
	fmt.Printf("▶️  - Waiting for pgbackrest config at: %s\n", configFile)

	pgbackrestCmd, err := StartPgBackRestServer(ctx, poolerDir, 30*time.Second)
	if err != nil {
		return nil, err
	}

	fmt.Printf("▶️  - Config file found, starting pgBackRest server (port:%d)...\n", pgbackrestPort)

	if err := telemetry.StartCmd(ctx, pgbackrestCmd); err != nil {
		return nil, fmt.Errorf("failed to start pgbackrest server: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(pgbackrestCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("pgbackrest process validation failed: %w", err)
	}

	fmt.Printf("▶️  - pgbackrest server started (PID %d) ✓\n", pgbackrestCmd.Process.Pid)

	// Create provision state for pgbackrest
	service := &LocalProvisionedService{
		ID:         pgbackrestServiceID,
		Service:    constants.ServicePgbackrest,
		PID:        pgbackrestCmd.Process.Pid,
		BinaryPath: pgbackrestCmd.Path,
		Ports:      map[string]int{"pgbackrest_port": pgbackrestPort},
		FQDN:       "localhost",
		LogFile:    pgbackrestLogFile,
		StartedAt:  time.Now(),
		DataDir:    certDir,
		Metadata: map[string]any{
			"cell":     cell,
			"database": dbName,
			"cert_dir": certDir,
		},
	}

	// Save pgbackrest service state to disk
	if err := p.saveServiceState(service, dbName); err != nil {
		fmt.Printf("Warning: failed to save pgbackrest service state: %v\n", err)
	}

	return &PgbackRestProvisionResult{
		Port:    pgbackrestPort,
		LogFile: pgbackrestLogFile,
	}, nil
}

// deprovisionPgbackRestServer deprovisions a pgbackrest server and cleans up certificates
func (p *localProvisioner) deprovisionPgbackRestServer(ctx context.Context, req *provisioner.DeprovisionRequest) error {
	// Delete certificate directory if cleaning
	if req.Clean {
		certDir := p.certDir()
		if err := os.RemoveAll(certDir); err != nil {
			fmt.Printf("Warning: failed to remove certificate directory %s: %v\n", certDir, err)
		} else {
			fmt.Printf("Cleaned up certificate directory: %s\n", certDir)
		}
	}

	// Use standard deprovision logic for the rest
	return p.deprovisionService(ctx, req)
}
