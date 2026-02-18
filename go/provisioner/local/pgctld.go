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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/telemetry"
)

// startPostgreSQLViaPgctld checks PostgreSQL status via pgctld gRPC.
// It does NOT auto-initialize PostgreSQL - that's handled by multiorch's bootstrap process.
// This function only starts PostgreSQL if the data directory is already initialized.
func (p *localProvisioner) startPostgreSQLViaPgctld(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := grpccommon.NewClient(address, grpccommon.WithDialOptions(grpccommon.LocalClientDialOptions()...))
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	// First, check if PostgreSQL is already running
	statusResp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get pgctld status: %w", err)
	}

	// If already running, we're good
	if statusResp.GetStatus() == pb.ServerStatus_RUNNING {
		fmt.Printf(" PostgreSQL already running ✓")
		return nil
	}

	// If not initialized, skip starting PostgreSQL.
	// Multiorch will handle initialization through the bootstrap process.
	if statusResp.GetStatus() == pb.ServerStatus_NOT_INITIALIZED {
		fmt.Printf(" PostgreSQL not initialized (multiorch will bootstrap) ✓")
		return nil
	}

	// Data directory exists but PostgreSQL is not running - start it
	fmt.Printf(" starting PostgreSQL...")
	startResp, err := client.Start(ctx, &pb.StartRequest{})
	if err != nil {
		return fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Verify PostgreSQL is now running
	statusResp, err = client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to verify PostgreSQL status after start: %w", err)
	}

	if statusResp.GetStatus() != pb.ServerStatus_RUNNING {
		return fmt.Errorf("PostgreSQL failed to start - status: %s, message: %s",
			statusResp.GetStatus().String(), statusResp.GetMessage())
	}

	fmt.Printf(" PostgreSQL started (PID: %d) ✓\n", statusResp.GetPid())
	if startResp.GetMessage() != "" {
		fmt.Printf(" - %s", startResp.GetMessage())
	}

	return nil
}

// stopPostgreSQLViaPgctld stops PostgreSQL via pgctld gRPC
func (p *localProvisioner) stopPostgreSQLViaPgctld(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := grpccommon.NewClient(address, grpccommon.WithDialOptions(grpccommon.LocalClientDialOptions()...))
	if err != nil {
		return fmt.Errorf("failed to connect to pgctld gRPC server: %w", err)
	}
	defer conn.Close()

	client := pb.NewPgCtldClient(conn)

	// Check if PostgreSQL is running
	statusResp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to get pgctld status: %w", err)
	}

	// If not running, nothing to stop
	if statusResp.GetStatus() != pb.ServerStatus_RUNNING {
		fmt.Printf(" PostgreSQL already stopped")
		return nil
	}

	// Stop PostgreSQL with fast mode
	fmt.Printf(" stopping PostgreSQL...")
	stopResp, err := client.Stop(ctx, &pb.StopRequest{Mode: "fast"})
	if err != nil {
		return fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	// Verify PostgreSQL is now stopped
	statusResp, err = client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return fmt.Errorf("failed to verify PostgreSQL status after stop: %w", err)
	}

	if statusResp.GetStatus() != pb.ServerStatus_STOPPED {
		return fmt.Errorf("PostgreSQL failed to stop - status: %s, message: %s",
			statusResp.GetStatus().String(), statusResp.GetMessage())
	}

	fmt.Printf(" PostgreSQL stopped ✓\n")
	if stopResp.GetMessage() != "" {
		fmt.Printf(" - %s", stopResp.GetMessage())
	}

	return nil
}

// provisionPgctld provisions a pgctld instance for a multipooler with the new directory structure
func (p *localProvisioner) provisionPgctld(ctx context.Context, dbName, tableGroup, serviceID, cell string) (*PgctldProvisionResult, error) {
	// Create unique pgctld service ID using multipooler's service ID
	pgctldServiceID := "pgctld-" + serviceID

	// Check if pgctld is already running for this service combination
	existingService, err := p.findRunningDbService("pgctld", dbName, cell)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing pgctld service: %w", err)
	}

	// Check if the existing service matches our specific service ID
	if existingService != nil && existingService.ID == pgctldServiceID {
		fmt.Printf("pgctld is already running (PID %d)", existingService.PID)

		// Verify PostgreSQL is running via gRPC health check
		grpcAddress := fmt.Sprintf("localhost:%d", existingService.Ports["grpc_port"])
		if err := p.checkPgctldGrpcHealth(ctx, grpcAddress); err != nil {
			logs := p.readServiceLogs(existingService.LogFile, 20)
			return nil, fmt.Errorf("pgctld health check failed: %w\n\nLast 20 lines from pgctld logs:\n%s", err, logs)
		}

		fmt.Printf(" ✓\n")
		return &PgctldProvisionResult{
			Address: fmt.Sprintf("localhost:%d", existingService.Ports["grpc_port"]),
			Port:    existingService.Ports["grpc_port"],
			LogFile: existingService.LogFile,
		}, nil
	}

	// Get cell-specific pgctld config
	pgctldConfig, err := p.getCellServiceConfig(cell, "pgctld")
	if err != nil {
		return nil, fmt.Errorf("failed to get pgctld config for cell %s: %w", cell, err)
	}

	// Find pgctld binary
	pgctldBinary, err := p.findBinary("pgctld", pgctldConfig)
	if err != nil {
		return nil, fmt.Errorf("pgctld binary not found: %w", err)
	}

	// Get gRPC port from config or use default
	grpcPort := ports.DefaultPgctldGRPC
	if port, ok := pgctldConfig["grpc_port"].(int); ok && port > 0 {
		grpcPort = port
	}

	// Get PostgreSQL port from config or use default
	pgPort := ports.DefaultLocalPostgresPort
	if port, ok := pgctldConfig["pg_port"].(int); ok && port > 0 {
		pgPort = port
	}

	// Get other pgctld configuration values with defaults
	pgDatabase := constants.DefaultPostgresDatabase
	if db, ok := pgctldConfig["pg_database"].(string); ok && db != "" {
		pgDatabase = db
	}

	pgUser := constants.DefaultPostgresUser
	if user, ok := pgctldConfig["pg_user"].(string); ok && user != "" {
		pgUser = user
	}

	timeout := 30
	if t, ok := pgctldConfig["timeout"].(int); ok {
		timeout = t
	}

	logLevel := "info"
	if level, ok := pgctldConfig["log_level"].(string); ok && level != "" {
		logLevel = level
	}

	poolerDir := ""
	dir, ok := pgctldConfig["pooler_dir"].(string)
	if !ok {
		return nil, errors.New("pooler_dir not found in config")
	}
	poolerDir = dir

	// Get gRPC socket file if configured
	socketFile, err := getGRPCSocketFile(pgctldConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure gRPC socket file: %w", err)
	}
	if socketFile != "" {
		fmt.Printf("▶️  - Configuring pgctld gRPC Unix socket: %s\n", socketFile)
	}

	// Create pgctld log file
	pgctldLogFile, err := p.createLogFile("pgctld", serviceID, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgctld log file: %w", err)
	}

	// Note: We do NOT run 'pgctld init' here because that would initialize
	// the PostgreSQL data directory (initdb) before multiorch can bootstrap
	// the cluster. Multiorch needs to control initialization to properly set up
	// primary/standby replication across zones.

	// Start pgctld server
	fmt.Printf("▶️  - Starting pgctld server (gRPC:%d)...", grpcPort)

	serverArgs := []string{
		"server",
		"--pooler-dir", poolerDir,
		"--grpc-port", strconv.Itoa(grpcPort),
		"--pg-port", strconv.Itoa(pgPort),
		"--pg-database", pgDatabase,
		"--pg-user", pgUser,
		"--timeout", strconv.Itoa(timeout),
		"--log-level", logLevel,
		"--log-output", pgctldLogFile,
	}

	// Add socket file if configured
	if socketFile != "" {
		serverArgs = append(serverArgs, "--grpc-socket-file", socketFile)
	}

	// Add pgBackRest configuration if certificates are available
	if p.pgBackRestCertPaths != nil {
		// Get pgbackrest port from config or use default
		pgbackrestPort := ports.DefaultPgbackRestPort
		if port, ok := pgctldConfig["pgbackrest_port"].(int); ok && port > 0 {
			pgbackrestPort = port
		}

		serverArgs = append(serverArgs,
			"--pgbackrest-port", strconv.Itoa(pgbackrestPort),
			"--pgbackrest-cert-dir", p.certDir(),
		)

		// Add backup configuration based on config type
		switch p.config.Backup.Type {
		case "local":
			if p.config.Backup.Local != nil && p.config.Backup.Local.Path != "" {
				serverArgs = append(serverArgs,
					"--backup-type", "filesystem",
					"--backup-path", p.config.Backup.Local.Path,
				)
			}
		case "s3":
			if p.config.Backup.S3 != nil {
				// Construct repo path (default to /multigres, or with key prefix)
				repoPath := "/multigres"
				if p.config.Backup.S3.KeyPrefix != "" {
					repoPath = "/" + strings.TrimSuffix(p.config.Backup.S3.KeyPrefix, "/") + "/multigres"
				}
				serverArgs = append(serverArgs,
					"--backup-type", "s3",
					"--backup-bucket", p.config.Backup.S3.Bucket,
					"--backup-region", p.config.Backup.S3.Region,
					"--backup-path", repoPath,
				)
				if p.config.Backup.S3.Endpoint != "" {
					serverArgs = append(serverArgs, "--backup-endpoint", p.config.Backup.S3.Endpoint)
				}
				if p.config.Backup.S3.KeyPrefix != "" {
					serverArgs = append(serverArgs, "--backup-key-prefix", p.config.Backup.S3.KeyPrefix)
				}
				if p.config.Backup.S3.UseEnvCredentials {
					serverArgs = append(serverArgs, "--backup-use-env-credentials")
				}
			}
		}
	}

	pgctldCmd := exec.CommandContext(ctx, pgctldBinary, serverArgs...)

	if err := telemetry.StartCmd(ctx, pgctldCmd); err != nil {
		return nil, fmt.Errorf("failed to start pgctld server: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(pgctldCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("pgctld process validation failed: %w", err)
	}

	// Wait for pgctld to be ready
	servicePorts := map[string]int{"grpc_port": grpcPort}
	if err := p.waitForServiceReady(ctx, "pgctld", "localhost", servicePorts, 60*time.Second); err != nil {
		logs := p.readServiceLogs(pgctldLogFile, 20)
		return nil, fmt.Errorf("pgctld readiness check failed: %w\n\nLast 20 lines from pgctld logs:\n%s", err, logs)
	}

	// Now that pgctld is healthy, start PostgreSQL
	grpcAddress := fmt.Sprintf("localhost:%d", grpcPort)
	if err := p.startPostgreSQLViaPgctld(ctx, grpcAddress); err != nil {
		logs := p.readServiceLogs(pgctldLogFile, 20)
		return nil, fmt.Errorf("failed to start PostgreSQL: %w\n\nLast 20 lines from pgctld logs:\n%s", err, logs)
	}

	fmt.Printf(" ready ✓\n")

	// Create provision state for pgctld
	service := &LocalProvisionedService{
		ID:         pgctldServiceID,
		Service:    "pgctld",
		PID:        pgctldCmd.Process.Pid,
		BinaryPath: pgctldBinary,
		Ports:      map[string]int{"grpc_port": grpcPort},
		FQDN:       "localhost",
		LogFile:    pgctldLogFile,
		StartedAt:  time.Now(),
		DataDir:    poolerDir,
		Metadata:   map[string]any{"cell": cell, "database": dbName, "table_group": tableGroup, "service_id": serviceID, "multipooler_service_id": serviceID},
	}

	// Save pgctld service state to disk
	if err := p.saveServiceState(service, dbName); err != nil {
		fmt.Printf("Warning: failed to save pgctld service state: %v\n", err)
	}

	return &PgctldProvisionResult{
		Address: fmt.Sprintf("localhost:%d", grpcPort),
		Port:    grpcPort,
		LogFile: pgctldLogFile,
	}, nil
}

// deprovisionPgctld stops PostgreSQL via gRPC and then stops the pgctld process
func (p *localProvisioner) deprovisionPgctld(ctx context.Context, service *LocalProvisionedService) error {
	// First, try to gracefully stop PostgreSQL via pgctld gRPC
	grpcPort := service.Ports["grpc_port"]
	address := fmt.Sprintf("localhost:%d", grpcPort)

	fmt.Printf("Stopping PostgreSQL via pgctld...")
	if err := p.stopPostgreSQLViaPgctld(ctx, address); err != nil {
		fmt.Printf("Warning: failed to stop PostgreSQL gracefully: %v\n", err)
	}

	// Then stop the pgctld process itself
	fmt.Printf("Stopping pgctld process...")
	if err := p.stopProcessByPID(ctx, service.Service, service.PID); err != nil {
		return fmt.Errorf("failed to stop pgctld process: %w", err)
	}

	// Clean up log file
	if service.LogFile != "" {
		fmt.Printf("Cleaning up pgctld log file...")
		if err := os.Remove(service.LogFile); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: failed to remove pgctld log file %s: %v\n", service.LogFile, err)
		}
	}

	fmt.Printf(" pgctld stopped ✓\n")
	return nil
}
