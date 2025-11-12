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
	"time"

	"google.golang.org/grpc"

	"github.com/multigres/multigres/go/grpccommon"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner/local/ports"
)

// startPostgreSQLViaPgctld starts PostgreSQL via pgctld gRPC and verifies it's running
func (p *localProvisioner) startPostgreSQLViaPgctld(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpccommon.LocalClientDialOptions()...)
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

	// If not initialized, initialize first
	if statusResp.GetStatus() == pb.ServerStatus_NOT_INITIALIZED {
		fmt.Printf(" initializing...")
		_, err = client.InitDataDir(ctx, &pb.InitDataDirRequest{})
		if err != nil {
			return fmt.Errorf("failed to initialize PostgreSQL data directory: %w", err)
		}
	}

	// Start PostgreSQL
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
func (p *localProvisioner) stopPostgreSQLViaPgctld(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(address, grpccommon.LocalClientDialOptions()...)
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
	pgctldServiceID := fmt.Sprintf("pgctld-%s", serviceID)

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
		if err := p.checkPgctldGrpcHealth(grpcAddress); err != nil {
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
	pgPort := ports.DefaultPostgresPort
	if port, ok := pgctldConfig["pg_port"].(int); ok && port > 0 {
		pgPort = port
	}

	// Get other pgctld configuration values with defaults
	pgDatabase := "postgres"
	if db, ok := pgctldConfig["pg_database"].(string); ok && db != "" {
		pgDatabase = db
	}

	pgUser := "postgres"
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
		return nil, fmt.Errorf("pooler_dir not found in config")
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

	// Initialize pgctld data directory
	fmt.Printf("▶️  - Initializing pgctld for %s/%s/%s...", dbName, tableGroup, serviceID)

	initArgs := []string{
		"init",
		"--pooler-dir", poolerDir,
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--pg-database", pgDatabase,
		"--pg-user", pgUser,
		"--timeout", fmt.Sprintf("%d", timeout),
		"--log-level", logLevel,
	}

	// Add password file if available
	if pgPwfile, ok := pgctldConfig["pg_pwfile"].(string); ok && pgPwfile != "" {
		initArgs = append(initArgs, "--pg-pwfile", pgPwfile)
	}

	initCmd := exec.CommandContext(ctx, pgctldBinary, initArgs...)
	// Inject trace context for distributed tracing
	injectTraceContext(ctx, initCmd)

	if err := initCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to initialize pgctld data directory: %w", err)
	}
	fmt.Printf(" initialized ✓\n")

	// Start pgctld server
	fmt.Printf("▶️  - Starting pgctld server (gRPC:%d)...", grpcPort)

	serverArgs := []string{
		"server",
		"--pooler-dir", poolerDir,
		"--grpc-port", fmt.Sprintf("%d", grpcPort),
		"--pg-port", fmt.Sprintf("%d", pgPort),
		"--pg-database", pgDatabase,
		"--pg-user", pgUser,
		"--timeout", fmt.Sprintf("%d", timeout),
		"--log-level", logLevel,
		"--log-output", pgctldLogFile,
	}

	// Add socket file if configured
	if socketFile != "" {
		serverArgs = append(serverArgs, "--grpc-socket-file", socketFile)
	}

	pgctldCmd := exec.CommandContext(ctx, pgctldBinary, serverArgs...)
	// Inject trace context for distributed tracing
	injectTraceContext(ctx, pgctldCmd)

	if err := pgctldCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pgctld server: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(pgctldCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("pgctld process validation failed: %w", err)
	}

	// Wait for pgctld to be ready
	servicePorts := map[string]int{"grpc_port": grpcPort}
	if err := p.waitForServiceReady("pgctld", "localhost", servicePorts, 60*time.Second); err != nil {
		logs := p.readServiceLogs(pgctldLogFile, 20)
		return nil, fmt.Errorf("pgctld readiness check failed: %w\n\nLast 20 lines from pgctld logs:\n%s", err, logs)
	}

	// Now that pgctld is healthy, start PostgreSQL
	grpcAddress := fmt.Sprintf("localhost:%d", grpcPort)
	if err := p.startPostgreSQLViaPgctld(grpcAddress); err != nil {
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
	if err := p.stopPostgreSQLViaPgctld(address); err != nil {
		fmt.Printf("Warning: failed to stop PostgreSQL gracefully: %v\n", err)
	}

	// Then stop the pgctld process itself
	fmt.Printf("Stopping pgctld process...")
	if err := p.stopProcessByPID(service.PID); err != nil {
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
