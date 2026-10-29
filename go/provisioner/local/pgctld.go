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
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	pb "github.com/multigres/multigres/go/pb/pgctldservice"
	"github.com/multigres/multigres/go/provisioner/local/ports"
	"github.com/multigres/multigres/go/tools/executil"
	"github.com/multigres/multigres/go/tools/grpccommon"
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

	// Resolve pgctld config up-front so we can materialize the postgres
	// password file before either branch (already-running or fresh start)
	// returns. The file is the wire format both pgctld and the multipooler
	// in this cell consume via POSTGRES_PASSWORD_FILE.
	pgctldConfig, err := p.getCellServiceConfig(cell, "pgctld")
	if err != nil {
		return nil, fmt.Errorf("failed to get pgctld config for cell %s: %w", cell, err)
	}
	poolerDir, ok := pgctldConfig["pooler_dir"].(string)
	if !ok || poolerDir == "" {
		return nil, errors.New("pooler_dir not found in config")
	}
	pgPassword, ok := pgctldConfig["password"].(string)
	if !ok || pgPassword == "" {
		return nil, fmt.Errorf("pgctld password not configured for cell %s: set pg-password in the local provisioner config", cell)
	}
	pgPasswordFile, err := writePostgresPasswordFile(poolerDir, pgPassword)
	if err != nil {
		return nil, fmt.Errorf("failed to materialize postgres password file: %w", err)
	}

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
		// archive_command dials the sidecar, so a sidecar that died while
		// pgctld lived has to come back before anything archives.
		if pid, ok := existingService.Metadata["pgbackrestd_pid"].(float64); !ok || p.validateProcessRunning(int(pid)) != nil {
			newPID, err := p.startSidecarForPooler(ctx, pgctldConfig, poolerDir, pgPassword, serviceID, dbName)
			if err != nil {
				return nil, err
			}
			if existingService.Metadata == nil {
				existingService.Metadata = map[string]any{}
			}
			existingService.Metadata["pgbackrestd_pid"] = newPID
			if err := p.saveServiceState(existingService, dbName); err != nil {
				fmt.Printf("Warning: failed to save pgctld service state: %v\n", err)
			}
		}
		return &PgctldProvisionResult{
			Address:      fmt.Sprintf("localhost:%d", existingService.Ports["grpc_port"]),
			Port:         existingService.Ports["grpc_port"],
			LogFile:      existingService.LogFile,
			PasswordFile: pgPasswordFile,
			PoolerDir:    poolerDir,
		}, nil
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

	// Get HTTP port from config or use default
	httpPort := ports.DefaultPgctldHTTP
	if port, ok := pgctldConfig["http_port"].(int); ok && port > 0 {
		httpPort = port
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

	// pgbackrestd starts first: postgres's archive_command dials its socket as
	// soon as postgres runs.
	provisioned := false
	pgbackrestdPID, err := p.startSidecarForPooler(ctx, pgctldConfig, poolerDir, pgPassword, serviceID, dbName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !provisioned {
			_ = p.stopProcessByPID(ctx, "pgbackrestd", pgbackrestdPID)
		}
	}()

	// Start pgctld server
	fmt.Printf("▶️  - Starting pgctld server (gRPC:%d, HTTP:%d)...", grpcPort, httpPort)

	serverArgs := []string{
		"server",
		"--pooler-dir", poolerDir,
		"--grpc-port", strconv.Itoa(grpcPort),
		"--http-port", strconv.Itoa(httpPort),
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

	pgctldCmd := executil.Command(ctx, pgctldBinary, serverArgs...)

	// On macOS, ensure a valid locale is set for pgctld and its children (initdb, pg_ctl).
	// Without LC_ALL or LANG, initdb fails with "invalid locale settings".
	// Only inject when neither is set; an existing value in either variable is left untouched.
	if runtime.GOOS == "darwin" && os.Getenv("LC_ALL") == "" && os.Getenv("LANG") == "" {
		pgctldCmd.AddEnv("LC_ALL=C")
	}

	// Set PGDATA so pgctld knows where the PostgreSQL data directory is.
	pgctldCmd.AddEnv(constants.PgDataDirEnvVar + "=" + filepath.Join(poolerDir, "pg_data"))

	// Point pgctld at the password file written above. pgctld reads it during
	// init (--pwfile) and at server startup; multipooler reads the same file
	// for its admin pool.
	pgctldCmd.AddEnv(constants.PgPasswordFileEnvVar + "=" + pgPasswordFile)

	if err := pgctldCmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pgctld server: %w", err)
	}

	// Validate process is running
	if err := p.validateProcessRunning(pgctldCmd.Process.Pid); err != nil {
		return nil, fmt.Errorf("pgctld process validation failed: %w", err)
	}

	// Wait for pgctld to be ready
	servicePorts := map[string]int{"grpc_port": grpcPort, "http_port": httpPort}
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
	service.Metadata["pgbackrestd_pid"] = pgbackrestdPID

	// Save pgctld service state to disk
	if err := p.saveServiceState(service, dbName); err != nil {
		fmt.Printf("Warning: failed to save pgctld service state: %v\n", err)
	}
	provisioned = true

	return &PgctldProvisionResult{
		Address:      fmt.Sprintf("localhost:%d", grpcPort),
		Port:         grpcPort,
		LogFile:      pgctldLogFile,
		PasswordFile: pgPasswordFile,
		PoolerDir:    poolerDir,
	}, nil
}

// deprovisionPgctld stops PostgreSQL via gRPC and then stops the pgctld process.
// The pgbackrestd sidecar is stopped last and whatever happened to pgctld, so a
// failed stop never leaves a sidecar nothing records.
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
	var errs []error
	if err := p.stopProcessByPID(ctx, service.Service, service.PID); err != nil {
		errs = append(errs, fmt.Errorf("failed to stop pgctld process: %w", err))
	} else {
		// Clean up log file
		if service.LogFile != "" {
			fmt.Printf("Cleaning up pgctld log file...")
			if err := os.Remove(service.LogFile); err != nil && !os.IsNotExist(err) {
				fmt.Printf("Warning: failed to remove pgctld log file %s: %v\n", service.LogFile, err)
			}
		}
		fmt.Printf(" pgctld stopped ✓\n")
	}

	// pgbackrestd outlives postgres until here so a shutdown WAL segment can
	// still be archived.
	if pid, ok := service.Metadata["pgbackrestd_pid"].(float64); ok && pid > 0 {
		fmt.Printf("Stopping pgbackrestd process...")
		if err := p.stopProcessByPID(ctx, "pgbackrestd", int(pid)); err != nil {
			errs = append(errs, fmt.Errorf("failed to stop pgbackrestd process: %w", err))
		} else {
			fmt.Printf(" pgbackrestd stopped ✓\n")
		}
	}
	return errors.Join(errs...)
}

// startSidecarForPooler resolves everything pgbackrestd needs from the pgctld
// config and starts it. The sidecar binary sits beside pgctld.
func (p *localProvisioner) startSidecarForPooler(ctx context.Context, pgctldConfig map[string]any, poolerDir, pgPassword, serviceID, dbName string) (int, error) {
	pgctldBinary, err := p.findBinary("pgctld", pgctldConfig)
	if err != nil {
		return 0, fmt.Errorf("pgctld binary not found: %w", err)
	}
	pgbackrestdBinary, err := p.findBinary("pgbackrestd", map[string]any{"path": filepath.Join(filepath.Dir(pgctldBinary), "pgbackrestd")})
	if err != nil {
		return 0, fmt.Errorf("pgbackrestd binary not found: %w", err)
	}
	pgPort := ports.DefaultLocalPostgresPort
	if port, ok := pgctldConfig["pg_port"].(int); ok && port > 0 {
		pgPort = port
	}
	pgUser := constants.DefaultPostgresUser
	if user, ok := pgctldConfig["pg_user"].(string); ok && user != "" {
		pgUser = user
	}
	serverPort := ports.DefaultPgbackRestPort
	if port, ok := pgctldConfig["pgbackrest_port"].(int); ok && port > 0 {
		serverPort = port
	}
	logFile, err := p.createLogFile("pgbackrestd", serviceID, dbName)
	if err != nil {
		return 0, fmt.Errorf("failed to create pgbackrestd log file: %w", err)
	}
	fmt.Printf("▶️  - Starting pgbackrestd (server:%d)...", serverPort)
	pid, err := p.startPgBackRestd(ctx, pgbackrestdBinary, poolerDir, pgUser, pgPassword, logFile, pgPort, serverPort)
	if err != nil {
		return 0, err
	}
	fmt.Printf(" ready ✓\n")
	return pid, nil
}

// writePostgresPasswordFile materializes the postgres password into a 0600
// file at <poolerDir>/postgres-password and returns its path. The file is the
// wire format both pgctld and the multipooler in this cell consume via
// POSTGRES_PASSWORD_FILE; writing it idempotently (overwriting on each
// provision) keeps the file in sync with the YAML config when operators rotate
// the value. Cleanup is handled by the provisioner tearing down poolerDir.
func writePostgresPasswordFile(poolerDir, password string) (string, error) {
	if err := os.MkdirAll(poolerDir, 0o700); err != nil {
		return "", fmt.Errorf("create pooler dir: %w", err)
	}
	path := filepath.Join(poolerDir, "postgres-password")
	if err := os.WriteFile(path, []byte(password), 0o600); err != nil {
		return "", fmt.Errorf("write postgres password file: %w", err)
	}
	return path, nil
}
