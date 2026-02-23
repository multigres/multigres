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

package command

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/services/pgctld"
	"github.com/multigres/multigres/go/tools/retry"
	"github.com/multigres/multigres/go/tools/viperutil"

	"github.com/spf13/cobra"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// intToInt32 safely converts int to int32 for protobuf fields.
// Returns an error if value exceeds int32 range (should never happen for PIDs/ports).
func intToInt32(v int) (int32, error) {
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, fmt.Errorf("value %d exceeds int32 range", v)
	}
	return int32(v), nil
}

// PgCtldServerCmd holds the server command configuration
type PgCtldServerCmd struct {
	pgCtlCmd          *PgCtlCommand
	grpcServer        *servenv.GrpcServer
	senv              *servenv.ServEnv
	pgbackrestPort    viperutil.Value[int]
	pgbackrestCertDir viperutil.Value[string]
}

// AddServerCommand adds the server subcommand to the root command
func AddServerCommand(root *cobra.Command, pc *PgCtlCommand) {
	serverCmd := &PgCtldServerCmd{
		pgCtlCmd:   pc,
		grpcServer: servenv.NewGrpcServer(pc.reg),
		senv:       servenv.NewServEnvWithConfig(pc.reg, pc.lg, pc.vc, pc.telemetry),
		pgbackrestPort: viperutil.Configure(pc.reg, "pgbackrest-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "pgbackrest-port",
			Dynamic:  false,
		}),
		pgbackrestCertDir: viperutil.Configure(pc.reg, "pgbackrest-cert-dir", viperutil.Options[string]{
			Default:  "",
			FlagName: "pgbackrest-cert-dir",
			Dynamic:  false,
		}),
	}
	serverCmd.senv.InitServiceMap("grpc", constants.ServicePgctld)
	root.AddCommand(serverCmd.createCommand())
}

// validateServerFlags validates required flags for the server command
func (s *PgCtldServerCmd) validateServerFlags(cmd *cobra.Command, args []string) error {
	if err := s.senv.CobraPreRunE(cmd); err != nil {
		return err
	}
	return s.pgCtlCmd.validateGlobalFlags(cmd, args)
}

func (s *PgCtldServerCmd) createCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "server",
		Short:   "Run pgctld as a gRPC server daemon",
		Long:    `Run pgctld as a background gRPC server daemon to handle PostgreSQL management requests.`,
		RunE:    s.runServer,
		Args:    cobra.NoArgs,
		PreRunE: s.validateServerFlags,
	}

	s.grpcServer.RegisterFlags(cmd.Flags())
	// Don't register logger and viper config flags since they're already registered
	// as persistent flags in the root command and we're sharing those instances
	s.senv.RegisterFlagsWithoutLoggerAndConfig(cmd.Flags())

	// pgBackRest TLS server flags (server command only)
	cmd.Flags().Int("pgbackrest-port", s.pgbackrestPort.Default(), "pgBackRest TLS server port")
	cmd.Flags().String("pgbackrest-cert-dir", s.pgbackrestCertDir.Default(), "Directory containing ca.crt, pgbackrest.crt, pgbackrest.key")
	viperutil.BindFlags(cmd.Flags(), s.pgbackrestPort, s.pgbackrestCertDir)

	return cmd
}

func (s *PgCtldServerCmd) runServer(cmd *cobra.Command, args []string) error {
	// TODO(dweitzman): Add ServiceInstanceID and Cell that relate to the multipooler this pgctld serves
	if err := s.senv.Init(servenv.ServiceIdentity{
		ServiceName: constants.ServicePgctld,
	}); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}

	// Get the configured logger
	logger := s.senv.GetLogger()

	// Start reaping orphaned children to prevent zombie processes
	// This is necessary because pg_ctl with -W flag can create orphaned child processes
	// that get reparented to pgctld (PID 1 in container). Without this, these processes
	// remain in defunct state after exit.
	go reapOrphanedChildren(logger)

	// Create and register our service
	poolerDir := s.pgCtlCmd.GetPoolerDir()
	pgbackrestPort := s.pgbackrestPort.Get()
	pgbackrestCertDir := s.pgbackrestCertDir.Get()

	pgctldService, err := NewPgCtldService(
		logger,
		s.pgCtlCmd.pgPort.Get(),
		s.pgCtlCmd.pgUser.Get(),
		s.pgCtlCmd.pgDatabase.Get(),
		s.pgCtlCmd.timeout.Get(),
		poolerDir,
		s.pgCtlCmd.pgListenAddresses.Get(),
		pgbackrestPort,
		pgbackrestCertDir,
	)
	if err != nil {
		return err
	}

	s.senv.OnRun(func() {
		logger.Info("pgctld server starting up",
			"grpc_port", s.grpcServer.Port(),
		)

		// Start pgBackRest management
		pgctldService.StartPgBackRestManagement()

		// Register gRPC service with the global GRPCServer
		if s.grpcServer.CheckServiceMap(constants.ServicePgctld, s.senv) {
			pb.RegisterPgCtldServer(s.grpcServer.Server, pgctldService)
		}
		// TODO(sougou): Add http server
	})

	s.senv.OnClose(func() {
		logger.Info("pgctld server shutting down")
		pgctldService.Close()
	})

	return s.senv.RunDefault(s.grpcServer)
}

// reapOrphanedChildren handles SIGCHLD signals to reap zombie processes.
// This is necessary because pg_ctl with -W flag creates child processes that get
// reparented to pgctld (when running as PID 1 in a container). Without this reaper,
// these child processes remain in defunct (zombie) state after exit.
//
// The function runs in a goroutine and continuously waits for SIGCHLD signals,
// then reaps all available zombie children using Wait4 with WNOHANG.
func reapOrphanedChildren(logger *slog.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGCHLD)

	for range sigCh {
		// Reap all zombie children
		for {
			var status syscall.WaitStatus
			pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, nil)
			if err != nil || pid <= 0 {
				// No more children to reap
				break
			}
			logger.Debug("Reaped orphaned child process", "pid", pid, "status", status)
		}
	}
}

// PgCtldService implements the pgctld gRPC service
type PgCtldService struct {
	pb.UnimplementedPgCtldServer
	logger     *slog.Logger
	pgPort     int
	pgUser     string
	pgDatabase string
	timeout    int
	poolerDir  string
	config     *pgctld.PostgresCtlConfig

	// pgBackRest management
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	pgBackRestCmd    *exec.Cmd
	pgBackRestStatus *pb.PgBackRestStatus
	statusMu         sync.RWMutex
	restartCount     int32
}

// pgbackrestServerConfigPath returns the path to the pgbackrest server config file.
func (s *PgCtldService) pgbackrestServerConfigPath() string {
	return filepath.Join(s.poolerDir, "pgbackrest", "pgbackrest-server.conf")
}

// NewPgCtldService creates a new PgCtldService with validation
func NewPgCtldService(
	logger *slog.Logger,
	pgPort int,
	pgUser string,
	pgDatabase string,
	timeout int,
	poolerDir string,
	listenAddresses string,
	pgbackrestPort int,
	pgbackrestCertDir string,
) (*PgCtldService, error) {
	// Validate essential parameters for service creation
	// Note: We don't validate postgresDataDir or postgresConfigFile existence here
	// because the server should be able to start even with uninitialized data directory
	if poolerDir == "" {
		return nil, errors.New("pooler-dir needs to be set")
	}
	if pgPort == 0 {
		return nil, errors.New("pg-port needs to be set")
	}
	if pgUser == "" {
		return nil, errors.New("pg-user needs to be set")
	}
	if pgDatabase == "" {
		return nil, errors.New("pg-database needs to be set")
	}
	if timeout == 0 {
		return nil, errors.New("timeout needs to be set")
	}
	if listenAddresses == "" {
		return nil, errors.New("listen-addresses needs to be set")
	}

	// Create the PostgreSQL config once during service initialization
	config, err := pgctld.NewPostgresCtlConfig(
		pgPort,
		pgUser,
		pgDatabase,
		timeout,
		pgctld.PostgresDataDir(poolerDir),
		pgctld.PostgresConfigFile(poolerDir),
		poolerDir,
		listenAddresses,
		pgctld.PostgresSocketDir(poolerDir),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL config: %w", err)
	}

	// Generate pgbackrest-server.conf if pgbackrest port and cert dir provided
	if pgbackrestPort > 0 && pgbackrestCertDir != "" {
		configPath, err := backup.WriteServerConfig(backup.ServerConfigOpts{
			PoolerDir:     poolerDir,
			CertDir:       pgbackrestCertDir,
			Port:          pgbackrestPort,
			Pg1Port:       pgPort,
			Pg1SocketPath: pgctld.PostgresSocketDir(poolerDir),
			Pg1Path:       pgctld.PostgresDataDir(poolerDir),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to generate pgbackrest-server.conf: %w", err)
		}
		logger.Info("Generated pgbackrest-server.conf", "path", configPath)
	}

	//nolint:gocritic // Background context for pgBackRest lifecycle management
	ctx, cancel := context.WithCancel(context.Background())

	return &PgCtldService{
		logger:     logger,
		pgPort:     pgPort,
		pgUser:     pgUser,
		pgDatabase: pgDatabase,
		timeout:    timeout,
		poolerDir:  poolerDir,
		config:     config,
		ctx:        ctx,
		cancel:     cancel,
		pgBackRestStatus: &pb.PgBackRestStatus{
			Running: false,
		},
	}, nil
}

// setPgBackRestStatus updates the pgBackRest status thread-safely and returns the current restart count
func (s *PgCtldService) setPgBackRestStatus(running bool, errorMessage string, incrementRestart bool) int32 {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()

	if incrementRestart {
		s.restartCount++
	}

	s.pgBackRestStatus.Running = running
	s.pgBackRestStatus.ErrorMessage = errorMessage
	s.pgBackRestStatus.RestartCount = s.restartCount

	if running {
		s.pgBackRestStatus.LastStarted = timestamppb.Now()
	}

	return s.restartCount
}

// getPgBackRestStatus returns a copy of the current status thread-safely
func (s *PgCtldService) getPgBackRestStatus() *pb.PgBackRestStatus {
	s.statusMu.RLock()
	defer s.statusMu.RUnlock()

	// Return a copy to avoid race conditions
	return &pb.PgBackRestStatus{
		Running:      s.pgBackRestStatus.Running,
		ErrorMessage: s.pgBackRestStatus.ErrorMessage,
		RestartCount: s.pgBackRestStatus.RestartCount,
		LastStarted:  s.pgBackRestStatus.LastStarted,
	}
}

// Close shuts down the pgctld service gracefully
func (s *PgCtldService) Close() {
	s.logger.Info("Shutting down pgctld service")

	// Signal managePgBackRest goroutine to stop
	s.cancel()

	// Kill pgBackRest process if running
	if s.pgBackRestCmd != nil && s.pgBackRestCmd.Process != nil {
		s.logger.Info("Terminating pgBackRest server", "pid", s.pgBackRestCmd.Process.Pid)
		if err := s.pgBackRestCmd.Process.Kill(); err != nil {
			s.logger.Warn("Failed to kill pgBackRest process", "error", err)
		}
	}

	// Wait for goroutines to fully exit
	s.wg.Wait()
	s.logger.Info("pgctld service shutdown complete")
}

// startPgBackRest starts the pgBackRest TLS server process
// Returns the command on success, or error on failure
func (s *PgCtldService) startPgBackRest(ctx context.Context) (*exec.Cmd, error) {
	configPath := s.pgbackrestServerConfigPath()

	// Verify config exists
	if _, err := os.Stat(configPath); err != nil {
		return nil, fmt.Errorf("pgbackrest-server.conf not found at %s: %w", configPath, err)
	}

	// Build command: pgbackrest server
	// Note: Config is passed via PGBACKREST_CONFIG environment variable
	cmd := exec.CommandContext(ctx, "pgbackrest", "server")
	cmd.Env = append(os.Environ(), "PGBACKREST_CONFIG="+configPath)

	// Start the process
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pgbackrest server: %w", err)
	}

	logPath := filepath.Join(s.poolerDir, "pgbackrest", "log")
	s.logger.InfoContext(ctx, "pgBackRest TLS server started",
		"pid", cmd.Process.Pid,
		"config", configPath,
		"logs", logPath)
	return cmd, nil
}

// managePgBackRest manages the pgBackRest TLS server lifecycle with retry and restart logic
func (s *PgCtldService) managePgBackRest(ctx context.Context) {
	// Check if pgbackrest config exists before attempting to start
	configPath := s.pgbackrestServerConfigPath()
	if _, err := os.Stat(configPath); err != nil {
		s.logger.InfoContext(ctx, "pgBackRest config not found, skipping pgBackRest server startup", "config_path", configPath)
		s.setPgBackRestStatus(false, "config not found", false)
		return
	}

	r := retry.New(1*time.Second, 10*time.Second)

	for {
		var cmd *exec.Cmd

		// Try to start with retry policy (max 5 attempts)
		for attempt, err := range r.Attempts(ctx) {
			if err != nil {
				// Context cancelled during startup - clean shutdown
				s.setPgBackRestStatus(false, fmt.Sprintf("context cancelled: %v", err), false)
				return
			}
			if attempt >= 4 {
				s.setPgBackRestStatus(false, "failed after 5 attempts", false)
				return
			}

			var startErr error
			cmd, startErr = s.startPgBackRest(ctx)
			if startErr == nil {
				s.setPgBackRestStatus(true, "", false)
				s.pgBackRestCmd = cmd
				break // Success, exit retry loop
			}
			s.logger.WarnContext(ctx, "pgBackRest startup failed", "attempt", attempt+1, "error", startErr)
		}

		if cmd == nil {
			// Failed to start after retries
			return
		}

		// Wait for exit OR context cancellation
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait() // Ignore error - process exit is expected
			close(done)
		}()

		select {
		case <-done:
			// Process exited, restart
			currentCount := s.setPgBackRestStatus(false, "process exited, restarting", true)
			s.logger.InfoContext(ctx, "pgBackRest exited, restarting", "restart_count", currentCount)

		case <-ctx.Done():
			// Shutdown requested, kill process
			if cmd.Process != nil {
				_ = cmd.Process.Kill() // Ignore error - process may already be dead
			}
			<-done // Wait for Wait() to complete
			return
		}
	}
}

// StartPgBackRestManagement begins pgBackRest management in background
func (s *PgCtldService) StartPgBackRestManagement() {
	s.wg.Go(func() {
		s.managePgBackRest(s.ctx)
	})
}

func (s *PgCtldService) Start(ctx context.Context, req *pb.StartRequest) (*pb.StartResponse, error) {
	s.logger.InfoContext(ctx, "gRPC Start request", "port", req.Port)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for start operation
	result, err := StartPostgreSQLWithResult(s.logger, s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	pid, err := intToInt32(result.PID)
	if err != nil {
		return nil, fmt.Errorf("invalid PID: %w", err)
	}

	return &pb.StartResponse{
		Pid:     pid,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.logger.InfoContext(ctx, "gRPC Stop request", "mode", req.Mode)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for stop operation
	result, err := StopPostgreSQLWithResult(s.logger, s.config, req.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return &pb.StopResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	s.logger.InfoContext(ctx, "gRPC Restart request", "mode", req.Mode, "port", req.Port, "as_standby", req.AsStandby)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for restart operation
	result, err := RestartPostgreSQLWithResult(s.logger, s.config, req.Mode, req.AsStandby)
	if err != nil {
		return nil, fmt.Errorf("failed to restart PostgreSQL: %w", err)
	}

	pid, err := intToInt32(result.PID)
	if err != nil {
		return nil, fmt.Errorf("invalid PID: %w", err)
	}

	return &pb.RestartResponse{
		Pid:     pid,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	s.logger.InfoContext(ctx, "gRPC ReloadConfig request")

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for reload operation
	result, err := ReloadPostgreSQLConfigWithResult(s.logger, s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to reload PostgreSQL configuration: %w", err)
	}

	return &pb.ReloadConfigResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	s.logger.DebugContext(ctx, "gRPC Status request")

	// First check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		port, err := intToInt32(s.pgPort)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %w", err)
		}
		return &pb.StatusResponse{
			Status:           pb.ServerStatus_NOT_INITIALIZED,
			DataDir:          pgctld.PostgresDataDir(s.poolerDir),
			Port:             port,
			Message:          "Data directory is not initialized",
			PgbackrestStatus: s.getPgBackRestStatus(),
		}, nil
	}

	// Use the pre-configured PostgreSQL config for status operation
	result, err := GetStatusWithResult(ctx, s.logger, s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}

	// Convert status string to protobuf enum
	var status pb.ServerStatus
	switch result.Status {
	case statusStopped:
		status = pb.ServerStatus_STOPPED
	case statusRunning:
		status = pb.ServerStatus_RUNNING
	default:
		status = pb.ServerStatus_STOPPED
	}

	pid, err := intToInt32(result.PID)
	if err != nil {
		return nil, fmt.Errorf("invalid PID: %w", err)
	}
	port, err := intToInt32(result.Port)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	return &pb.StatusResponse{
		Status:           status,
		Pid:              pid,
		Version:          result.Version,
		Uptime:           durationpb.New(time.Duration(result.UptimeSeconds) * time.Second),
		DataDir:          result.DataDir,
		Port:             port,
		Ready:            result.Ready,
		Message:          result.Message,
		PgbackrestStatus: s.getPgBackRestStatus(),
	}, nil
}

func (s *PgCtldService) Version(ctx context.Context, req *pb.VersionRequest) (*pb.VersionResponse, error) {
	s.logger.DebugContext(ctx, "gRPC Version request")
	result, err := GetVersionWithResult(ctx, s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	return &pb.VersionResponse{
		Version: result.Version,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) InitDataDir(ctx context.Context, req *pb.InitDataDirRequest) (*pb.InitDataDirResponse, error) {
	s.logger.InfoContext(ctx, "gRPC InitDataDir request")

	// Use the shared init function with detailed result
	result, err := InitDataDirWithResult(s.logger, s.poolerDir, s.pgPort, s.pgUser)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	return &pb.InitDataDirResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) PgRewind(ctx context.Context, req *pb.PgRewindRequest) (*pb.PgRewindResponse, error) {
	s.logger.InfoContext(ctx, "gRPC PgRewind request",
		"source_host", req.GetSourceHost(),
		"source_port", req.GetSourcePort(),
		"dry_run", req.GetDryRun())

	// Check if postgres is cleanly stopped before pg_rewind
	// If not, try crash recovery - this is needed for rewind dry-run to work
	// This check is best effort. It's not harmful to try the pg_rewind if
	// crash recovery fails, the dry run is just unlikely to succeed in that case.
	cleanlyStopped, err := isPostgresCleanlyStopped(ctx, s.poolerDir)
	if err != nil {
		s.logger.WarnContext(ctx, "Failed to check postgres state (continuing anyway)", "error", err)
	} else if !cleanlyStopped {
		// Try to run crash recovery.
		// It's not harmful to do this if postgres is already running.
		if err := runCrashRecovery(ctx, s.logger, s.poolerDir); err != nil {
			s.logger.WarnContext(ctx, "Crash recovery failed (continuing anyway)", "error", err)
		}
	}

	// Resolve password using existing function
	password, err := resolvePassword(s.poolerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve password: %w", err)
	}

	// Construct source server connection string (without password - will use PGPASSWORD env var)
	// Include application_name if provided (used for replication identification)
	sourceServer := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres",
		req.GetSourceHost(), req.GetSourcePort())
	if req.GetApplicationName() != "" {
		sourceServer = fmt.Sprintf("%s application_name=%s", sourceServer, req.GetApplicationName())
	}

	// Use the shared rewind function with detailed result, passing password separately
	result, err := PgRewindWithResult(ctx, s.logger, s.poolerDir, sourceServer, password, req.GetDryRun(), req.GetExtraArgs())
	if err != nil {
		s.logger.ErrorContext(ctx, "pg_rewind output", "output", result.Output)
		return nil, fmt.Errorf("failed to rewind PostgreSQL: %w", err)
	}

	return &pb.PgRewindResponse{
		Message: result.Message,
		Output:  result.Output,
	}, nil
}
