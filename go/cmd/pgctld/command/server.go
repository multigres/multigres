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
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/services/pgctld"

	"github.com/spf13/cobra"

	"google.golang.org/protobuf/types/known/durationpb"

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
	pgCtlCmd   *PgCtlCommand
	grpcServer *servenv.GrpcServer
	senv       *servenv.ServEnv
}

// AddServerCommand adds the server subcommand to the root command
func AddServerCommand(root *cobra.Command, pc *PgCtlCommand) {
	serverCmd := &PgCtldServerCmd{
		pgCtlCmd:   pc,
		grpcServer: servenv.NewGrpcServer(pc.reg),
		senv:       servenv.NewServEnvWithConfig(pc.reg, pc.lg, pc.vc, pc.telemetry),
	}
	serverCmd.senv.InitServiceMap("grpc", constants.ServicePgctld)
	root.AddCommand(serverCmd.createCommand())
}

// validateServerFlags validates required flags for the server command
func (s *PgCtldServerCmd) validateServerFlags(cmd *cobra.Command, args []string) error {
	// First run the standard servenv validation
	if err := s.senv.CobraPreRunE(cmd); err != nil {
		return err
	}

	// Then run our global validation (but not initialization validation -
	// the gRPC server should start and validate initialization per method)
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

	// Create and register our service
	poolerDir := s.pgCtlCmd.GetPoolerDir()
	pgctldService, err := NewPgCtldService(logger, s.pgCtlCmd.pgPort.Get(), s.pgCtlCmd.pgUser.Get(), s.pgCtlCmd.pgDatabase.Get(), s.pgCtlCmd.timeout.Get(), poolerDir, s.pgCtlCmd.pgListenAddresses.Get())
	if err != nil {
		return err
	}

	s.senv.OnRun(func() {
		logger.Info("pgctld server starting up",
			"grpc_port", s.grpcServer.Port(),
		)

		// Register gRPC service with the global GRPCServer
		if s.grpcServer.CheckServiceMap(constants.ServicePgctld, s.senv) {
			pb.RegisterPgCtldServer(s.grpcServer.Server, pgctldService)
		}
		// TODO(sougou): Add http server
	})

	s.senv.OnClose(func() {
		logger.Info("pgctld server shutting down")
		// TODO: add closing hooks
	})

	return s.senv.RunDefault(s.grpcServer)
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
}

// validatePortConsistency is no longer needed because port, listen_addresses, and unix_socket_directories
// are now passed as command-line parameters and not stored in the config file.
// This makes backups portable across different environments.

// NewPgCtldService creates a new PgCtldService with validation
func NewPgCtldService(logger *slog.Logger, pgPort int, pgUser string, pgDatabase string, timeout int, poolerDir string, listenAddresses string) (*PgCtldService, error) {
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

	return &PgCtldService{
		logger:     logger,
		pgPort:     pgPort,
		pgUser:     pgUser,
		pgDatabase: pgDatabase,
		timeout:    timeout,
		poolerDir:  poolerDir,
		config:     config,
	}, nil
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
			Status:  pb.ServerStatus_NOT_INITIALIZED,
			DataDir: pgctld.PostgresDataDir(s.poolerDir),
			Port:    port,
			Message: "Data directory is not initialized",
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
		Status:  status,
		Pid:     pid,
		Version: result.Version,
		Uptime:  durationpb.New(time.Duration(result.UptimeSeconds) * time.Second),
		DataDir: result.DataDir,
		Port:    port,
		Ready:   result.Ready,
		Message: result.Message,
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

	// Resolve password using existing function
	password, err := resolvePassword(s.poolerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve password: %w", err)
	}

	// Construct source server connection string (without password - will use PGPASSWORD env var)
	sourceServer := fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres",
		req.GetSourceHost(), req.GetSourcePort())

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
