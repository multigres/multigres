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
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"

	"google.golang.org/protobuf/types/known/durationpb"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

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
		grpcServer: servenv.NewGrpcServer(),
		senv:       servenv.NewServEnvWithConfig(pc.lg, pc.vc),
	}
	serverCmd.senv.InitServiceMap("grpc", "pgctld")
	root.AddCommand(serverCmd.createCommand())
}

// validateServerFlags validates required flags for the server command
func (s *PgCtldServerCmd) validateServerFlags(cmd *cobra.Command, args []string) error {
	// Setup logging using the shared logger instance from root command
	s.pgCtlCmd.lg.SetupLogging()

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
	s.senv.Init()

	// Get the configured logger
	logger := s.senv.GetLogger()

	// Create and register our service
	poolerDir := s.pgCtlCmd.GetPoolerDir()
	testOrphanDetection := s.senv.GetTestOrphanDetection()
	pgctldService, err := NewPgCtldService(logger, s.pgCtlCmd.pgPort.Get(), s.pgCtlCmd.pgUser.Get(), s.pgCtlCmd.pgDatabase.Get(), s.pgCtlCmd.timeout.Get(), poolerDir, s.pgCtlCmd.pgListenAddresses.Get(), testOrphanDetection)
	if err != nil {
		return err
	}

	s.senv.OnRun(func() {
		logger.Info("pgctld server starting up",
			"grpc_port", s.grpcServer.Port(),
		)

		// Register gRPC service with the global GRPCServer
		if s.grpcServer.CheckServiceMap("pgctld", s.senv) {
			pb.RegisterPgCtldServer(s.grpcServer.Server, pgctldService)
		}
		// TODO(sougou): Add http server
	})

	s.senv.OnClose(func() {
		logger.Info("pgctld server shutting down")
		// TODO: add closing hooks
	})

	s.senv.RunDefault(s.grpcServer)

	return nil
}

// PgCtldService implements the pgctld gRPC service
type PgCtldService struct {
	pb.UnimplementedPgCtldServer
	logger          *slog.Logger
	pgPort          int
	pgUser          string
	pgDatabase      string
	pgPassword      string
	timeout         int
	poolerDir       string
	config          *pgctld.PostgresCtlConfig
	postgresManager PostgresManager
}

// validatePortConsistency is no longer needed because port, listen_addresses, and unix_socket_directories
// are now passed as command-line parameters and not stored in the config file.
// This makes backups portable across different environments.

// NewPgCtldService creates a new PgCtldService with validation
func NewPgCtldService(logger *slog.Logger, pgPort int, pgUser string, pgDatabase string, timeout int, poolerDir string, listenAddresses string, testOrphanDetection bool) (*PgCtldService, error) {
	// Validate essential parameters for service creation
	// Note: We don't validate postgresDataDir or postgresConfigFile existence here
	// because the server should be able to start even with uninitialized data directory
	if poolerDir == "" {
		return nil, fmt.Errorf("pooler-dir needs to be set")
	}
	if pgPort == 0 {
		return nil, fmt.Errorf("pg-port needs to be set")
	}
	if pgUser == "" {
		return nil, fmt.Errorf("pg-user needs to be set")
	}
	if pgDatabase == "" {
		return nil, fmt.Errorf("pg-database needs to be set")
	}
	if timeout == 0 {
		return nil, fmt.Errorf("timeout needs to be set")
	}
	if listenAddresses == "" {
		return nil, fmt.Errorf("listen-addresses needs to be set")
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

	// Choose the appropriate PostgresManager based on test mode
	var pgManager PostgresManager
	if testOrphanDetection {
		pgManager = &ForegroundPostgresManager{}
	} else {
		pgManager = &DaemonPostgresManager{}
	}

	return &PgCtldService{
		logger:          logger,
		pgPort:          pgPort,
		pgUser:          pgUser,
		pgDatabase:      pgDatabase,
		timeout:         timeout,
		poolerDir:       poolerDir,
		config:          config,
		postgresManager: pgManager,
	}, nil
}

func (s *PgCtldService) Start(ctx context.Context, req *pb.StartRequest) (*pb.StartResponse, error) {
	s.logger.Info("gRPC Start request", "port", req.Port)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for start operation
	result, err := StartPostgreSQLWithResult(s.logger, s.postgresManager, s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	return &pb.StartResponse{
		Pid:     int32(result.PID),
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.logger.Info("gRPC Stop request", "mode", req.Mode)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for stop operation
	result, err := StopPostgreSQLWithResult(s.logger, s.postgresManager, s.config, req.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return &pb.StopResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	s.logger.Info("gRPC Restart request", "mode", req.Mode, "port", req.Port, "as_standby", req.AsStandby)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for restart operation
	result, err := RestartPostgreSQL(s.logger, s.postgresManager, s.config, req.Mode, req.AsStandby)
	if err != nil {
		return nil, fmt.Errorf("failed to restart PostgreSQL: %w", err)
	}

	return &pb.RestartResponse{
		Pid:     int32(result.PID),
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	s.logger.Info("gRPC ReloadConfig request")

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
	s.logger.Debug("gRPC Status request")

	// First check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		return &pb.StatusResponse{
			Status:  pb.ServerStatus_NOT_INITIALIZED,
			DataDir: pgctld.PostgresDataDir(s.poolerDir),
			Port:    int32(s.pgPort),
			Message: "Data directory is not initialized",
		}, nil
	}

	// Use the pre-configured PostgreSQL config for status operation
	result, err := GetStatusWithResult(s.logger, s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}

	// Convert status string to protobuf enum
	var status pb.ServerStatus
	switch result.Status {
	case "STOPPED":
		status = pb.ServerStatus_STOPPED
	case "RUNNING":
		status = pb.ServerStatus_RUNNING
	default:
		status = pb.ServerStatus_STOPPED
	}

	return &pb.StatusResponse{
		Status:  status,
		Pid:     int32(result.PID),
		Version: result.Version,
		Uptime:  durationpb.New(time.Duration(result.UptimeSeconds) * time.Second),
		DataDir: result.DataDir,
		Port:    int32(result.Port),
		Ready:   result.Ready,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Version(ctx context.Context, req *pb.VersionRequest) (*pb.VersionResponse, error) {
	s.logger.Debug("gRPC Version request")
	result, err := GetVersionWithResult(s.config)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	return &pb.VersionResponse{
		Version: result.Version,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) InitDataDir(ctx context.Context, req *pb.InitDataDirRequest) (*pb.InitDataDirResponse, error) {
	s.logger.Info("gRPC InitDataDir request")

	// Use the shared init function with detailed result
	result, err := InitDataDirWithResult(s.logger, s.poolerDir, s.pgPort, s.pgUser, req.PgPwfile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	return &pb.InitDataDirResponse{
		Message: result.Message,
	}, nil
}
