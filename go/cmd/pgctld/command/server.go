// Copyright 2025 The Supabase, Inc.
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

	"github.com/multigres/multigres/go/pgctld"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
)

func init() {
	Root.AddCommand(ServerCmd)
	ServerCmd.Flags().IntVar(&pgPort, "pg-port", pgPort, "PostgreSQL port")
}

// validateServerFlags validates required flags for the server command
func validateServerFlags(cmd *cobra.Command, args []string) error {
	// First run the standard servenv validation
	if err := servenv.CobraPreRunE(cmd, args); err != nil {
		return err
	}

	// Then run our global validation (but not initialization validation -
	// the gRPC server should start and validate initialization per method)
	return validateGlobalFlags(cmd, args)
}

var ServerCmd = &cobra.Command{
	Use:     "server",
	Short:   "Run pgctld as a gRPC server daemon",
	Long:    `Run pgctld as a background gRPC server daemon to handle PostgreSQL management requests.`,
	RunE:    runServer,
	Args:    cobra.NoArgs,
	PreRunE: validateServerFlags,
}

func runServer(cmd *cobra.Command, args []string) error {
	servenv.Init()

	// Get the configured logger
	logger := servenv.GetLogger()

	// Create and register our service
	poolerDir := pgctld.GetPoolerDir()
	pgctldService, err := NewPgCtldService(logger, pgHost, pgPort, pgUser, pgDatabase, timeout, poolerDir)
	if err != nil {
		return err
	}

	servenv.OnRun(func() {
		logger.Info("pgctld server starting up",
			"grpc_port", servenv.GRPCPort(),
		)

		// Register gRPC service with the global GRPCServer
		if servenv.GRPCCheckServiceMap("pgctld") {
			pb.RegisterPgCtldServer(servenv.GRPCServer, pgctldService)
		}
	})

	servenv.OnClose(func() {
		logger.Info("pgctld server shutting down")
		// TODO: add closing hooks
	})

	servenv.RunDefault()

	return nil
}

// PgCtldService implements the pgctld gRPC service
type PgCtldService struct {
	pb.UnimplementedPgCtldServer
	logger     *slog.Logger
	pgHost     string
	pgPort     int
	pgUser     string
	pgDatabase string
	pgPassword string
	timeout    int
	poolerDir  string
	config     *pgctld.PostgresCtlConfig
}

// validatePortConsistency checks that the provided port matches the port in the PostgreSQL config file
func validatePortConsistency(expectedPort int, configFilePath string) error {
	// Create a minimal PostgresServerConfig just to read the config file
	tempConfig := &pgctld.PostgresServerConfig{
		Path: configFilePath,
	}

	// Read the configuration from file (no wait time since this is validation)
	readConfig, err := pgctld.ReadPostgresServerConfig(tempConfig, 0)
	if err != nil {
		return fmt.Errorf("failed to read PostgreSQL config file %s: %w", configFilePath, err)
	}

	// Check if the port from config matches the expected port
	if readConfig.Port != expectedPort {
		return fmt.Errorf("pg-port flag (%d) does not match port in config file (%d). "+
			"The port may have been changed after initialization. "+
			"Either update the config file or re-initialize with the correct port",
			expectedPort, readConfig.Port)
	}

	return nil
}

// NewPgCtldService creates a new PgCtldService with validation
func NewPgCtldService(logger *slog.Logger, pgHost string, pgPort int, pgUser string, pgDatabase string, timeout int, poolerDir string) (*PgCtldService, error) {
	// Validate essential parameters for service creation
	// Note: We don't validate postgresDataDir or postgresConfigFile existence here
	// because the server should be able to start even with uninitialized data directory
	if poolerDir == "" {
		return nil, fmt.Errorf("pooler-dir needs to be set")
	}
	if pgHost == "" {
		return nil, fmt.Errorf("pg-host needs to be set")
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

	// Resolve password from file or environment variable
	effectivePassword, err := resolvePassword()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve password: %w", err)
	}

	// Create the PostgreSQL config once during service initialization
	config, err := pgctld.NewPostgresCtlConfig(
		pgHost,
		pgPort,
		pgUser,
		pgDatabase,
		effectivePassword,
		timeout,
		pgctld.PostgresDataDir(poolerDir),
		pgctld.PostgresConfigFile(poolerDir),
		poolerDir,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL config: %w", err)
	}

	// If data directory is initialized, validate that pg-port matches the config
	if pgctld.IsDataDirInitialized(poolerDir) {
		configFilePath := pgctld.PostgresConfigFile(poolerDir)
		if err := validatePortConsistency(pgPort, configFilePath); err != nil {
			return nil, fmt.Errorf("port validation failed: %w", err)
		}
	}

	return &PgCtldService{
		logger:     logger,
		pgHost:     pgHost,
		pgPort:     pgPort,
		pgUser:     pgUser,
		pgDatabase: pgDatabase,
		pgPassword: effectivePassword,
		timeout:    timeout,
		poolerDir:  poolerDir,
		config:     config,
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
	result, err := StartPostgreSQLWithResult(s.config)
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
	result, err := StopPostgreSQLWithResult(s.config, req.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return &pb.StopResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	s.logger.Info("gRPC Restart request", "mode", req.Mode, "port", req.Port)

	// Check if data directory is initialized
	if !pgctld.IsDataDirInitialized(s.poolerDir) {
		dataDir := pgctld.PostgresDataDir(s.poolerDir)
		return nil, fmt.Errorf("data directory not initialized: %s. Run 'pgctld init' first", dataDir)
	}

	// Use the pre-configured PostgreSQL config for restart operation
	result, err := RestartPostgreSQLWithResult(s.config, req.Mode)
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
	result, err := ReloadPostgreSQLConfigWithResult(s.config)
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
			Host:    s.pgHost,
			Message: "Data directory is not initialized",
		}, nil
	}

	// Use the pre-configured PostgreSQL config for status operation
	result, err := GetStatusWithResult(s.config)
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
		Status:        status,
		Pid:           int32(result.PID),
		Version:       result.Version,
		UptimeSeconds: result.UptimeSeconds,
		DataDir:       result.DataDir,
		Port:          int32(result.Port),
		Host:          result.Host,
		Ready:         result.Ready,
		Message:       result.Message,
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
	result, err := InitDataDirWithResult(s.poolerDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	return &pb.InitDataDirResponse{
		Message: result.Message,
	}, nil
}
