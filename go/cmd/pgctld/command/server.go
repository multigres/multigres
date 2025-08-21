/*
Copyright 2025 The Multigres Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package command

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	pb "github.com/multigres/multigres/go/pb"
)

func init() {
	Root.AddCommand(serverCmd)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run pgctld as a gRPC server daemon",
	Long:  `Run pgctld as a background gRPC server daemon to handle PostgreSQL management requests.`,
	RunE:  runServer,
}

func runServer(cmd *cobra.Command, args []string) error {
	logger := slog.Default()

	port := viper.GetInt("grpc-port")

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Create and register our service
	pgctldService := &PgCtldService{
		logger: logger,
	}
	pb.RegisterPgCtldServer(grpcServer, pgctldService)

	// Create listener
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	logger.Info("Starting pgctld gRPC server", "port", port)

	// Handle graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start server in goroutine
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server failed", "error", err)
		}
	}()

	logger.Info("pgctld gRPC server listening", "address", lis.Addr().String())

	// Wait for shutdown signal
	<-ctx.Done()

	logger.Info("Shutting down pgctld gRPC server")
	grpcServer.GracefulStop()

	return nil
}

// PgCtldService implements the pgctld gRPC service
type PgCtldService struct {
	pb.UnimplementedPgCtldServer
	logger *slog.Logger
}

func (s *PgCtldService) Start(ctx context.Context, req *pb.StartRequest) (*pb.StartResponse, error) {
	s.logger.Info("gRPC Start request", "data_dir", req.DataDir, "port", req.Port)

	// Create config from request parameters
	config := NewPostgresConfigFromStartRequest(req)

	if config.DataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	// Check if PostgreSQL is already running before attempting to start
	if isPostgreSQLRunning(config.DataDir) {
		// Get PID of running instance
		var pid int32
		if pidValue, err := readPostmasterPID(config.DataDir); err == nil {
			pid = int32(pidValue)
		}
		return &pb.StartResponse{
			Pid:     pid,
			Message: "PostgreSQL is already running",
		}, nil
	}

	// Start PostgreSQL using the config-based function
	if err := StartPostgreSQLWithConfig(config); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Get PID
	var pid int32
	if pidValue, err := readPostmasterPID(config.DataDir); err == nil {
		pid = int32(pidValue)
	}

	return &pb.StartResponse{
		Pid:     pid,
		Message: "PostgreSQL server started successfully",
	}, nil
}

func (s *PgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.logger.Info("gRPC Stop request", "data_dir", req.DataDir, "mode", req.Mode)

	// Create config from request parameters
	config := NewPostgresConfigFromStopRequest(req)

	if config.DataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	mode := req.Mode
	if mode == "" {
		mode = "fast"
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config.DataDir) {
		return &pb.StopResponse{
			Message: "PostgreSQL is not running",
		}, nil
	}

	// Stop PostgreSQL using the config-based function
	if err := StopPostgreSQLWithConfig(config, mode); err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return &pb.StopResponse{
		Message: "PostgreSQL server stopped successfully",
	}, nil
}

func (s *PgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	s.logger.Info("gRPC Restart request", "data_dir", req.DataDir, "mode", req.Mode)

	// Stop first
	stopReq := &pb.StopRequest{
		Mode:    req.Mode,
		Timeout: req.Timeout,
		DataDir: req.DataDir,
	}

	if _, err := s.Stop(ctx, stopReq); err != nil {
		return nil, fmt.Errorf("failed to stop during restart: %w", err)
	}

	// Then start
	startReq := &pb.StartRequest{
		DataDir:    req.DataDir,
		Port:       req.Port,
		SocketDir:  req.SocketDir,
		ConfigFile: req.ConfigFile,
		ExtraArgs:  req.ExtraArgs,
	}

	startResp, err := s.Start(ctx, startReq)
	if err != nil {
		return nil, fmt.Errorf("failed to start during restart: %w", err)
	}

	return &pb.RestartResponse{
		Pid:     startResp.Pid,
		Message: "PostgreSQL server restarted successfully",
	}, nil
}

func (s *PgCtldService) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	s.logger.Info("gRPC ReloadConfig request", "data_dir", req.DataDir)

	dataDir := req.DataDir
	if dataDir == "" {
		dataDir = viper.GetString("data-dir")
	}

	if dataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(dataDir) {
		return nil, fmt.Errorf("PostgreSQL is not running")
	}

	// Reload configuration
	if err := reloadPostgreSQLConfig(dataDir); err != nil {
		return nil, fmt.Errorf("failed to reload PostgreSQL configuration: %w", err)
	}

	return &pb.ReloadConfigResponse{
		Message: "PostgreSQL configuration reloaded successfully",
	}, nil
}

func (s *PgCtldService) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	s.logger.Debug("gRPC Status request", "data_dir", req.DataDir)

	// Create config from request parameters
	config := NewPostgresConfigFromStatusRequest(req)

	if config.DataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	// Check if data directory is initialized
	if !isDataDirInitialized(config.DataDir) {
		return &pb.StatusResponse{
			Status:  pb.ServerStatus_NOT_INITIALIZED,
			DataDir: config.DataDir,
			Port:    int32(config.Port),
			Host:    config.Host,
			Message: "Data directory is not initialized",
		}, nil
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(config.DataDir) {
		return &pb.StatusResponse{
			Status:  pb.ServerStatus_STOPPED,
			DataDir: config.DataDir,
			Port:    int32(config.Port),
			Host:    config.Host,
			Ready:   false,
			Message: "PostgreSQL server is stopped",
		}, nil
	}

	// Get PID
	var pid int32
	if pidValue, err := readPostmasterPID(config.DataDir); err == nil {
		pid = int32(pidValue)
	}

	// Check if server is ready
	ready := isServerReadyWithConfig(config)

	// Get version
	version := getServerVersionWithConfig(config)

	// Get uptime (approximate based on pidfile mtime)
	var uptimeSeconds int64
	pidFile := fmt.Sprintf("%s/postmaster.pid", config.DataDir)
	if stat, err := os.Stat(pidFile); err == nil {
		uptimeSeconds = int64(time.Now().Unix() - stat.ModTime().Unix())
	}

	return &pb.StatusResponse{
		Status:        pb.ServerStatus_RUNNING,
		Pid:           pid,
		Version:       version,
		UptimeSeconds: uptimeSeconds,
		DataDir:       config.DataDir,
		Port:          int32(config.Port),
		Host:          config.Host,
		Ready:         ready,
		Message:       "PostgreSQL server is running",
	}, nil
}

func (s *PgCtldService) Version(ctx context.Context, req *pb.VersionRequest) (*pb.VersionResponse, error) {
	s.logger.Debug("gRPC Version request")

	// Create config from base viper settings
	config := NewPostgresConfigFromViper()

	// Override with request parameters if provided
	if req.Host != "" {
		config.Host = req.Host
	}
	if req.Port > 0 {
		config.Port = int(req.Port)
	}
	if req.Database != "" {
		config.Database = req.Database
	}
	if req.User != "" {
		config.User = req.User
	}

	version := getServerVersionWithConfig(config)

	return &pb.VersionResponse{
		Version: version,
		Message: "Version retrieved successfully",
	}, nil
}

func (s *PgCtldService) InitDataDir(ctx context.Context, req *pb.InitDataDirRequest) (*pb.InitDataDirResponse, error) {
	s.logger.Info("gRPC InitDataDir request", "data_dir", req.DataDir)

	dataDir := req.DataDir
	if dataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	// Check if already initialized
	if isDataDirInitialized(dataDir) {
		return &pb.InitDataDirResponse{
			Message: "Data directory is already initialized",
		}, nil
	}

	// Initialize data directory
	if err := initializeDataDir(dataDir); err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	return &pb.InitDataDirResponse{
		Message: "Data directory initialized successfully",
	}, nil
}
