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

	// Use request parameters or fall back to viper defaults
	dataDir := req.DataDir
	if dataDir == "" {
		dataDir = viper.GetString("data-dir")
	}

	if dataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	// Set up temporary viper config for this request
	origDataDir := viper.GetString("data-dir")
	origPort := viper.GetInt("pg-port")
	origSocketDir := viper.GetString("socket-dir")
	origConfigFile := viper.GetString("config-file")

	defer func() {
		viper.Set("data-dir", origDataDir)
		viper.Set("pg-port", origPort)
		viper.Set("socket-dir", origSocketDir)
		viper.Set("config-file", origConfigFile)
	}()

	viper.Set("data-dir", dataDir)
	if req.Port > 0 {
		viper.Set("pg-port", req.Port)
	}
	if req.SocketDir != "" {
		viper.Set("socket-dir", req.SocketDir)
	}
	if req.ConfigFile != "" {
		viper.Set("config-file", req.ConfigFile)
	}

	// Check if data directory is initialized
	if !isDataDirInitialized(dataDir) {
		s.logger.Info("Data directory not initialized, running initdb", "data_dir", dataDir)
		if err := initializeDataDir(dataDir); err != nil {
			return nil, fmt.Errorf("failed to initialize data directory: %w", err)
		}
	}

	// Check if PostgreSQL is already running
	if isPostgreSQLRunning(dataDir) {
		return &pb.StartResponse{
			Pid:     0,
			Message: "PostgreSQL is already running",
		}, nil
	}

	// Start PostgreSQL
	if err := startPostgreSQL(dataDir); err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	// Wait for server to be ready
	if err := waitForPostgreSQL(); err != nil {
		return nil, fmt.Errorf("PostgreSQL failed to become ready: %w", err)
	}

	// Get PID
	var pid int32
	if pidValue, err := readPostmasterPID(dataDir); err == nil {
		pid = int32(pidValue)
	}

	return &pb.StartResponse{
		Pid:     pid,
		Message: "PostgreSQL server started successfully",
	}, nil
}

func (s *PgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.logger.Info("gRPC Stop request", "data_dir", req.DataDir, "mode", req.Mode)

	dataDir := req.DataDir
	if dataDir == "" {
		dataDir = viper.GetString("data-dir")
	}

	if dataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	mode := req.Mode
	if mode == "" {
		mode = "fast"
	}

	timeout := int(req.Timeout)
	if timeout <= 0 {
		timeout = viper.GetInt("timeout")
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(dataDir) {
		return &pb.StopResponse{
			Message: "PostgreSQL is not running",
		}, nil
	}

	// Stop PostgreSQL
	if err := stopPostgreSQL(dataDir, mode, timeout); err != nil {
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

	dataDir := req.DataDir
	if dataDir == "" {
		dataDir = viper.GetString("data-dir")
	}

	if dataDir == "" {
		return nil, fmt.Errorf("data-dir is required")
	}

	// Check if data directory is initialized
	if !isDataDirInitialized(dataDir) {
		return &pb.StatusResponse{
			Status:  pb.ServerStatus_NOT_INITIALIZED,
			DataDir: dataDir,
			Port:    int32(viper.GetInt("pg-port")),
			Host:    viper.GetString("pg-host"),
			Message: "Data directory is not initialized",
		}, nil
	}

	// Check if PostgreSQL is running
	if !isPostgreSQLRunning(dataDir) {
		return &pb.StatusResponse{
			Status:  pb.ServerStatus_STOPPED,
			DataDir: dataDir,
			Port:    int32(viper.GetInt("pg-port")),
			Host:    viper.GetString("pg-host"),
			Ready:   false,
			Message: "PostgreSQL server is stopped",
		}, nil
	}

	// Get PID
	var pid int32
	if pidValue, err := readPostmasterPID(dataDir); err == nil {
		pid = int32(pidValue)
	}

	// Check if server is ready
	ready := isServerReady()

	// Get version
	version := getServerVersion()

	// Get uptime (approximate based on pidfile mtime)
	var uptimeSeconds int64
	pidFile := fmt.Sprintf("%s/postmaster.pid", dataDir)
	if stat, err := os.Stat(pidFile); err == nil {
		uptimeSeconds = int64(time.Now().Unix() - stat.ModTime().Unix())
	}

	return &pb.StatusResponse{
		Status:        pb.ServerStatus_RUNNING,
		Pid:           pid,
		Version:       version,
		UptimeSeconds: uptimeSeconds,
		DataDir:       dataDir,
		Port:          int32(viper.GetInt("pg-port")),
		Host:          viper.GetString("pg-host"),
		Ready:         ready,
		Message:       "PostgreSQL server is running",
	}, nil
}

func (s *PgCtldService) Version(ctx context.Context, req *pb.VersionRequest) (*pb.VersionResponse, error) {
	s.logger.Debug("gRPC Version request")

	// Set up temporary viper config for this request
	origHost := viper.GetString("pg-host")
	origPort := viper.GetInt("pg-port")
	origDatabase := viper.GetString("pg-database")
	origUser := viper.GetString("pg-user")

	defer func() {
		viper.Set("pg-host", origHost)
		viper.Set("pg-port", origPort)
		viper.Set("pg-database", origDatabase)
		viper.Set("pg-user", origUser)
	}()

	if req.Host != "" {
		viper.Set("pg-host", req.Host)
	}
	if req.Port > 0 {
		viper.Set("pg-port", req.Port)
	}
	if req.Database != "" {
		viper.Set("pg-database", req.Database)
	}
	if req.User != "" {
		viper.Set("pg-user", req.User)
	}

	version := getServerVersion()

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
