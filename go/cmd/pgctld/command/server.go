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
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	pb "github.com/multigres/multigres/go/pb/pgctldservice"
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

	// Use the shared start function with detailed result
	result, err := StartPostgreSQLWithResult(config)
	if err != nil {
		return nil, fmt.Errorf("failed to start PostgreSQL: %w", err)
	}

	return &pb.StartResponse{
		Pid:     int32(result.PID),
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Stop(ctx context.Context, req *pb.StopRequest) (*pb.StopResponse, error) {
	s.logger.Info("gRPC Stop request", "data_dir", req.DataDir, "mode", req.Mode)

	// Create config from request parameters
	config := NewPostgresConfigFromStopRequest(req)

	// Use the shared stop function with detailed result
	result, err := StopPostgreSQLWithResult(config, req.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to stop PostgreSQL: %w", err)
	}

	return &pb.StopResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Restart(ctx context.Context, req *pb.RestartRequest) (*pb.RestartResponse, error) {
	s.logger.Info("gRPC Restart request", "data_dir", req.DataDir, "mode", req.Mode)

	// Create config from request parameters
	config := NewPostgresConfigFromDefaults()
	if req.DataDir != "" {
		config.DataDir = req.DataDir
	}
	if req.Port > 0 {
		config.Port = int(req.Port)
	}
	if req.SocketDir != "" {
		config.SocketDir = req.SocketDir
	}
	if req.ConfigFile != "" {
		config.ConfigFile = req.ConfigFile
	}
	if req.Timeout > 0 {
		config.Timeout = int(req.Timeout)
	}

	// Use the shared restart function with detailed result
	result, err := RestartPostgreSQLWithResult(config, req.Mode)
	if err != nil {
		return nil, fmt.Errorf("failed to restart PostgreSQL: %w", err)
	}

	return &pb.RestartResponse{
		Pid:     int32(result.PID),
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) ReloadConfig(ctx context.Context, req *pb.ReloadConfigRequest) (*pb.ReloadConfigResponse, error) {
	s.logger.Info("gRPC ReloadConfig request", "data_dir", req.DataDir)

	// Create config from request parameters
	config := NewPostgresConfigFromDefaults()
	if req.DataDir != "" {
		config.DataDir = req.DataDir
	}

	// Use the shared reload function with detailed result
	result, err := ReloadPostgreSQLConfigWithResult(config)
	if err != nil {
		return nil, fmt.Errorf("failed to reload PostgreSQL configuration: %w", err)
	}

	return &pb.ReloadConfigResponse{
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	s.logger.Debug("gRPC Status request", "data_dir", req.DataDir)

	// Create config from request parameters
	config := NewPostgresConfigFromStatusRequest(req)

	// Use the shared status function with detailed result
	result, err := GetStatusWithResult(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}

	// Convert status string to protobuf enum
	var status pb.ServerStatus
	switch result.Status {
	case "NOT_INITIALIZED":
		status = pb.ServerStatus_NOT_INITIALIZED
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

	// Use the shared version function with detailed result
	result, err := GetVersionWithResult(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	return &pb.VersionResponse{
		Version: result.Version,
		Message: result.Message,
	}, nil
}

func (s *PgCtldService) InitDataDir(ctx context.Context, req *pb.InitDataDirRequest) (*pb.InitDataDirResponse, error) {
	s.logger.Info("gRPC InitDataDir request", "data_dir", req.DataDir)

	// Create config from request parameters
	config := NewPostgresConfigFromDefaults()
	if req.DataDir != "" {
		config.DataDir = req.DataDir
	}

	// Use the shared init function with detailed result
	result, err := InitDataDirWithResult(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	return &pb.InitDataDirResponse{
		Message: result.Message,
	}, nil
}
