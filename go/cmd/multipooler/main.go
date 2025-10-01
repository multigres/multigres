// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// multipooler provides connection pooling and communicates with pgctld via gRPC
// to serve queries from multigateway instances.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multipooler/server"
	"github.com/multigres/multigres/go/netutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

// CreateMultiPoolerCommand creates a cobra command with a MultiPooler instance and registers its flags
func CreateMultiPoolerCommand() (*cobra.Command, *server.MultiPooler) {
	mp := server.NewMultiPooler()

	cmd := &cobra.Command{
		Use:     "multipooler",
		Short:   "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Long:    "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args, mp)
		},
	}

	mp.RegisterFlags(cmd.Flags())

	return cmd, mp
}

// CheckCellFlags validates the cell flag against available cells in the topology.
// It helps avoid strange behaviors when multipooler runs but actually does not work
// due to referencing non-existent cells.
func CheckCellFlags(ts topo.Store, cell string) error {
	if ts == nil {
		return fmt.Errorf("topo server cannot be nil")
	}

	// Validate cell flag is set
	if cell == "" {
		return fmt.Errorf("cell flag must be set")
	}

	// Create context with timeout for topology operations
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Get all known cells from topology with timeout
	cellsInTopo, err := ts.GetCellNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cells from topology (timeout after 2s): %w", err)
	}
	if len(cellsInTopo) == 0 {
		return fmt.Errorf("topo server should have at least one cell configured")
	}

	// Check if the specified cell exists in topology
	hasCell := slices.Contains(cellsInTopo, cell)
	if !hasCell {
		return fmt.Errorf("cell '%s' does not exist in topology. Available cells: [%s]",
			cell, strings.Join(cellsInTopo, ", "))
	}

	return nil
}

func main() {
	cmd, _ := CreateMultiPoolerCommand()
	servenv.RegisterServiceCmd(cmd)

	if err := cmd.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string, mp *server.MultiPooler) error {
	// Validate required flags first, before initializing service environment
	if mp.Database.Get() == "" {
		return fmt.Errorf("--database flag is required")
	}
	if mp.TableGroup.Get() == "" {
		return fmt.Errorf("--table-group flag is required")
	}

	servenv.Init()

	// Get the configured logger
	logger := servenv.GetLogger()

	// Ensure we open the topo before we start the context, so that the
	// defer that closes the topo runs after cancelling the context.
	// This ensures that we've properly closed things like the watchers
	// at that point.
	ts := topo.Open()
	defer func() { _ = ts.Close() }()

	// Validate cell configuration early to fail fast if misconfigured
	if err := CheckCellFlags(ts, mp.Cell.Get()); err != nil {
		logger.Error("Cell validation failed", "error", err)
		return fmt.Errorf("cell validation failed: %w", err)
	}
	logger.Info("Cell validation passed", "cell", mp.Cell)

	servenv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multipooler starting up",
			"pgctld_addr", mp.PgctldAddr,
			"cell", mp.Cell,
			"database", mp.Database,
			"table_group", mp.TableGroup,
			"socket_file_path", mp.SocketFilePath,
			"pooler_dir", mp.PoolerDir,
			"pg_port", mp.PgPort,
			"http_port", servenv.HTTPPort(),
			"grpc_port", mp.GrpcServer.Port(),
		)

		// Register multipooler gRPC service with servenv's GRPCServer
		if mp.GrpcServer.CheckServiceMap("pooler") {
			mp.PoolerServer = server.NewMultiPoolerServer(logger, &server.Config{
				SocketFilePath: mp.SocketFilePath.Get(),
				PoolerDir:      mp.PoolerDir.Get(),
				PgPort:         mp.PgPort.Get(),
				Database:       mp.Database.Get(),
			})
			mp.PoolerServer.RegisterWithGRPCServer(mp.GrpcServer.Server)
			logger.Info("MultiPooler gRPC service registered with servenv")
		}

		// Register with topology service
		hostname, err := netutil.FullyQualifiedHostname()
		if err != nil {
			logger.Warn("Failed to get fully qualified hostname, falling back to simple hostname", "error", err)
			hostname, err = os.Hostname()
			if err != nil {
				logger.Error("Failed to get hostname", "error", err)
				return
			}
		}

		// Create MultiPooler instance for topo registration
		multipooler := topo.NewMultiPooler(mp.ServiceID.Get(), mp.Cell.Get(), hostname, mp.TableGroup.Get())
		multipooler.PortMap["grpc"] = int32(mp.GrpcServer.Port())
		multipooler.PortMap["http"] = int32(servenv.HTTPPort())
		multipooler.Database = mp.Database.Get()

		if mp.ServiceID.Get() == "" {
			mp.ServiceID.Set(multipooler.GetId().GetName())
		}

		// Store ID for deregistration during shutdown
		mp.MultipoolerID = multipooler.Id

		// Register with topology
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ts.InitMultiPooler(ctx, multipooler, true); err != nil {
			logger.Error("Failed to register multipooler with topology", "error", err)
		} else {
			logger.Info("Successfully registered multipooler with topology", "id", multipooler.Id)
		}

		// TEMPORARY: Add a demo HTTP endpoint for testing - this will be removed later
		servenv.HTTPHandleFunc("/discovery/status", getHandleStatusEndpoint(mp))
		logger.Info("TEMPORARY: Discovery HTTP endpoint available at /discovery/status (for testing only)")
	})
	servenv.OnClose(func() {
		logger.Info("multipooler shutting down")

		// Deregister from topology service
		if mp.MultipoolerID != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := ts.DeleteMultiPooler(ctx, mp.MultipoolerID); err != nil {
				logger.Error("Failed to deregister multipooler from topology", "error", err, "id", mp.MultipoolerID)
			} else {
				logger.Info("Successfully deregistered multipooler from topology", "id", mp.MultipoolerID)
			}
		}
	})
	// TODO: Initialize gRPC connection to pgctld
	// TODO: Setup health check endpoint
	servenv.RunDefault(mp.GrpcServer)

	return nil
}

// StatusResponse represents the response from the temporary status endpoint
// TEMPORARY: This is only for testing and will be removed later
type StatusResponse struct {
	ServiceType    string                `json:"service_type"`
	Cell           string                `json:"cell"`
	Database       string                `json:"database"`
	TableGroup     string                `json:"table_group"`
	ServiceID      string                `json:"service_id"`
	ID             *clustermetadatapb.ID `json:"id"`
	PgctldAddr     string                `json:"pgctld_addr"`
	SocketFilePath string                `json:"socket_file_path"`
	Status         string                `json:"status"`
	Message        string                `json:"message"`
}

// getHandleStatusEndpoint handles the temporary HTTP endpoint that shows multipooler status
// TEMPORARY: This is only for testing and will be removed later
func getHandleStatusEndpoint(mp *server.MultiPooler) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		response := StatusResponse{
			ServiceType:    "multipooler",
			Cell:           mp.Cell.Get(),
			Database:       mp.Database.Get(),
			TableGroup:     mp.TableGroup.Get(),
			ServiceID:      mp.ServiceID.Get(),
			ID:             mp.MultipoolerID,
			PgctldAddr:     mp.PgctldAddr.Get(),
			SocketFilePath: mp.SocketFilePath.Get(),
			Status:         "running",
			Message:        "TEMPORARY: This endpoint is for testing only and will be removed",
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
	}
}
