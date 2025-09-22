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
	"strings"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multipooler/server"
	"github.com/multigres/multigres/go/netutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	pgctldAddr     string
	cell           string
	database       string
	tableGroup     string
	serviceID      string
	socketFilePath string
	poolerDir      string
	pgPort         int
	// multipoolerID stores the ID for deregistration during shutdown
	multipoolerID *clustermetadatapb.ID
	// poolerServer holds the gRPC multipooler server instance
	poolerServer *server.MultiPoolerServer

	Main = &cobra.Command{
		Use:     "multipooler",
		Short:   "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Long:    "Multipooler provides connection pooling and communicates with pgctld via gRPC to serve queries from multigateway instances.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

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
	hasCell := false
	for _, v := range cellsInTopo {
		if v == cell {
			hasCell = true
			break
		}
	}
	if !hasCell {
		return fmt.Errorf("cell '%s' does not exist in topology. Available cells: [%s]",
			cell, strings.Join(cellsInTopo, ", "))
	}

	return nil
}

func main() {
	if err := Main.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Validate required flags first, before initializing service environment
	if database == "" {
		return fmt.Errorf("--database flag is required")
	}
	if tableGroup == "" {
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
	if err := CheckCellFlags(ts, cell); err != nil {
		logger.Error("Cell validation failed", "error", err)
		return fmt.Errorf("cell validation failed: %w", err)
	}
	logger.Info("Cell validation passed", "cell", cell)

	servenv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multipooler starting up",
			"pgctld_addr", pgctldAddr,
			"cell", cell,
			"database", database,
			"table_group", tableGroup,
			"socket_file_path", socketFilePath,
			"pooler_dir", poolerDir,
			"pg_port", pgPort,
			"http_port", servenv.HTTPPort(),
			"grpc_port", servenv.GRPCPort(),
		)

		// Register multipooler gRPC service with servenv's GRPCServer
		if servenv.GRPCCheckServiceMap("pooler") {
			poolerServer = server.NewMultiPoolerServer(logger, &server.Config{
				SocketFilePath: socketFilePath,
				PoolerDir:      poolerDir,
				PgPort:         pgPort,
				Database:       database,
			})
			poolerServer.RegisterWithGRPCServer(servenv.GRPCServer)
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
		multipooler := topo.NewMultiPooler(serviceID, cell, hostname, tableGroup)
		multipooler.PortMap["grpc"] = int32(servenv.GRPCPort())
		multipooler.PortMap["http"] = int32(servenv.HTTPPort())
		multipooler.Database = database

		// Store ID for deregistration during shutdown
		multipoolerID = multipooler.Id

		// Register with topology
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ts.InitMultiPooler(ctx, multipooler, true); err != nil {
			logger.Error("Failed to register multipooler with topology", "error", err)
		} else {
			logger.Info("Successfully registered multipooler with topology", "id", multipooler.Id)
		}

		// TEMPORARY: Add a demo HTTP endpoint for testing - this will be removed later
		servenv.HTTPHandleFunc("/discovery/status", handleStatusEndpoint)
		logger.Info("TEMPORARY: Discovery HTTP endpoint available at /discovery/status (for testing only)")
	})
	servenv.OnClose(func() {
		logger.Info("multipooler shutting down")

		// Deregister from topology service
		if multipoolerID != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := ts.DeleteMultiPooler(ctx, multipoolerID); err != nil {
				logger.Error("Failed to deregister multipooler from topology", "error", err, "id", multipoolerID)
			} else {
				logger.Info("Successfully deregistered multipooler from topology", "id", multipoolerID)
			}
		}
	})
	// TODO: Initialize gRPC connection to pgctld
	// TODO: Setup health check endpoint
	servenv.RunDefault()

	return nil
}

func init() {
	// Adds multipooler specific flags
	servenv.OnParseFor("multipooler", registerFlags)

	servenv.RegisterServiceCmd(Main)
	servenv.RegisterGRPCServerFlags()
}

func registerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&pgctldAddr, "pgctld-addr", "localhost:15200", "Address of pgctld gRPC service")
	fs.StringVar(&cell, "cell", "", "cell to use")
	fs.StringVar(&database, "database", "", "database name this multipooler serves (required)")
	fs.StringVar(&tableGroup, "table-group", "", "table group this multipooler serves (required)")
	fs.StringVar(&serviceID, "service-id", "", "optional service ID (if empty, a random ID will be generated)")
	fs.StringVar(&socketFilePath, "socket-file", "", "PostgreSQL Unix socket file path (if empty, TCP connection will be used)")
	fs.StringVar(&poolerDir, "pooler-dir", "", "pooler directory path (if empty, socket-file path will be used as-is)")
	fs.IntVar(&pgPort, "pg-port", 5432, "PostgreSQL port number")
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

// handleStatusEndpoint handles the temporary HTTP endpoint that shows multipooler status
// TEMPORARY: This is only for testing and will be removed later
func handleStatusEndpoint(w http.ResponseWriter, r *http.Request) {
	response := StatusResponse{
		ServiceType:    "multipooler",
		Cell:           cell,
		Database:       database,
		TableGroup:     tableGroup,
		ServiceID:      serviceID,
		ID:             multipoolerID,
		PgctldAddr:     pgctldAddr,
		SocketFilePath: socketFilePath,
		Status:         "running",
		Message:        "TEMPORARY: This endpoint is for testing only and will be removed",
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}
