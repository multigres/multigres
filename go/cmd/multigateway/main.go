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

// multigateway is the top-level proxy that masquerades as a PostgreSQL server,
// handling client connections and routing queries to multipooler instances.
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
	"github.com/multigres/multigres/go/netutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/postgres"
	"github.com/multigres/multigres/go/servenv"

	"github.com/spf13/cobra"
)

var (
	cell string
	// multigatewayID stores the ID for deregistration during shutdown
	multigatewayID *clustermetadatapb.ID
	// poolerDiscovery handles discovery of multipoolers
	poolerDiscovery *PoolerDiscovery
	// pgServer is the PostgreSQL protocol server
	pgServer *postgres.Server
	// pgPort is the PostgreSQL port to listen on
	pgPort int

	Main = &cobra.Command{
		Use:     "multigateway",
		Short:   "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Long:    "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE:    run,
	}
)

// CheckCellFlags validates the cell flag against available cells in the topology.
// It helps avoid strange behaviors when multigateway runs but actually does not work
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

	// Get all known cells from topology.
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

	servenv.Init()

	logger := servenv.GetLogger()

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
		logger.Info("multigateway starting up",
			"cell", cell,
			"http_port", servenv.HTTPPort(),
			"grpc_port", servenv.GRPCPort(),
			"pg_port", pgPort,
		)

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

		// Create MultiGateway instance for topo registration
		multigateway := topo.NewMultiGateway("", cell, hostname)
		multigateway.PortMap["grpc"] = int32(servenv.GRPCPort())
		multigateway.PortMap["http"] = int32(servenv.HTTPPort())

		// Store ID for deregistration during shutdown
		multigatewayID = multigateway.Id

		// Register with topology
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ts.InitMultiGateway(ctx, multigateway, true); err != nil {
			logger.Error("Failed to register multigateway with topology", "error", err)
		} else {
			logger.Info("Successfully registered multigateway with topology", "id", multigateway.Id)
		}

		// Start pooler discovery
		poolerDiscovery = NewPoolerDiscovery(context.Background(), ts, cell, logger)
		poolerDiscovery.Start()
		logger.Info("Pooler discovery started with topology watch", "cell", cell)

		// Add a demo HTTP endpoint to show discovered poolers
		servenv.HTTPHandleFunc("/discovery/poolers", handlePoolersEndpoint)
		logger.Info("Discovery HTTP endpoint available at /discovery/poolers")
		
		// Start PostgreSQL protocol server
		if pgPort > 0 {
			pgConfig := postgres.ServerConfig{
				Address:    fmt.Sprintf(":%d", pgPort),
				AuthMethod: "trust", // Start with trust auth for development
				Parameters: map[string]string{
					"server_version":     "15.0 (Multigres)",
					"server_encoding":    "UTF8",
					"client_encoding":    "UTF8",
					"DateStyle":          "ISO, MDY",
					"TimeZone":           "UTC",
					"integer_datetimes":  "on",
				},
				MaxConnections: 100,
			}
			
			// Create handler
			pgHandler := NewPostgresHandler(logger, poolerDiscovery)
			
			// Create server
			pgServer = postgres.NewServer(pgConfig, pgHandler, logger)
			
			// Start listening
			if err := pgServer.Listen(); err != nil {
				logger.Error("Failed to start PostgreSQL server", "error", err)
				return
			}
			
			// Start serving connections in a goroutine
			go func() {
				if err := pgServer.Serve(); err != nil {
					logger.Error("PostgreSQL server error", "error", err)
				}
			}()
			
			logger.Info("PostgreSQL server started", "port", pgPort)
		}
	})
	servenv.OnClose(func() {
		logger.Info("multigateway shutting down")

		// Stop PostgreSQL server
		if pgServer != nil {
			if err := pgServer.Close(); err != nil {
				logger.Error("Failed to stop PostgreSQL server", "error", err)
			} else {
				logger.Info("PostgreSQL server stopped")
			}
		}

		// Stop pooler discovery
		if poolerDiscovery != nil {
			poolerDiscovery.Stop()
			logger.Info("Pooler discovery stopped")
		}

		// Deregister from topology service
		if multigatewayID != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := ts.DeleteMultiGateway(ctx, multigatewayID); err != nil {
				logger.Error("Failed to deregister multigateway from topology", "error", err, "id", multigatewayID)
			} else {
				logger.Info("Successfully deregistered multigateway from topology", "id", multigatewayID)
			}
		}
	})
	servenv.RunDefault()

	return nil
}

func init() {
	servenv.RegisterServiceCmd(Main)

	// Adds multigateway specific flags
	Main.Flags().StringVar(&cell, "cell", cell, "cell to use")
	Main.Flags().IntVar(&pgPort, "pg-port", 5432, "PostgreSQL port to listen on (0 to disable)")
}

// DiscoveryResponse represents the response from the discovery endpoint
type DiscoveryResponse struct {
	Cell        string    `json:"cell"`
	PoolerCount int       `json:"pooler_count"`
	LastRefresh time.Time `json:"last_refresh"`
	PoolerNames []string  `json:"pooler_names"`
}

// handlePoolersEndpoint handles the HTTP endpoint that shows discovered poolers
func handlePoolersEndpoint(w http.ResponseWriter, r *http.Request) {
	if poolerDiscovery == nil {
		http.Error(w, "Pooler discovery not initialized", http.StatusServiceUnavailable)
		return
	}

	response := DiscoveryResponse{
		Cell:        cell,
		PoolerCount: poolerDiscovery.PoolerCount(),
		LastRefresh: poolerDiscovery.LastRefresh(),
		PoolerNames: poolerDiscovery.GetPoolersName(),
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}
