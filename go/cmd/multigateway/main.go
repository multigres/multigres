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
	"slices"
	"strings"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/netutil"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"

	"github.com/spf13/cobra"
)

type MultiGateway struct {
	cell viperutil.Value[string]
	// serviceID string
	serviceID viperutil.Value[string]
	// multigatewayID stores the ID for deregistration during shutdown
	multigatewayID *clustermetadatapb.ID
	// poolerDiscovery handles discovery of multipoolers
	poolerDiscovery *PoolerDiscovery
	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer
	// senv is the serving environment
	senv *servenv.ServEnv
}

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
	hasCell := slices.Contains(cellsInTopo, cell)
	if !hasCell {
		return fmt.Errorf("cell '%s' does not exist in topology. Available cells: [%s]",
			cell, strings.Join(cellsInTopo, ", "))
	}

	return nil
}

func main() {
	mg := &MultiGateway{
		cell: viperutil.Configure("cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
		serviceID: viperutil.Configure("service-id", viperutil.Options[string]{
			Default:  "",
			FlagName: "service-id",
			Dynamic:  false,
			EnvVars:  []string{"MT_SERVICE_ID"},
		}),
		grpcServer: servenv.NewGrpcServer(),
		senv:       servenv.NewServEnv(),
	}

	main := &cobra.Command{
		Use:     "multigateway",
		Short:   "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Long:    "Multigateway is a stateless proxy responsible for accepting requests from applications and routing them to the appropriate multipooler server(s) for query execution. It speaks both the PostgresSQL Protocol and a gRPC protocol.",
		Args:    cobra.NoArgs,
		PreRunE: servenv.CobraPreRunE,
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, args, mg)
		},
	}

	main.Flags().String("cell", mg.cell.Default(), "cell to use")
	main.Flags().String("service-id", mg.serviceID.Default(), "optional service ID (if empty, a random ID will be generated)")
	viperutil.BindFlags(main.Flags(),
		mg.cell,
		mg.serviceID,
	)
	mg.senv.RegisterFlags(main.Flags())
	mg.grpcServer.RegisterFlags(main.Flags())

	if err := main.Execute(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string, mg *MultiGateway) error {
	mg.senv.Init()

	logger := mg.senv.GetLogger()

	ts := topo.Open()
	defer func() { _ = ts.Close() }()

	// Validate cell configuration early to fail fast if misconfigured
	if err := CheckCellFlags(ts, mg.cell.Get()); err != nil {
		logger.Error("Cell validation failed", "error", err)
		return fmt.Errorf("cell validation failed: %w", err)
	}
	logger.Info("Cell validation passed", "cell", mg.cell)

	mg.senv.OnRun(func() {
		// Flags are parsed now.
		logger.Info("multigateway starting up",
			"cell", mg.cell,
			"http_port", mg.senv.HTTPPort.Get(),
			"grpc_port", mg.grpcServer.Port(),
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
		multigateway := topo.NewMultiGateway(mg.serviceID.Get(), mg.cell.Get(), hostname)
		multigateway.PortMap["grpc"] = int32(mg.grpcServer.Port())
		multigateway.PortMap["http"] = int32(mg.senv.HTTPPort.Get())

		if mg.serviceID.Get() == "" {
			mg.serviceID.Set(multigateway.GetId().GetName())
		}

		// Store ID for deregistration during shutdown
		mg.multigatewayID = multigateway.Id

		// Register with topology
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := ts.InitMultiGateway(ctx, multigateway, true); err != nil {
			logger.Error("Failed to register multigateway with topology", "error", err)
		} else {
			logger.Info("Successfully registered multigateway with topology", "id", multigateway.Id)
		}

		// Start pooler discovery
		mg.poolerDiscovery = NewPoolerDiscovery(context.Background(), ts, mg.cell.Get(), logger)
		mg.poolerDiscovery.Start()
		logger.Info("Pooler discovery started with topology watch", "cell", mg.cell)

		// Add a demo HTTP endpoint to show discovered poolers
		mg.senv.HTTPHandleFunc("/discovery/poolers", getHandlePoolersEndpoint(mg))
		logger.Info("Discovery HTTP endpoint available at /discovery/poolers")
	})
	mg.senv.OnClose(func() {
		logger.Info("multigateway shutting down")

		// Stop pooler discovery
		if mg.poolerDiscovery != nil {
			mg.poolerDiscovery.Stop()
			logger.Info("Pooler discovery stopped")
		}

		// Deregister from topology service
		if mg.multigatewayID != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := ts.DeleteMultiGateway(ctx, mg.multigatewayID); err != nil {
				logger.Error("Failed to deregister multigateway from topology", "error", err, "id", mg.multigatewayID)
			} else {
				logger.Info("Successfully deregistered multigateway from topology", "id", mg.multigatewayID)
			}
		}
	})
	mg.senv.RunDefault(mg.grpcServer)

	return nil
}

// getHandlePoolersEndpoint handles the HTTP endpoint that shows discovered poolers
func getHandlePoolersEndpoint(mg *MultiGateway) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if mg.poolerDiscovery == nil {
			http.Error(w, "Pooler discovery not initialized", http.StatusServiceUnavailable)
			return
		}

		response := DiscoveryResponse{
			Cell:        mg.cell.Get(),
			PoolerCount: mg.poolerDiscovery.PoolerCount(),
			LastRefresh: mg.poolerDiscovery.LastRefresh(),
			PoolerNames: mg.poolerDiscovery.GetPoolersName(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}
	}
}

// DiscoveryResponse represents the response from the discovery endpoint
type DiscoveryResponse struct {
	Cell        string    `json:"cell"`
	PoolerCount int       `json:"pooler_count"`
	LastRefresh time.Time `json:"last_refresh"`
	PoolerNames []string  `json:"pooler_names"`
}
