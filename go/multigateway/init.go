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

// Package multigateway provides multigateway functionality.
package multigateway

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/netutil"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/timertools"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

var (
	cell      string
	serviceID string

	ts     topo.Store
	logger *slog.Logger

	// multigatewayID stores the ID for deregistration during shutdown
	multigatewayID *clustermetadatapb.ID

	serverctx    context.Context
	servercancel context.CancelFunc
	serverwg     sync.WaitGroup
)

// Init initializes the multigateway. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func Init() {
	logger = servenv.GetLogger()
	ts = topo.Open()

	serverctx, servercancel = context.WithCancel(context.Background())
	// This doen't change
	serverStatus.Cell = cell

	logger.Info("multigateway starting up",
		"cell", cell,
		"http_port", servenv.HTTPPort(),
		"grpc_port", servenv.GRPCPort(),
	)

	// Register with topology service
	hostname, err := netutil.FullyQualifiedHostname()
	if err != nil {
		logger.Warn("Failed to get fully qualified hostname, falling back to simple hostname", "error", err)
		hostname, err = os.Hostname()
		if err != nil {
			serverStatus.InitError = append(serverStatus.InitError, fmt.Sprintf("Failed to get hostname: %v", err))
			logger.Error("Failed to get hostname", "error", err)
			return
		}
	}

	// Create MultiGateway instance for topo registration
	multigateway := topo.NewMultiGateway(serviceID, cell, hostname)
	multigateway.PortMap["grpc"] = int32(servenv.GRPCPort())
	multigateway.PortMap["http"] = int32(servenv.HTTPPort())

	// Store ID for deregistration during shutdown
	multigatewayID = multigateway.Id

	// Validate cell
	if err := checkCellFlags(); err != nil {
		serverStatus.InitError = append(serverStatus.InitError, fmt.Sprintf("Failed to validate cell: %v", err))
		logger.Error("Failed to validate cell", "error", err)
	}

	// Publish in topo
	topoPublish(multigateway)

	// Start pooler discovery
	poolerDiscovery = NewPoolerDiscovery(context.Background(), ts, cell, logger)
	poolerDiscovery.Start()
	logger.Info("Pooler discovery started with topology watch", "cell", cell)
}

// checkCellFlags validates the cell flag against available cells
// in the topology. This function does not retry because this gets
// indirectly retried when we publish the gateway.
func checkCellFlags() error {
	if cell == "" {
		return fmt.Errorf("cell flag must be set")
	}

	// Create context with timeout for topology operations
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Get all known cells from topology.
	cellsInTopo, err := ts.GetCellNames(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cells from topology: %w", err)
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

func topoPublish(multigateway *clustermetadatapb.MultiGateway) {
	// Register with topology
	ctx, cancel := context.WithTimeout(serverctx, 10*time.Second)
	defer cancel()

	if err := ts.InitMultiGateway(ctx, multigateway, true); err == nil {
		logger.Info("Successfully registered multigateway with topology", "id", multigateway.Id)
		return
	} else {
		serverStatus.InitError = append(serverStatus.InitError, fmt.Sprintf("Failed to register multigateway with topology: %v", err))
		logger.Error("Failed to register multigateway with topology", "error", err)
	}
	serverwg.Go(func() {
		ticker := timertools.NewBackoffTicker(1*time.Second, 30*time.Second)
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(serverctx, 10*time.Second)
				if err := ts.InitMultiGateway(ctx, multigateway, true); err == nil {
					logger.Info("Successfully registered multigateway with topology", "id", multigateway.Id)
					// This means all previous errors have been resolved.
					serverStatus.InitError = nil
					cancel()
					return
				}
				cancel()
			case <-serverctx.Done():
				return
			}
		}
	})
}

func Shutdown() {
	logger := servenv.GetLogger()
	logger.Info("multigateway shutting down")

	// Stop pooler discovery
	if poolerDiscovery != nil {
		poolerDiscovery.Stop()
		logger.Info("Pooler discovery stopped")
	}

	// Stop any lingering initializations
	servercancel()
	serverwg.Wait()

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
	ts.Close()
}

// Register flags that are specific to multigateway.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&cell, "cell", cell, "cell to use")
	fs.StringVar(&serviceID, "service-id", "", "optional service ID (if empty, a random ID will be generated)")
}
