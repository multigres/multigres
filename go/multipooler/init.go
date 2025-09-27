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

// Package multipooler provides multipooler functionality.
package multipooler

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
	"github.com/multigres/multigres/go/multipooler/server"
	"github.com/multigres/multigres/go/netutil"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/timertools"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
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

	ts     topo.Store
	logger *slog.Logger

	// multipoolerID stores the ID for deregistration during shutdown
	multipoolerID *clustermetadatapb.ID
	// poolerServer holds the gRPC multipooler server instance
	poolerServer *server.MultiPoolerServer

	serverctx    context.Context
	servercancel context.CancelFunc
	serverwg     sync.WaitGroup
)

func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&pgctldAddr, "pgctld-addr", "localhost:15200", "Address of pgctld gRPC service")
	fs.StringVar(&cell, "cell", "", "cell to use")
	fs.StringVar(&database, "database", "", "database name this multipooler serves (required)")
	fs.StringVar(&tableGroup, "table-group", "", "table group this multipooler serves (required)")
	fs.StringVar(&serviceID, "service-id", "", "optional service ID (if empty, a random ID will be generated)")
	fs.StringVar(&socketFilePath, "socket-file", "", "PostgreSQL Unix socket file path (if empty, TCP connection will be used)")
	fs.StringVar(&poolerDir, "pooler-dir", "", "pooler directory path (if empty, socket-file path will be used as-is)")
	fs.IntVar(&pgPort, "pg-port", 5432, "PostgreSQL port number")
}

// Init initializes the multipooler. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func Init() {
	logger = servenv.GetLogger()
	ts = topo.Open()
	serverctx, servercancel = context.WithCancel(context.Background())

	// This doen't change
	serverStatus.Cell = cell
	serverStatus.ServiceID = serviceID
	serverStatus.Database = database
	serverStatus.TableGroup = tableGroup
	serverStatus.PgctldAddr = pgctldAddr
	serverStatus.SocketFilePath = socketFilePath

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

	if database == "" {
		serverStatus.InitError = append(serverStatus.InitError, "database is required")
		logger.Error("database is required")
		return
	}

	if tableGroup == "" {
		serverStatus.InitError = append(serverStatus.InitError, "table group is required")
		logger.Error("table group is required")
		return
	}

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
			serverStatus.InitError = append(serverStatus.InitError, fmt.Sprintf("Failed to get hostname: %v", err))
			logger.Error("Failed to get hostname", "error", err)
			return
		}
	}

	// Validate cell
	if err := checkCellFlags(); err != nil {
		serverStatus.InitError = append(serverStatus.InitError, fmt.Sprintf("Failed to validate cell: %v", err))
		logger.Error("Failed to validate cell", "error", err)
	}

	// Create MultiPooler instance for topo registration
	multipooler := topo.NewMultiPooler(serviceID, cell, hostname, tableGroup)
	multipooler.PortMap["grpc"] = int32(servenv.GRPCPort())
	multipooler.PortMap["http"] = int32(servenv.HTTPPort())
	multipooler.Database = database

	// Store ID for deregistration during shutdown
	multipoolerID = multipooler.Id

	// Publish in topo
	topoPublish(multipooler)
}

// checkCellFlags validates the cell flag against available cells
// in the topology. This function does not retry because this gets
// indirectly retried when we publish the pooler.
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

func topoPublish(multipooler *clustermetadatapb.MultiPooler) {
	// Register with topology
	ctx, cancel := context.WithTimeout(serverctx, 10*time.Second)
	defer cancel()

	if err := ts.InitMultiPooler(ctx, multipooler, true); err != nil {
		logger.Info("Successfully registered multipooler with topology", "id", multipooler.Id)
		return
	} else {
		serverStatus.InitError = append(serverStatus.InitError, fmt.Sprintf("Failed to register multipooler with topology: %v", err))
		logger.Error("Failed to register multipooler with topology", "error", err)
	}
	serverwg.Go(func() {
		ticker := timertools.NewBackoffTicker(1*time.Second, 30*time.Second)
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(serverctx, 10*time.Second)
				if err := ts.InitMultiPooler(ctx, multipooler, true); err == nil {
					logger.Info("Successfully registered multipooler with topology", "id", multipooler.Id)
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
	logger.Info("multipooler shutting down")

	// Stop any lingering initializations
	servercancel()
	serverwg.Wait()

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
	ts.Close()
}
