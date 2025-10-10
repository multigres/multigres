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
	"log/slog"
	"os"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/toporeg"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/multipooler/poolerserver"
	"github.com/multigres/multigres/go/servenv"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

var (
	pgctldAddr               string
	cell                     string
	database                 string
	tableGroup               string
	serviceID                string
	socketFilePath           string
	poolerDir                string
	pgPort                   int
	heartbeatIntervalMs      int

	ts     topo.Store
	logger *slog.Logger

	tr *toporeg.TopoReg
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
	fs.IntVar(&heartbeatIntervalMs, "heartbeat-interval-milliseconds", 1000, "interval in milliseconds between heartbeat writes")
}

// Init initializes the multipooler. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func Init() {
	logger = servenv.GetLogger()
	ts = topo.Open()

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
		logger.Error("database is required")
		os.Exit(1)
	}

	if tableGroup == "" {
		logger.Error("table group is required")
		os.Exit(1)
	}
	// Create MultiPooler instance for topo registration
	multipooler := topo.NewMultiPooler(serviceID, cell, servenv.Hostname, tableGroup)
	multipooler.PortMap["grpc"] = int32(servenv.GRPCPort())
	multipooler.PortMap["http"] = int32(servenv.HTTPPort())
	multipooler.Database = database
	multipooler.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING

	// Initialize the MultiPoolerManager (following Vitess tm_init.go pattern)
	logger.Info("Initializing MultiPoolerManager")
	poolerManager := manager.NewMultiPoolerManager(logger, &manager.Config{
		SocketFilePath:      socketFilePath,
		PoolerDir:           poolerDir,
		PgPort:              pgPort,
		Database:            database,
		TopoClient:          ts,
		ServiceID:           multipooler.Id,
		HeartbeatIntervalMs: heartbeatIntervalMs,
	})

	// Start the MultiPoolerManager
	poolerManager.Start()

	// Initialize and start the MultiPooler
	pooler := poolerserver.NewMultiPooler(logger, &manager.Config{
		SocketFilePath:      socketFilePath,
		PoolerDir:           poolerDir,
		PgPort:              pgPort,
		Database:            database,
		TopoClient:          ts,
		ServiceID:           multipooler.Id,
		HeartbeatIntervalMs: heartbeatIntervalMs,
	})
	pooler.Start()

	servenv.OnRun(
		func() {
			registerFunc := func(ctx context.Context) error {
				return ts.RegisterMultiPooler(ctx, multipooler, true /* allowUpdate */)
			}
			// For poolers, we don't un-register them on shutdown (they are persistent component)
			// If they are actually deleted, they need to be cleaned up outside the lifecycle of starting / stopping.
			unregisterFunc := func(ctx context.Context) error {
				_, err := ts.UpdateMultiPoolerFields(ctx, multipooler.Id,
					func(mp *clustermetadatapb.MultiPooler) error {
						mp.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
						return nil
					})
				return err
			}

			tr = toporeg.Register(
				registerFunc,
				unregisterFunc,
				func(s string) { serverStatus.InitError = s }, /* alarm */
			)
		},
	)
}

func Shutdown() {
	logger.Info("multipooler shutting down")
	tr.Unregister()
	ts.Close()
}
