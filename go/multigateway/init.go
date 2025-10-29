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

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/toporeg"
	"github.com/multigres/multigres/go/multigateway/executor"
	"github.com/multigres/multigres/go/multigateway/poolergateway"
	"github.com/multigres/multigres/go/multigateway/scatterconn"
	"github.com/multigres/multigres/go/pgprotocol/server"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"
)

type MultiGateway struct {
	cell viperutil.Value[string]
	// serviceID string
	serviceID viperutil.Value[string]
	// pgPort is the PostgreSQL protocol listen port
	pgPort viperutil.Value[int]
	// poolerDiscovery handles discovery of multipoolers
	poolerDiscovery *PoolerDiscovery
	// poolerGateway manages connections to poolers
	poolerGateway *poolergateway.PoolerGateway
	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer
	// pgListener is the PostgreSQL protocol listener
	pgListener *server.Listener
	// scatterConn coordinates query execution across poolers
	scatterConn *scatterconn.ScatterConn
	// executor handles query execution and routing
	executor *executor.Executor
	// senv is the serving environment
	senv *servenv.ServEnv
	// topoConfig holds topology configuration
	topoConfig   *topo.TopoConfig
	ts           topo.Store
	tr           *toporeg.TopoReg
	serverStatus Status
}

func NewMultiGateway() *MultiGateway {
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
		pgPort: viperutil.Configure("pg-port", viperutil.Options[int]{
			Default:  5432,
			FlagName: "pg-port",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_PORT"},
		}),
		grpcServer: servenv.NewGrpcServer(),
		senv:       servenv.NewServEnv(),
		topoConfig: topo.NewTopoConfig(),
		serverStatus: Status{
			Title: "Multigateway",
			Links: []Link{
				{"Config", "Server configuration details", "/config"},
				{"Live", "URL for liveness check", "/live"},
				{"Ready", "URL for readiness check", "/ready"},
			},
		},
	}

	return mg
}

func (mg *MultiGateway) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("cell", mg.cell.Default(), "cell to use")
	fs.String("service-id", mg.serviceID.Default(), "optional service ID (if empty, a random ID will be generated)")
	fs.Int("pg-port", mg.pgPort.Default(), "PostgreSQL protocol listen port")
	viperutil.BindFlags(fs,
		mg.cell,
		mg.serviceID,
		mg.pgPort,
	)
	mg.senv.RegisterFlags(fs)
	mg.grpcServer.RegisterFlags(fs)
	mg.topoConfig.RegisterFlags(fs)
}

// Init initializes the multigateway. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func (mg *MultiGateway) Init() {
	mg.senv.Init()
	logger := mg.senv.GetLogger()

	mg.ts = mg.topoConfig.Open()

	// This doesn't change
	mg.serverStatus.Cell = mg.cell.Get()
	mg.serverStatus.ServiceID = mg.serviceID.Get()

	// Start pooler discovery first
	mg.poolerDiscovery = NewPoolerDiscovery(context.Background(), mg.ts, mg.cell.Get(), logger)
	mg.poolerDiscovery.Start()
	logger.Info("Pooler discovery started with topology watch", "cell", mg.cell.Get())

	// Initialize PoolerGateway for managing pooler connections
	mg.poolerGateway = poolergateway.NewPoolerGateway(mg.poolerDiscovery, logger)

	// Initialize ScatterConn for query coordination
	mg.scatterConn = scatterconn.NewScatterConn(mg.poolerGateway, logger)

	// Initialize the executor for query routing
	// Pass ScatterConn as the IExecute implementation
	mg.executor = executor.NewExecutor(mg.scatterConn, logger)

	// Create and start PostgreSQL protocol listener
	pgHandler := NewMultiGatewayHandler(mg)
	pgAddr := fmt.Sprintf("localhost:%d", mg.pgPort.Get())
	var err error
	mg.pgListener, err = server.NewListener(server.ListenerConfig{
		Address: pgAddr,
		Handler: pgHandler,
		Logger:  logger,
	})
	if err != nil {
		logger.Error("failed to create PostgreSQL listener", "error", err, "port", mg.pgPort.Get())
		panic(err)
	}

	// Start the PostgreSQL listener in a goroutine
	go func() {
		logger.Info("PostgreSQL listener starting", "port", mg.pgPort.Get())
		if err := mg.pgListener.Serve(); err != nil {
			logger.Error("PostgreSQL listener error", "error", err)
		}
	}()

	logger.Info("multigateway starting up",
		"cell", mg.cell.Get(),
		"service_id", mg.serviceID.Get(),
		"http_port", mg.senv.GetHTTPPort(),
		"grpc_port", mg.grpcServer.Port(),
		"pg_port", mg.pgPort.Get(),
	)

	// Create MultiGateway instance for topo registration
	multigateway := topo.NewMultiGateway(mg.serviceID.Get(), mg.cell.Get(), mg.senv.GetHostname())
	multigateway.PortMap["grpc"] = int32(mg.grpcServer.Port())
	multigateway.PortMap["http"] = int32(mg.senv.GetHTTPPort())
	multigateway.PortMap["pg"] = int32(mg.pgPort.Get())

	mg.tr = toporeg.Register(
		func(ctx context.Context) error { return mg.ts.RegisterMultiGateway(ctx, multigateway, true) },
		func(ctx context.Context) error { return mg.ts.UnregisterMultiGateway(ctx, multigateway.Id) },
		func(s string) { mg.serverStatus.InitError = s },
	)

	mg.senv.HTTPHandleFunc("/", mg.getHandleIndex())
	mg.senv.HTTPHandleFunc("/ready", mg.getHandleReady())

	mg.senv.OnClose(func() {
		mg.Shutdown()
	})
}

func (mg *MultiGateway) RunDefault() {
	mg.senv.RunDefault(mg.grpcServer)
}

func (mg *MultiGateway) CobraPreRunE(cmd *cobra.Command) error {
	return mg.senv.CobraPreRunE(cmd)
}

func (mg *MultiGateway) Shutdown() {
	mg.senv.GetLogger().Info("multigateway shutting down")

	// Stop PostgreSQL listener
	if mg.pgListener != nil {
		if err := mg.pgListener.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing PostgreSQL listener", "error", err)
		} else {
			mg.senv.GetLogger().Info("PostgreSQL listener stopped")
		}
	}

	// Close pooler gateway connections
	if mg.poolerGateway != nil {
		if err := mg.poolerGateway.Close(context.Background()); err != nil {
			mg.senv.GetLogger().Error("error closing pooler gateway", "error", err)
		} else {
			mg.senv.GetLogger().Info("Pooler gateway closed")
		}
	}

	// Stop pooler discovery
	if mg.poolerDiscovery != nil {
		mg.poolerDiscovery.Stop()
		mg.senv.GetLogger().Info("Pooler discovery stopped")
	}

	mg.tr.Unregister()
	mg.ts.Close()
}
