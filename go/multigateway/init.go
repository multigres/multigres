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

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/toporeg"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"
)

type MultiGateway struct {
	cell viperutil.Value[string]
	// serviceID string
	serviceID viperutil.Value[string]
	// poolerDiscovery handles discovery of multipoolers
	poolerDiscovery *PoolerDiscovery
	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer
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
	viperutil.BindFlags(fs,
		mg.cell,
		mg.serviceID,
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

	logger.Info("multigateway starting up",
		"cell", mg.cell.Get(),
		"service_id", mg.serviceID.Get(),
		"http_port", mg.senv.HTTPPort.Get(),
		"grpc_port", mg.grpcServer.Port(),
	)

	// Create MultiGateway instance for topo registration
	multigateway := topo.NewMultiGateway(mg.serviceID.Get(), mg.cell.Get(), mg.senv.Hostname.Get())
	multigateway.PortMap["grpc"] = int32(mg.grpcServer.Port())
	multigateway.PortMap["http"] = int32(mg.senv.HTTPPort.Get())

	mg.tr = toporeg.Register(
		func(ctx context.Context) error { return mg.ts.RegisterMultiGateway(ctx, multigateway, true) },
		func(ctx context.Context) error { return mg.ts.UnregisterMultiGateway(ctx, multigateway.Id) },
		func(s string) { mg.serverStatus.InitError = s },
	)

	// Start pooler discovery
	mg.poolerDiscovery = NewPoolerDiscovery(context.Background(), mg.ts, mg.cell.Get(), logger)
	mg.poolerDiscovery.Start()
	logger.Info("Pooler discovery started with topology watch", "cell", mg.cell.Get())

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

	// Stop pooler discovery
	if mg.poolerDiscovery != nil {
		mg.poolerDiscovery.Stop()
		mg.senv.GetLogger().Info("Pooler discovery stopped")
	}

	mg.tr.Unregister()
	mg.ts.Close()
}
