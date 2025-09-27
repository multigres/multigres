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
	"log/slog"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/topopublish"
	"github.com/multigres/multigres/go/servenv"
)

var (
	cell      string
	serviceID string

	ts     topo.Store
	logger *slog.Logger

	tp *topopublish.TopoPublisher
)

// Register flags that are specific to multigateway.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&cell, "cell", cell, "cell to use")
	fs.StringVar(&serviceID, "service-id", "", "optional service ID (if empty, a random ID will be generated)")
}

// Init initializes the multigateway. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func Init() {
	logger = servenv.GetLogger()
	ts = topo.Open()

	// This doesn't change
	serverStatus.Cell = cell
	serverStatus.ServiceID = serviceID

	logger.Info("multigateway starting up",
		"cell", cell,
		"service_id", serviceID,
		"http_port", servenv.HTTPPort(),
		"grpc_port", servenv.GRPCPort(),
	)

	// Create MultiGateway instance for topo registration
	multigateway := topo.NewMultiGateway(serviceID, cell, servenv.Hostname)
	multigateway.PortMap["grpc"] = int32(servenv.GRPCPort())
	multigateway.PortMap["http"] = int32(servenv.HTTPPort())

	tp = topopublish.Publish(
		func(ctx context.Context) error { return ts.InitMultiGateway(ctx, multigateway, true) },
		func(ctx context.Context) error { return ts.DeleteMultiGateway(ctx, multigateway.Id) },
		func(s string) { serverStatus.InitError = s },
	)

	// Start pooler discovery
	poolerDiscovery = NewPoolerDiscovery(context.Background(), ts, cell, logger)
	poolerDiscovery.Start()
	logger.Info("Pooler discovery started with topology watch", "cell", cell)
}

func Shutdown() {
	logger.Info("multigateway shutting down")

	// Stop pooler discovery
	if poolerDiscovery != nil {
		poolerDiscovery.Stop()
		logger.Info("Pooler discovery stopped")
	}

	tp.Unpublish()
	ts.Close()
}
