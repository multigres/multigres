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

// Package multiadmin provides multiadmin functionality.
package multiadmin

import (
	"log/slog"

	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/admin/server"
	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/servenv"
)

var (
	ts     topo.Store
	logger *slog.Logger

	// adminServer holds the gRPC admin server instance
	adminServer *server.MultiAdminServer
)

// RegisterFlags registers flags specific to multiadmin.
func RegisterFlags(fs *pflag.FlagSet) {
	// Nothing to register for now.
}

// Init initializes the multiadmin. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func Init() {
	logger = servenv.GetLogger()
	ts = topo.Open()

	logger.Info("multiadmin starting up",
		"http_port", servenv.HTTPPort(),
		"grpc_port", servenv.GRPCPort(),
	)

	// Register multiadmin gRPC service with servenv's GRPCServer
	if servenv.GRPCCheckServiceMap("multiadmin") {
		adminServer = server.NewMultiAdminServer(ts, logger)
		adminServer.RegisterWithGRPCServer(servenv.GRPCServer)
		logger.Info("MultiAdmin gRPC service registered with servenv")
	}
}

func Shutdown() {
	logger.Info("multiadmin shutting down")
	ts.Close()
}
