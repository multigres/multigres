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
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/admin/server"
	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type MultiAdmin struct {
	// adminServer holds the gRPC admin server instance
	adminServer *server.MultiAdminServer

	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer

	// senv is the serving environment
	senv *servenv.ServEnv

	// topoConfig holds topology configuration
	topoConfig   *topo.TopoConfig
	ts           topo.Store
	serverStatus Status
}

func (ma *MultiAdmin) RunDefault() {
	ma.senv.RunDefault(ma.grpcServer)
}

func (ma *MultiAdmin) CobraPreRunE(cmd *cobra.Command) error {
	return ma.senv.CobraPreRunE(cmd)
}

func NewMultiAdmin() *MultiAdmin {
	reg := viperutil.NewRegistry()
	return &MultiAdmin{
		grpcServer: servenv.NewGrpcServer(reg),
		senv:       servenv.NewServEnv(reg),
		topoConfig: topo.NewTopoConfig(reg),
		serverStatus: Status{
			Title: "Multiadmin",
			Links: []Link{
				{"Services", "Discover and navigate to cluster services", "/services"},
				{"Config", "Server configuration details", "/config"},
				{"Live", "URL for liveness check", "/live"},
				{"Ready", "URL for readiness check", "/ready"},
			},
		},
	}
}

// RegisterFlags registers flags specific to multiadmin.
func (ma *MultiAdmin) RegisterFlags(fs *pflag.FlagSet) {
	ma.senv.RegisterFlags(fs)
	ma.grpcServer.RegisterFlags(fs)
	ma.topoConfig.RegisterFlags(fs)
}

// Init initializes the multiadmin. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func (ma *MultiAdmin) Init() {
	ma.senv.Init("multiadmin")
	// Get the configured logger
	logger := ma.senv.GetLogger()
	ma.ts = ma.topoConfig.Open()

	logger.Info("multiadmin starting up",
		"http_port", ma.senv.GetHTTPPort(),
		"grpc_port", ma.grpcServer.Port(),
	)

	ma.senv.OnRun(func() {
		// Register multiadmin gRPC service with servenv's GRPCServer
		if ma.grpcServer.CheckServiceMap("multiadmin", ma.senv) {
			ma.adminServer = server.NewMultiAdminServer(ma.ts, logger)
			ma.adminServer.RegisterWithGRPCServer(ma.grpcServer.Server)
			logger.Info("MultiAdmin gRPC service registered with servenv")
		}
	})

	ma.senv.HTTPHandleFunc("/", ma.handleIndex)
	ma.senv.HTTPHandleFunc("/proxy/", ma.handleProxy)
	ma.senv.HTTPHandleFunc("/ready", ma.handleReady)
	ma.senv.HTTPHandleFunc("/services", ma.handleServices)

	ma.senv.OnClose(func() {
		ma.Shutdown()
	})
}

func (ma *MultiAdmin) Shutdown() {
	ma.senv.GetLogger().Info("multiadmin shutting down")
	ma.ts.Close()
}
