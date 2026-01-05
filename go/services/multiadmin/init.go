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
	"context"
	"fmt"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type MultiAdmin struct {
	// adminServer holds the gRPC admin server instance
	adminServer *MultiAdminServer

	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer

	// senv is the serving environment
	senv *servenv.ServEnv

	// topoConfig holds topology configuration
	topoConfig   *topoclient.TopoConfig
	ts           topoclient.Store
	serverStatus Status
}

func (ma *MultiAdmin) RunDefault() error {
	return ma.senv.RunDefault(ma.grpcServer)
}

func (ma *MultiAdmin) CobraPreRunE(cmd *cobra.Command) error {
	return ma.senv.CobraPreRunE(cmd)
}

func NewMultiAdmin() *MultiAdmin {
	reg := viperutil.NewRegistry()
	return &MultiAdmin{
		grpcServer: servenv.NewGrpcServer(reg),
		senv:       servenv.NewServEnv(reg),
		topoConfig: topoclient.NewTopoConfig(reg),
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
func (ma *MultiAdmin) Init(ctx context.Context) error {
	if err := ma.senv.Init(servenv.ServiceIdentity{
		ServiceName: constants.ServiceMultiadmin,
	}); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}
	// Get the configured logger
	logger := ma.senv.GetLogger()

	var err error
	ma.ts, err = ma.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}

	logger.InfoContext(ctx, "multiadmin starting up",
		"http_port", ma.senv.GetHTTPPort(),
		"grpc_port", ma.grpcServer.Port(),
	)

	ma.senv.OnRun(func() {
		// Register multiadmin gRPC and HTTP API services if enabled in service map
		if ma.grpcServer.CheckServiceMap(constants.ServiceMultiadmin, ma.senv) {
			ma.adminServer = NewMultiAdminServer(ma.ts, logger)
			ma.adminServer.RegisterWithGRPCServer(ma.grpcServer.Server)

			// Set up grpc-gateway for REST API
			gwmux := runtime.NewServeMux(
				runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
					MarshalOptions:   protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true},
					UnmarshalOptions: protojson.UnmarshalOptions{DiscardUnknown: true},
				}),
			)
			// NOTE: The ctx parameter to the generated method here is unused.
			if err := multiadminpb.RegisterMultiAdminServiceHandlerServer(ctx, gwmux, ma.adminServer); err != nil {
				logger.Error("failed to register grpc-gateway handler", "error", err)
			} else {
				ma.senv.HTTPHandle("/api/", gwmux)
				logger.Info("MultiAdmin gRPC and HTTP API services registered")
			}
		}
	})

	ma.senv.HTTPHandleFunc("/", ma.handleIndex)
	ma.senv.HTTPHandleFunc("/proxy/", ma.handleProxy)
	ma.senv.HTTPHandleFunc("/ready", ma.handleReady)
	ma.senv.HTTPHandleFunc("/services", ma.handleServices)

	ma.senv.OnClose(func() {
		ma.Shutdown()
	})
	return nil
}

func (ma *MultiAdmin) Shutdown() {
	ma.senv.GetLogger().Info("multiadmin shutting down")
	ma.ts.Close()
}
