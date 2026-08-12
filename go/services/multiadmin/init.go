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

	"connectrpc.com/vanguard"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type Multiadmin struct {
	// adminServer holds the gRPC admin server instance
	adminServer *MultiadminServer

	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer

	// senv is the serving environment
	senv *servenv.ServEnv

	// connConfig holds RPC client configuration (TLS, etc.)
	connConfig *rpcclient.ConnConfig

	// topoConfig holds topology configuration
	topoConfig   *topoclient.TopoConfig
	ts           topoclient.Store
	serverStatus Status

	// enableAuth gates Multiadmin's HTTP/Connect/REST/pprof surface behind
	// JWT bearer-token auth. gRPC stays unauthenticated regardless - see
	// Init, where this is wired to grpcServer via SetAuthMode/SetHTTPOnlyAuth
	// rather than exposing --grpc-auth-mode/mechanism choice directly.
	enableAuth viperutil.Value[bool]
}

func (ma *Multiadmin) RunDefault() error {
	return ma.senv.RunDefault(ma.grpcServer)
}

func (ma *Multiadmin) CobraPreRunE(cmd *cobra.Command) error {
	return ma.senv.CobraPreRunE(cmd)
}

func NewMultiadmin() *Multiadmin {
	reg := viperutil.NewRegistry()
	return &Multiadmin{
		grpcServer: servenv.NewGrpcServer(reg),
		senv:       servenv.NewServEnv(reg),
		connConfig: rpcclient.NewConnConfig(reg),
		topoConfig: topoclient.NewTopoConfig(reg),
		enableAuth: viperutil.Configure(reg, "enable-auth", viperutil.Options[bool]{
			Default:  false,
			FlagName: "enable-auth",
			Dynamic:  false,
		}),
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
func (ma *Multiadmin) RegisterFlags(fs *pflag.FlagSet) {
	ma.senv.RegisterFlags(fs)
	ma.grpcServer.RegisterFlags(fs)
	ma.connConfig.RegisterFlags(fs)
	ma.topoConfig.RegisterFlags(fs)

	fs.Bool("enable-auth", ma.enableAuth.Default(), "Require JWT bearer-token authentication on multiadmin's HTTP/Connect/REST/pprof surface. gRPC is unaffected and stays unauthenticated. Requires --grpc-auth-jwt-issuer and --grpc-auth-jwt-jwks-uri.")
	viperutil.BindFlags(fs, ma.enableAuth)
}

// Init initializes the multiadmin. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func (ma *Multiadmin) Init(ctx context.Context) error {
	// --enable-auth is the only auth-related flag multiadmin exposes: it
	// picks JWT for HTTP/Connect/REST/pprof and leaves gRPC untouched. Which
	// plugin backs it, and that gRPC is excluded, are internal wiring
	// decisions, not something operators configure directly.
	if ma.enableAuth.Get() {
		ma.grpcServer.SetAuthMode("jwt")
		ma.grpcServer.SetHTTPOnlyAuth(true)
	}

	// Let built-in servenv HTTP endpoints (currently just /debug/pprof/*) be
	// gated by whichever auth plugin --enable-auth selects, same as the
	// routes multiadmin registers itself below. Safe to call before Init:
	// the accessor is resolved fresh per-request, not now.
	ma.senv.SetAuthPlugin(ma.grpcServer.AuthPlugin)

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

	transportCreds, err := ma.connConfig.TransportCredentials(logger)
	if err != nil {
		return fmt.Errorf("failed to configure multipooler TLS: %w", err)
	}

	ma.senv.OnRun(func() {
		// Register multiadmin gRPC and Connect API services if enabled in service map
		if ma.grpcServer.CheckServiceMap(constants.ServiceMultiadmin, ma.senv) {
			ma.adminServer = NewMultiadminServer(ma.ts, logger, transportCreds)
			ma.adminServer.RegisterWithGRPCServer(ma.grpcServer.Server)

			connectPath, connectHandler := newConnectHandler(ma.adminServer, ma.grpcServer.AuthPlugin)
			// Serve the Connect/gRPC-Web protocol (canonical camelCase JSON) for
			// the web UI directly.
			ma.senv.HTTPHandle(connectPath, connectHandler)

			// Also expose the RESTful /api/v1 routes (from the proto's
			// google.api.http annotations) via a Vanguard transcoder that wraps
			// the same handler. REST serves canonical proto3 JSON (camelCase),
			// matching the Connect API and standard transcoder defaults.
			transcoder, err := vanguard.NewTranscoder(
				[]*vanguard.Service{vanguard.NewService(connectPath, connectHandler)},
			)
			if err != nil {
				logger.ErrorContext(ctx, "failed to build REST transcoder", "error", err)
			} else {
				ma.senv.HTTPHandle("/api/", transcoder)
			}
			logger.InfoContext(ctx, "multiadmin gRPC, Connect, and REST API services registered")
		}
	})

	// servenv.RequireBearerAuth resolves ma.grpcServer.AuthPlugin() fresh on
	// every request rather than once here, since these routes are registered
	// before the gRPC server (and therefore the active auth plugin) exists.
	ma.senv.HTTPHandleFunc("/", servenv.RequireBearerAuth(ma.grpcServer.AuthPlugin, ma.handleIndex))
	ma.senv.HTTPHandleFunc("/proxy/", servenv.RequireBearerAuth(ma.grpcServer.AuthPlugin, ma.handleProxy))
	ma.senv.HTTPHandleFunc("/services", servenv.RequireBearerAuth(ma.grpcServer.AuthPlugin, ma.handleServices))

	ma.senv.OnClose(func() {
		ma.Shutdown()
	})
	return nil
}

func (ma *Multiadmin) Shutdown() {
	ma.senv.GetLogger().Info("multiadmin shutting down")
	ma.ts.Close()
}
