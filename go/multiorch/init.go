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

// Package multiorch provides multiorch functionality.
package multiorch

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/toporeg"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/viperutil"
)

type MultiOrch struct {
	cell viperutil.Value[string]
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

func (mo *MultiOrch) CobraPreRunE(cmd *cobra.Command) error {
	return mo.senv.CobraPreRunE(cmd)
}

func (mo *MultiOrch) RunDefault() {
	mo.senv.RunDefault(mo.grpcServer)
}

// Register flags that are specific to multiorch.
func (mo *MultiOrch) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("cell", mo.cell.Default(), "cell to use")
	viperutil.BindFlags(fs, mo.cell)
	mo.senv.RegisterFlags(fs)
	mo.grpcServer.RegisterFlags(fs)
	mo.topoConfig.RegisterFlags(fs)
}

func NewMultiOrch() *MultiOrch {
	return &MultiOrch{
		cell: viperutil.Configure("cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
		grpcServer: servenv.NewGrpcServer(),
		senv:       servenv.NewServEnv(),
		topoConfig: topo.NewTopoConfig(),
		serverStatus: Status{
			Title: "Multiorch",
			Links: []Link{
				{"Config", "Server configuration details", "/config"},
				{"Live", "URL for liveness check", "/live"},
				{"Ready", "URL for readiness check", "/ready"},
			},
		},
	}
}

// Init initializes the multiorch. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func (mo *MultiOrch) Init() {
	mo.senv.Init()
	// Get the configured logger
	logger := mo.senv.GetLogger()
	mo.ts = mo.topoConfig.Open()

	// This doesn't change
	mo.serverStatus.Cell = mo.cell.Get()

	logger.Info("multiorch starting up",
		"cell", mo.cell.Get(),
		"http_port", mo.senv.GetHTTPPort(),
		"grpc_port", mo.grpcServer.Port(),
	)

	// Create MultiOrch instance for topo registration
	// TODO(sougou): Is serviceID needed? It's sent as empty string for now.
	multiorch := topo.NewMultiOrch("", mo.cell.Get(), mo.senv.GetHostname())
	multiorch.PortMap["grpc"] = int32(mo.grpcServer.Port())
	multiorch.PortMap["http"] = int32(mo.senv.GetHTTPPort())

	mo.tr = toporeg.Register(
		func(ctx context.Context) error { return mo.ts.RegisterMultiOrch(ctx, multiorch, true) },
		func(ctx context.Context) error { return mo.ts.UnregisterMultiOrch(ctx, multiorch.Id) },
		func(s string) { mo.serverStatus.InitError = s },
	)

	mo.senv.HTTPHandleFunc("/", mo.handleIndex)
	mo.senv.HTTPHandleFunc("/ready", mo.handleReady)

	mo.senv.OnClose(func() {
		mo.Shutdown()
	})
}

func (mo *MultiOrch) Shutdown() {
	mo.senv.GetLogger().Info("multiorch shutting down")
	mo.tr.Unregister()
	mo.ts.Close()
}
