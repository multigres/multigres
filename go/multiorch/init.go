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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/clustermetadata/toporeg"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/config"
	"github.com/multigres/multigres/go/multiorch/recovery"
	"github.com/multigres/multigres/go/servenv"
	"github.com/multigres/multigres/go/tools/viperutil"
)

// maxPoolerConnections is the maximum number of simultaneous RPC connections
// to multipooler instances that multiorch will maintain. This limits both the
// RPC client connection cache capacity and serves as a warning threshold for
// the number of poolers being monitored.
const maxPoolerConnections = 1000

type MultiOrch struct {
	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer
	// senv is the serving environment
	senv *servenv.ServEnv
	// topoConfig holds topology configuration
	topoConfig *topo.TopoConfig
	// connConfig holds multipooler RPC client configuration
	connConfig   *rpcclient.ConnConfig
	ts           topo.Store
	tr           *toporeg.TopoReg
	serverStatus Status

	// Orchestration components
	cfg            *config.Config
	recoveryEngine *recovery.Engine
}

func (mo *MultiOrch) CobraPreRunE(cmd *cobra.Command) error {
	return mo.senv.CobraPreRunE(cmd)
}

func (mo *MultiOrch) RunDefault() error {
	return mo.senv.RunDefault(mo.grpcServer)
}

// Register flags that are specific to multiorch.
func (mo *MultiOrch) RegisterFlags(fs *pflag.FlagSet) {
	mo.cfg.RegisterFlags(fs)
	mo.senv.RegisterFlags(fs)
	mo.grpcServer.RegisterFlags(fs)
	mo.topoConfig.RegisterFlags(fs)
	mo.connConfig.RegisterFlags(fs)
}

func NewMultiOrch() *MultiOrch {
	reg := viperutil.NewRegistry()
	return &MultiOrch{
		cfg:        config.NewConfig(reg),
		grpcServer: servenv.NewGrpcServer(reg),
		senv:       servenv.NewServEnv(reg),
		topoConfig: topo.NewTopoConfig(reg),
		connConfig: rpcclient.NewConnConfig(reg),
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
func (mo *MultiOrch) Init() error {
	if err := mo.senv.Init("multiorch"); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}
	// Get the configured logger
	logger := mo.senv.GetLogger()

	var err error
	mo.ts, err = mo.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}

	// Validate and parse shard watch targets
	targetsRaw := mo.cfg.GetShardWatchTargets()
	if len(targetsRaw) == 0 {
		return fmt.Errorf("watch-targets is required")
	}

	targets, err := config.ParseShardWatchTargets(targetsRaw)
	if err != nil {
		return fmt.Errorf("failed to parse watch-targets: %w", err)
	}

	logger.Info("multiorch starting up",
		"cell", mo.cfg.GetCell(),
		"http_port", mo.senv.GetHTTPPort(),
		"grpc_port", mo.grpcServer.Port(),
		"watch_targets", targets,
	)

	// Create MultiOrch instance for topo registration
	// TODO(sougou): Is serviceID needed? It's sent as empty string for now.
	multiorch := topo.NewMultiOrch("", mo.cfg.GetCell(), mo.senv.GetHostname())
	multiorch.PortMap["grpc"] = int32(mo.grpcServer.Port())
	multiorch.PortMap["http"] = int32(mo.senv.GetHTTPPort())

	mo.tr = toporeg.Register(
		func(ctx context.Context) error { return mo.ts.RegisterMultiOrch(ctx, multiorch, true) },
		func(ctx context.Context) error { return mo.ts.UnregisterMultiOrch(ctx, multiorch.Id) },
		func(s string) {
			mo.serverStatus.mu.Lock()
			defer mo.serverStatus.mu.Unlock()
			mo.serverStatus.InitError = s
		},
	)

	mo.senv.HTTPHandleFunc("/", mo.handleIndex)
	mo.senv.HTTPHandleFunc("/ready", mo.handleReady)

	// Create RPC client for recovery engine health checks
	rpcClient := rpcclient.NewMultiPoolerClient(maxPoolerConnections)

	// Create and start recovery engine
	mo.recoveryEngine = recovery.NewEngine(
		mo.ts,
		logger,
		mo.cfg,
		targets,
		rpcClient,
	)

	if err := mo.recoveryEngine.Start(); err != nil {
		return fmt.Errorf("failed to start recovery engine: %w", err)
	}

	mo.senv.OnClose(func() {
		mo.Shutdown()
	})
	return nil
}

func (mo *MultiOrch) Shutdown() {
	mo.senv.GetLogger().Info("multiorch shutting down")
	if mo.recoveryEngine != nil {
		mo.recoveryEngine.Stop()
	}
	mo.tr.Unregister()
	mo.ts.Close()
}
