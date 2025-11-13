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
	"os"
	"time"

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

	// Orchestration components
	shardWatchTargets              viperutil.Value[[]string]
	bookkeepingInterval            viperutil.Value[time.Duration]
	clusterMetadataRefreshInterval viperutil.Value[time.Duration]
	clusterMetadataRefreshTimeout  viperutil.Value[time.Duration]
	recoveryEngine                 *RecoveryEngine
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
	fs.StringSlice("shard-watch-targets", mo.shardWatchTargets.Default(), "list of db/tablegroup/shard targets to watch")
	fs.Duration("bookkeeping-interval", mo.bookkeepingInterval.Default(), "interval for bookkeeping tasks")
	fs.Duration("cluster-metadata-refresh-interval", mo.clusterMetadataRefreshInterval.Default(), "interval for refreshing cluster metadata from topology")
	fs.Duration("cluster-metadata-refresh-timeout", mo.clusterMetadataRefreshTimeout.Default(), "timeout for cluster metadata refresh operation")
	viperutil.BindFlags(fs, mo.cell, mo.shardWatchTargets, mo.bookkeepingInterval, mo.clusterMetadataRefreshInterval, mo.clusterMetadataRefreshTimeout)
	mo.senv.RegisterFlags(fs)
	mo.grpcServer.RegisterFlags(fs)
	mo.topoConfig.RegisterFlags(fs)
}

func NewMultiOrch() *MultiOrch {
	reg := viperutil.NewRegistry()
	return &MultiOrch{
		cell: viperutil.Configure(reg, "cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
		shardWatchTargets: viperutil.Configure(reg, "shard-watch-targets", viperutil.Options[[]string]{
			FlagName: "shard-watch-targets",
			Dynamic:  true,
			EnvVars:  []string{"MT_SHARD_WATCH_TARGETS"},
		}),
		bookkeepingInterval: viperutil.Configure(reg, "bookkeeping-interval", viperutil.Options[time.Duration]{
			Default:  1 * time.Minute,
			FlagName: "bookkeeping-interval",
			Dynamic:  false,
			EnvVars:  []string{"MT_BOOKKEEPING_INTERVAL"},
		}),
		clusterMetadataRefreshInterval: viperutil.Configure(reg, "cluster-metadata-refresh-interval", viperutil.Options[time.Duration]{
			Default:  15 * time.Second,
			FlagName: "cluster-metadata-refresh-interval",
			Dynamic:  false,
			EnvVars:  []string{"MT_CLUSTER_METADATA_REFRESH_INTERVAL"},
		}),
		clusterMetadataRefreshTimeout: viperutil.Configure(reg, "cluster-metadata-refresh-timeout", viperutil.Options[time.Duration]{
			Default:  30 * time.Second,
			FlagName: "cluster-metadata-refresh-timeout",
			Dynamic:  false,
			EnvVars:  []string{"MT_CLUSTER_METADATA_REFRESH_TIMEOUT"},
		}),
		grpcServer: servenv.NewGrpcServer(reg),
		senv:       servenv.NewServEnv(reg),
		topoConfig: topo.NewTopoConfig(reg),
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
	mo.senv.Init("multiorch")
	// Get the configured logger
	logger := mo.senv.GetLogger()
	mo.ts = mo.topoConfig.Open()

	// Validate and parse shard watch targets
	targetsRaw := mo.shardWatchTargets.Get()
	if len(targetsRaw) == 0 {
		logger.Error("shard-watch-targets is required")
		os.Exit(1)
	}

	targets, err := ParseShardWatchTargets(targetsRaw)
	if err != nil {
		logger.Error("failed to parse shard-watch-targets", "error", err)
		os.Exit(1)
	}

	logger.Info("multiorch starting up",
		"cell", mo.cell.Get(),
		"http_port", mo.senv.GetHTTPPort(),
		"grpc_port", mo.grpcServer.Port(),
		"watch_targets", targets,
	)

	// Create MultiOrch instance for topo registration
	// TODO(sougou): Is serviceID needed? It's sent as empty string for now.
	multiorch := topo.NewMultiOrch("", mo.cell.Get(), mo.senv.GetHostname())
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

	// Create and start recovery engine
	mo.recoveryEngine = NewRecoveryEngine(
		mo.cell.Get(),
		mo.ts,
		logger,
		targets,
		mo.bookkeepingInterval.Get(),
		mo.clusterMetadataRefreshInterval.Get(),
		mo.clusterMetadataRefreshTimeout.Get(),
	)

	// Set up dynamic config reloader for shard watch targets
	mo.recoveryEngine.SetConfigReloader(func() []string {
		return mo.shardWatchTargets.Get()
	})

	if err := mo.recoveryEngine.Start(); err != nil {
		logger.Error("failed to start recovery engine", "error", err)
		os.Exit(1)
	}

	mo.senv.OnClose(func() {
		mo.Shutdown()
	})
}

func (mo *MultiOrch) Shutdown() {
	mo.senv.GetLogger().Info("multiorch shutting down")
	if mo.recoveryEngine != nil {
		mo.recoveryEngine.Stop()
	}
	mo.tr.Unregister()
	mo.ts.Close()
}
