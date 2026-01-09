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

// Package multipooler provides multipooler functionality.
package multipooler

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/servenv/toporeg"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/multipooler/connpoolmanager"
	"github.com/multigres/multigres/go/multipooler/grpcconsensusservice"
	"github.com/multigres/multigres/go/multipooler/grpcmanagerservice"
	"github.com/multigres/multigres/go/multipooler/grpcpoolerservice"
	"github.com/multigres/multigres/go/multipooler/manager"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/viperutil"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// MultiPooler represents the main multipooler instance with all configuration and state
type MultiPooler struct {
	pgctldAddr          viperutil.Value[string]
	cell                viperutil.Value[string]
	database            viperutil.Value[string]
	tableGroup          viperutil.Value[string]
	shard               viperutil.Value[string]
	serviceID           viperutil.Value[string]
	socketFilePath      viperutil.Value[string]
	poolerDir           viperutil.Value[string]
	pgPort              viperutil.Value[int]
	heartbeatIntervalMs viperutil.Value[int]
	pgBackRestStanza    viperutil.Value[string] // TODO(sougou): this is deprecated. It's now hardcoded to multigres.
	// pgBackRest TLS certificate paths (used for both server and client authentication)
	pgBackRestCertFile viperutil.Value[string]
	pgBackRestKeyFile  viperutil.Value[string]
	pgBackRestCAFile   viperutil.Value[string]
	pgBackRestPort     viperutil.Value[int]
	// GrpcServer is the grpc server
	grpcServer *servenv.GrpcServer
	// Senv is the serving environment
	senv *servenv.ServEnv
	// TopoConfig holds topology configuration
	topoConfig *topoclient.TopoConfig
	telemetry  *telemetry.Telemetry
	// connPoolConfig holds connection pool configuration (manager created inside MultiPoolerManager)
	connPoolConfig *connpoolmanager.Config

	ts           topoclient.Store
	tr           *toporeg.TopoReg
	serverStatus Status
}

func (mp *MultiPooler) CobraPreRunE(cmd *cobra.Command) error {
	return mp.senv.CobraPreRunE(cmd)
}

// NewMultiPooler creates a new MultiPooler instance with default configuration
func NewMultiPooler(telemetry *telemetry.Telemetry) *MultiPooler {
	reg := viperutil.NewRegistry()
	mp := &MultiPooler{
		pgctldAddr: viperutil.Configure(reg, "pgctld-addr", viperutil.Options[string]{
			Default:  "localhost:15200",
			FlagName: "pgctld-addr",
			Dynamic:  false,
		}),
		cell: viperutil.Configure(reg, "cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default:  "",
			FlagName: "database",
			Dynamic:  false,
		}),
		tableGroup: viperutil.Configure(reg, "table-group", viperutil.Options[string]{
			Default:  "",
			FlagName: "table-group",
			Dynamic:  false,
		}),
		shard: viperutil.Configure(reg, "shard", viperutil.Options[string]{
			Default:  "",
			FlagName: "shard",
			Dynamic:  false,
		}),
		serviceID: viperutil.Configure(reg, "service-id", viperutil.Options[string]{
			Default:  "",
			FlagName: "service-id",
			Dynamic:  false,
			EnvVars:  []string{"MT_SERVICE_ID"},
		}),
		socketFilePath: viperutil.Configure(reg, "socket-file", viperutil.Options[string]{
			Default:  "",
			FlagName: "socket-file",
			Dynamic:  false,
		}),
		poolerDir: viperutil.Configure(reg, "pooler-dir", viperutil.Options[string]{
			Default:  "",
			FlagName: "pooler-dir",
			Dynamic:  false,
		}),
		pgPort: viperutil.Configure(reg, "pg-port", viperutil.Options[int]{
			Default:  5432,
			FlagName: "pg-port",
			Dynamic:  false,
		}),
		heartbeatIntervalMs: viperutil.Configure(reg, "heartbeat-interval-milliseconds", viperutil.Options[int]{
			Default:  1000,
			FlagName: "heartbeat-interval-milliseconds",
			Dynamic:  false,
		}),
		pgBackRestStanza: viperutil.Configure(reg, "pgbackrest-stanza", viperutil.Options[string]{
			Default:  "",
			FlagName: "pgbackrest-stanza",
			Dynamic:  false,
		}),
		pgBackRestCertFile: viperutil.Configure(reg, "pgbackrest-cert-file", viperutil.Options[string]{
			Default:  "/certs/pgbackrest.crt",
			FlagName: "pgbackrest-cert-file",
			Dynamic:  false,
		}),
		pgBackRestKeyFile: viperutil.Configure(reg, "pgbackrest-key-file", viperutil.Options[string]{
			Default:  "/certs/pgbackrest.key",
			FlagName: "pgbackrest-key-file",
			Dynamic:  false,
		}),
		pgBackRestCAFile: viperutil.Configure(reg, "pgbackrest-ca-file", viperutil.Options[string]{
			Default:  "/certs/ca.crt",
			FlagName: "pgbackrest-ca-file",
			Dynamic:  false,
		}),
		pgBackRestPort: viperutil.Configure(reg, "pgbackrest-port", viperutil.Options[int]{
			Default:  8432,
			FlagName: "pgbackrest-port",
			Dynamic:  false,
		}),
		grpcServer:     servenv.NewGrpcServer(reg),
		senv:           servenv.NewServEnvWithConfig(reg, servenv.NewLogger(reg, telemetry), viperutil.NewViperConfig(reg), telemetry),
		telemetry:      telemetry,
		topoConfig:     topoclient.NewTopoConfig(reg),
		connPoolConfig: connpoolmanager.NewConfig(reg),
		serverStatus: Status{
			Title: "Multipooler",
			Links: []Link{
				{"Config", "Server configuration details", "/config"},
				{"Live", "URL for liveness check", "/live"},
				{"Ready", "URL for readiness check", "/ready"},
			},
		},
	}
	mp.senv.InitServiceMap("grpc", "pooler")
	mp.senv.InitServiceMap("grpc", "poolermanager")
	mp.senv.InitServiceMap("grpc", "consensus")
	return mp
}

// RegisterFlags registers all multipooler flags with the given FlagSet
func (mp *MultiPooler) RegisterFlags(flags *pflag.FlagSet) {
	flags.String("pgctld-addr", mp.pgctldAddr.Default(), "Address of pgctld gRPC service")
	flags.String("cell", mp.cell.Default(), "cell to use")
	flags.String("database", mp.database.Default(), "database name this multipooler serves (required)")
	flags.String("table-group", mp.tableGroup.Default(), "table group this multipooler serves (required)")
	flags.String("shard", mp.shard.Default(), "shard this multipooler serves (required)")
	flags.String("service-id", mp.serviceID.Default(), "optional service ID (if empty, a random ID will be generated)")
	flags.String("socket-file", mp.socketFilePath.Default(), "PostgreSQL Unix socket file path (if empty, TCP connection will be used)")
	flags.String("pooler-dir", mp.poolerDir.Default(), "pooler directory path (if empty, socket-file path will be used as-is)")
	flags.Int("pg-port", mp.pgPort.Default(), "PostgreSQL port number")
	flags.Int("heartbeat-interval-milliseconds", mp.heartbeatIntervalMs.Default(), "interval in milliseconds between heartbeat writes")
	flags.String("pgbackrest-stanza", mp.pgBackRestStanza.Default(), "pgBackRest stanza name (defaults to service ID if empty)")
	flags.String("pgbackrest-cert-file", mp.pgBackRestCertFile.Default(), "pgBackRest TLS certificate file path (used for both server and client)")
	flags.String("pgbackrest-key-file", mp.pgBackRestKeyFile.Default(), "pgBackRest TLS key file path (used for both server and client)")
	flags.String("pgbackrest-ca-file", mp.pgBackRestCAFile.Default(), "pgBackRest TLS CA file path (used for both server and client)")
	flags.Int("pgbackrest-port", mp.pgBackRestPort.Default(), "pgBackRest TLS server port")

	viperutil.BindFlags(flags,
		mp.pgctldAddr,
		mp.cell,
		mp.database,
		mp.tableGroup,
		mp.shard,
		mp.serviceID,
		mp.socketFilePath,
		mp.poolerDir,
		mp.pgPort,
		mp.heartbeatIntervalMs,
		mp.pgBackRestStanza,
		mp.pgBackRestCertFile,
		mp.pgBackRestKeyFile,
		mp.pgBackRestCAFile,
		mp.pgBackRestPort,
	)

	mp.grpcServer.RegisterFlags(flags)
	mp.senv.RegisterFlags(flags)
	mp.topoConfig.RegisterFlags(flags)
	mp.connPoolConfig.RegisterFlags(flags)
}

// Init initializes the multipooler. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func (mp *MultiPooler) Init(startCtx context.Context) error {
	startCtx, span := telemetry.Tracer().Start(startCtx, "Init")
	defer span.End()

	// Resolve service ID early for telemetry resource attributes
	serviceID := mp.serviceID.Get()
	if serviceID == "" {
		serviceID = servenv.GenerateRandomServiceID()
	}
	cell := mp.cell.Get()

	if err := mp.senv.Init(servenv.ServiceIdentity{
		ServiceName:       constants.ServiceMultipooler,
		ServiceInstanceID: serviceID,
		Cell:              cell,
		Shard:             mp.shard.Get(),
		Database:          mp.database.Get(),
		TableGroup:        mp.tableGroup.Get(),
	}); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}
	// Get the configured logger
	logger := mp.senv.GetLogger()

	// Ensure we open the topo before we start the context, so that the
	// defer that closes the topo runs after cancelling the context.
	// This ensures that we've properly closed things like the watchers
	// at that point.
	var err error
	mp.ts, err = mp.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}

	logger.InfoContext(startCtx, "multipooler starting up",
		"pgctld_addr", mp.pgctldAddr.Get(),
		"cell", mp.cell.Get(),
		"database", mp.database.Get(),
		"table_group", mp.tableGroup.Get(),
		"shard", mp.shard.Get(),
		"socket_file_path", mp.socketFilePath.Get(),
		"pooler_dir", mp.poolerDir.Get(),
		"pg_port", mp.pgPort.Get(),
		"http_port", mp.senv.GetHTTPPort(),
		"grpc_port", mp.grpcServer.Port(),
	)

	if mp.database.Get() == "" {
		return errors.New("database is required")
	}

	if mp.tableGroup.Get() == "" {
		return errors.New("table group is required")
	}

	if mp.shard.Get() == "" {
		return errors.New("shard is required")
	}

	// Create multipooler record with all fields now that servenv.Init() has set them up
	multipooler := topoclient.NewMultiPooler(serviceID, cell, mp.senv.GetHostname(), mp.tableGroup.Get())
	multipooler.PortMap["grpc"] = int32(mp.grpcServer.Port())
	multipooler.PortMap["http"] = int32(mp.senv.GetHTTPPort())
	multipooler.PortMap["postgres"] = int32(mp.pgPort.Get())
	multipooler.PortMap["pgbackrest"] = int32(mp.pgBackRestPort.Get())
	multipooler.Database = mp.database.Get()
	multipooler.TableGroup = mp.tableGroup.Get()
	multipooler.Shard = mp.shard.Get()
	multipooler.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
	multipooler.PoolerDir = mp.poolerDir.Get()
	// For now, all poolers start as REPLICA
	multipooler.Type = clustermetadatapb.PoolerType_REPLICA

	logger.InfoContext(startCtx, "Initializing MultiPoolerManager")
	poolerManager, err := manager.NewMultiPoolerManager(logger, multipooler, &manager.Config{
		SocketFilePath:      mp.socketFilePath.Get(),
		TopoClient:          mp.ts,
		HeartbeatIntervalMs: mp.heartbeatIntervalMs.Get(),
		PgctldAddr:          mp.pgctldAddr.Get(),
		ConsensusEnabled:    mp.grpcServer.CheckServiceMap("consensus", mp.senv),
		ConnPoolConfig:      mp.connPoolConfig,
		PgBackRestCertFile:  mp.pgBackRestCertFile.Get(),
		PgBackRestKeyFile:   mp.pgBackRestKeyFile.Get(),
		PgBackRestCAFile:    mp.pgBackRestCAFile.Get(),
		PgBackRestPort:      mp.pgBackRestPort.Get(),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipooler: %w", err)
	}

	// Start the MultiPoolerManager
	poolerManager.Start(mp.senv)
	grpcmanagerservice.RegisterPoolerManagerServices(mp.senv, mp.grpcServer)
	grpcconsensusservice.RegisterConsensusServices(mp.senv, mp.grpcServer)
	grpcpoolerservice.RegisterPoolerServices(mp.senv, mp.grpcServer)

	mp.senv.HTTPHandleFunc("/", mp.handleIndex)
	mp.senv.HTTPHandleFunc("/ready", mp.handleReady)

	mp.senv.OnRun(
		func() {
			registerFunc := func(ctx context.Context) error {
				return mp.ts.RegisterMultiPooler(ctx, multipooler, true /* allowUpdate */)
			}
			// For poolers, we don't un-register them on shutdown (they are persistent component)
			// If they are actually deleted, they need to be cleaned up outside the lifecycle of starting / stopping.
			unregisterFunc := func(ctx context.Context) error {
				_, err := mp.ts.UpdateMultiPoolerFields(ctx, multipooler.Id,
					func(mp *clustermetadatapb.MultiPooler) error {
						mp.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
						return nil
					})
				return err
			}

			mp.tr = toporeg.Register(
				registerFunc,
				unregisterFunc,
				func(s string) {
					mp.serverStatus.mu.Lock()
					defer mp.serverStatus.mu.Unlock()
					mp.serverStatus.InitError = s
				}, /* alarm */
			)
		},
	)

	mp.senv.OnClose(func() {
		mp.Shutdown()
	})
	return nil
}

func (mp *MultiPooler) RunDefault() error {
	return mp.senv.RunDefault(mp.grpcServer)
}

func (mp *MultiPooler) Shutdown() {
	mp.senv.GetLogger().Info("multipooler shutting down")
	mp.tr.Unregister()
	mp.ts.Close()
}
