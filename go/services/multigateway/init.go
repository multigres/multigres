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
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/pgprotocol/pid"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/servenv/toporeg"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/auth"
	"github.com/multigres/multigres/go/services/multigateway/buffer"
	"github.com/multigres/multigres/go/services/multigateway/executor"
	"github.com/multigres/multigres/go/services/multigateway/handler"
	"github.com/multigres/multigres/go/services/multigateway/handler/queryregistry"
	"github.com/multigres/multigres/go/services/multigateway/poolergateway"
	"github.com/multigres/multigres/go/services/multigateway/scatterconn"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type MultiGateway struct {
	cell viperutil.Value[string]
	// serviceID string
	serviceID viperutil.Value[string]
	// pgPort is the PostgreSQL protocol listen port
	pgPort viperutil.Value[int]
	// pgBindAddress is the address to bind the PostgreSQL listener to
	pgBindAddress viperutil.Value[string]
	// pgTLSCertFile is the path to the TLS certificate file for PostgreSQL SSL connections.
	pgTLSCertFile viperutil.Value[string]
	// pgTLSKeyFile is the path to the TLS private key file for PostgreSQL SSL connections.
	pgTLSKeyFile viperutil.Value[string]
	// poolerDiscovery handles discovery of multipoolers across all cells
	poolerDiscovery *GlobalPoolerDiscovery
	// poolerGateway manages connections to poolers
	poolerGateway *poolergateway.PoolerGateway
	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer
	// pgListener is the PostgreSQL protocol listener
	pgListener *server.Listener
	// pgHandler is the PostgreSQL protocol handler
	pgHandler *handler.MultiGatewayHandler
	// pgReplicaPort is the optional port for replica-reads connections.
	// When set, a second listener accepts connections that are allowed to read from replicas.
	pgReplicaPort viperutil.Value[int]
	// pgReplicaListener is the optional replica-reads listener
	pgReplicaListener *server.Listener
	// pgReplicaLowLagMs is the preferred replication lag threshold (ms) for replicas.
	// Replicas at or below this lag are considered "healthy" and preferred.
	pgReplicaLowLagMs viperutil.Value[int]
	// pgReplicaHighLagToleranceMs is the absolute maximum replication lag (ms).
	// Replicas above this are never selected. 0 means no upper bound.
	pgReplicaHighLagToleranceMs viperutil.Value[int]
	// cancelManager handles cross-gateway query cancellation
	cancelManager *CancelManager
	// scatterConn coordinates query execution across poolers
	scatterConn *scatterconn.ScatterConn
	// executor handles query execution and routing
	executor *executor.Executor
	// buffer holds requests during PRIMARY failovers
	buffer *buffer.Buffer
	// bufferConfig holds buffer configuration
	bufferConfig *buffer.Config
	// statementTimeout is the default statement execution timeout
	statementTimeout viperutil.Value[time.Duration]
	// planCacheMemory is the maximum memory (bytes) for the plan cache (0 disables)
	planCacheMemory viperutil.Value[int]
	// queryMetricsMemory is the maximum memory (bytes) for per-query-shape metrics
	// tracking (0 disables fingerprint labeling and /debug/queries).
	queryMetricsMemory viperutil.Value[int]
	// queryMetricsSQLMaxBytes is the maximum bytes of representative normalized
	// SQL stored per tracked fingerprint.
	queryMetricsSQLMaxBytes viperutil.Value[int]
	// queryRegistry tracks per-fingerprint query statistics; shared across
	// primary and replica handlers so metrics aggregate to the same bucket.
	queryRegistry *queryregistry.Registry
	// senv is the serving environment
	senv *servenv.ServEnv
	// connConfig holds RPC client configuration (TLS, etc.) for multipooler connections
	connConfig *rpcclient.ConnConfig
	// topoConfig holds topology configuration
	topoConfig   *topoclient.TopoConfig
	ts           topoclient.Store
	tr           *toporeg.TopoReg
	serverStatus Status
	// shutdownCtx is cancelled during Shutdown to propagate cancellation
	// to all long-running goroutines (health streams, discovery, etc.)
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func NewMultiGateway() *MultiGateway {
	reg := viperutil.NewRegistry()
	mg := &MultiGateway{
		cell: viperutil.Configure(reg, "cell", viperutil.Options[string]{
			Default:  "",
			FlagName: "cell",
			Dynamic:  false,
			EnvVars:  []string{"MT_CELL"},
		}),
		serviceID: viperutil.Configure(reg, "service-id", viperutil.Options[string]{
			Default:  "",
			FlagName: "service-id",
			Dynamic:  false,
			EnvVars:  []string{"MT_SERVICE_ID"},
		}),
		pgPort: viperutil.Configure(reg, "pg-port", viperutil.Options[int]{
			Default:  5432,
			FlagName: "pg-port",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_PORT"},
		}),
		pgBindAddress: viperutil.Configure(reg, "pg-bind-address", viperutil.Options[string]{
			Default:  "0.0.0.0",
			FlagName: "pg-bind-address",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_BIND_ADDRESS"},
		}),
		statementTimeout: viperutil.Configure(reg, "statement-timeout", viperutil.Options[time.Duration]{
			Default:  30 * time.Second,
			FlagName: "statement-timeout",
			Dynamic:  false,
			EnvVars:  []string{"MT_STATEMENT_TIMEOUT"},
		}),
		planCacheMemory: viperutil.Configure(reg, "plan-cache-memory", viperutil.Options[int]{
			Default:  4 * 1024 * 1024, // 4 MB
			FlagName: "plan-cache-memory",
			Dynamic:  false,
			EnvVars:  []string{"MT_PLAN_CACHE_MEMORY"},
		}),
		queryMetricsMemory: viperutil.Configure(reg, "query-metrics-memory", viperutil.Options[int]{
			Default:  2 * 1024 * 1024, // 2 MB; 0 disables per-query tracking
			FlagName: "query-metrics-memory",
			Dynamic:  false,
			EnvVars:  []string{"MT_QUERY_METRICS_MEMORY"},
		}),
		queryMetricsSQLMaxBytes: viperutil.Configure(reg, "query-metrics-sql-max-bytes", viperutil.Options[int]{
			Default:  4096,
			FlagName: "query-metrics-sql-max-bytes",
			Dynamic:  false,
			EnvVars:  []string{"MT_QUERY_METRICS_SQL_MAX_BYTES"},
		}),
		pgTLSCertFile: viperutil.Configure(reg, "pg-tls-cert-file", viperutil.Options[string]{
			Default:  "",
			FlagName: "pg-tls-cert-file",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_TLS_CERT_FILE"},
		}),
		pgTLSKeyFile: viperutil.Configure(reg, "pg-tls-key-file", viperutil.Options[string]{
			Default:  "",
			FlagName: "pg-tls-key-file",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_TLS_KEY_FILE"},
		}),
		pgReplicaPort: viperutil.Configure(reg, "pg-replica-port", viperutil.Options[int]{
			Default:  0,
			FlagName: "pg-replica-port",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_REPLICA_PORT"},
		}),
		pgReplicaLowLagMs: viperutil.Configure(reg, "low-replication-lag-ms", viperutil.Options[int]{
			Default:  30000,
			FlagName: "low-replication-lag-ms",
			Dynamic:  false,
			EnvVars:  []string{"MT_LOW_REPLICATION_LAG_MS"},
		}),
		pgReplicaHighLagToleranceMs: viperutil.Configure(reg, "high-replication-lag-tolerance-ms", viperutil.Options[int]{
			Default:  0,
			FlagName: "high-replication-lag-tolerance-ms",
			Dynamic:  false,
			EnvVars:  []string{"MT_HIGH_REPLICATION_LAG_TOLERANCE_MS"},
		}),
		bufferConfig: buffer.NewConfig(reg),
		grpcServer:   servenv.NewGrpcServer(reg),
		senv:         servenv.NewServEnv(reg),
		connConfig:   rpcclient.NewConnConfig(reg),
		topoConfig:   topoclient.NewTopoConfig(reg),
		serverStatus: Status{
			Title: "Multigateway",
			Links: []Link{
				{"Config", "Server configuration details", "/config"},
				{"Live", "URL for liveness check", "/live"},
				{"Ready", "URL for readiness check", "/ready"},
				{"Consolidator", "Prepared statement consolidator stats", "/debug/consolidator"},
				{"Queries", "Per-query-shape metrics", "/debug/queries"},
			},
		},
	}

	return mg
}

// Executor returns the query executor for this multigateway.
func (mg *MultiGateway) Executor() *executor.Executor {
	return mg.executor
}

// ServEnv returns the serving environment for this multigateway.
func (mg *MultiGateway) ServEnv() *servenv.ServEnv {
	return mg.senv
}

func (mg *MultiGateway) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("cell", mg.cell.Default(), "cell to use")
	fs.String("service-id", mg.serviceID.Default(), "optional service ID (if empty, a random ID will be generated)")
	fs.Int("pg-port", mg.pgPort.Default(), "PostgreSQL protocol listen port")
	fs.String("pg-bind-address", mg.pgBindAddress.Default(), "address to bind the PostgreSQL listener to")
	fs.Duration("statement-timeout", mg.statementTimeout.Default(), "Default statement execution timeout. 0 disables.")
	fs.String("pg-tls-cert-file", mg.pgTLSCertFile.Default(), "path to TLS certificate file for PostgreSQL SSL connections")
	fs.String("pg-tls-key-file", mg.pgTLSKeyFile.Default(), "path to TLS private key file for PostgreSQL SSL connections")
	fs.Int("pg-replica-port", mg.pgReplicaPort.Default(), "optional port for replica-reads connections; 0 disables the replica listener")
	fs.Int("low-replication-lag-ms", mg.pgReplicaLowLagMs.Default(), "replicas at or below this lag (milliseconds) are preferred; 0 treats all replicas equally")
	fs.Int("high-replication-lag-tolerance-ms", mg.pgReplicaHighLagToleranceMs.Default(), "absolute max lag (milliseconds) for replicas; 0 means no upper bound")
	fs.Int("plan-cache-memory", mg.planCacheMemory.Default(), "maximum memory in bytes for the query plan cache; 0 disables caching")
	fs.Int("query-metrics-memory", mg.queryMetricsMemory.Default(), "memory budget (bytes) for per-query-shape metrics tracking; 0 disables per-query metrics and /debug/queries")
	fs.Int("query-metrics-sql-max-bytes", mg.queryMetricsSQLMaxBytes.Default(), "maximum bytes of representative normalized SQL stored per tracked fingerprint")
	viperutil.BindFlags(fs,
		mg.cell,
		mg.serviceID,
		mg.pgPort,
		mg.pgBindAddress,
		mg.statementTimeout,
		mg.pgTLSCertFile,
		mg.pgTLSKeyFile,
		mg.pgReplicaPort,
		mg.pgReplicaLowLagMs,
		mg.pgReplicaHighLagToleranceMs,
		mg.planCacheMemory,
		mg.queryMetricsMemory,
		mg.queryMetricsSQLMaxBytes,
	)
	mg.bufferConfig.RegisterFlags(fs)
	mg.senv.RegisterFlags(fs)
	mg.grpcServer.RegisterFlags(fs)
	mg.connConfig.RegisterFlags(fs)
	mg.topoConfig.RegisterFlags(fs)
}

// Init initializes the multigateway. If any services fail to start,
// or if some connections fail, it launches goroutines that retry
// until successful.
func (mg *MultiGateway) Init(ctx context.Context) error {
	// Resolve service ID early for telemetry resource attributes
	serviceID := mg.serviceID.Get()
	if serviceID == "" {
		serviceID = servenv.GenerateRandomServiceID()
	}
	cell := mg.cell.Get()

	if err := mg.senv.Init(servenv.ServiceIdentity{
		ServiceName:       constants.ServiceMultigateway,
		ServiceInstanceID: serviceID,
		Cell:              cell,
	}); err != nil {
		return fmt.Errorf("servenv init: %w", err)
	}
	logger := mg.senv.GetLogger()

	var err error
	mg.ts, err = mg.topoConfig.Open()
	if err != nil {
		return fmt.Errorf("topo open: %w", err)
	}

	// This doesn't change
	mg.serverStatus.LocalCell = mg.cell.Get()
	mg.serverStatus.ServiceID = mg.serviceID.Get()

	// Create a service-lifetime context cancelled on shutdown.
	mg.shutdownCtx, mg.shutdownCancel = context.WithCancel(ctx)

	// Start pooler discovery (watches all cells)
	mg.poolerDiscovery = NewGlobalPoolerDiscovery(mg.shutdownCtx, mg.ts, mg.cell.Get(), logger)
	mg.poolerDiscovery.Start()
	logger.InfoContext(ctx, "Global pooler discovery started", "local_cell", mg.cell.Get())

	// Build transport credentials for multipooler gRPC connections.
	poolerTransportCreds, err := mg.connConfig.TransportCredentials(logger)
	if err != nil {
		return fmt.Errorf("failed to configure multipooler TLS: %w", err)
	}

	// Create LoadBalancer and register with discovery for real-time updates
	loadBalancer := poolergateway.NewLoadBalancer(mg.shutdownCtx, mg.cell.Get(), logger, poolerTransportCreds)
	lowLagMs := mg.pgReplicaLowLagMs.Get()
	highToleranceMs := mg.pgReplicaHighLagToleranceMs.Get()
	if lowLagMs > 0 || highToleranceMs > 0 {
		loadBalancer.SetReplicationLagThresholds(
			time.Duration(lowLagMs)*time.Millisecond,
			time.Duration(highToleranceMs)*time.Millisecond,
		)
	}
	mg.poolerDiscovery.RegisterListener(poolergateway.NewLoadBalancerListener(loadBalancer))
	logger.InfoContext(ctx, "LoadBalancer registered with pooler discovery")

	// Create failover buffer if enabled.
	if err := mg.bufferConfig.Validate(); err != nil {
		return fmt.Errorf("buffer config: %w", err)
	}
	if mg.bufferConfig.Enabled.Get() {
		mg.buffer = buffer.New(mg.shutdownCtx, mg.bufferConfig, logger)
		// Stop buffering when the streaming health check detects a new primary.
		// This is a direct signal from the pooler's health stream — more reliable
		// and lower latency than topology-based propagation via etcd.
		loadBalancer.SetOnPrimaryServing(func(tableGroup, shard string) {
			mg.buffer.StopBuffering(commontypes.ShardKey{
				TableGroup: tableGroup,
				Shard:      shard,
			})
		})
		logger.InfoContext(ctx, "Failover buffering enabled")
	}

	// Initialize PoolerGateway for managing pooler connections
	mg.poolerGateway = poolergateway.NewPoolerGateway(loadBalancer, mg.buffer, logger)

	// Initialize ScatterConn for query coordination
	mg.scatterConn = scatterconn.NewScatterConn(mg.poolerGateway, logger)

	// Initialize the executor for query routing
	// Pass ScatterConn as the IExecute implementation
	mg.executor = executor.NewExecutor(mg.scatterConn, logger, mg.planCacheMemory.Get())

	// Create hash provider for SCRAM authentication using the pooler gateway
	hashProvider := auth.NewPoolerHashProvider(mg.poolerGateway)

	// Build TLS config if cert and key files are provided.
	certFile := mg.pgTLSCertFile.Get()
	keyFile := mg.pgTLSKeyFile.Get()
	pgTLSConfig, err := buildPGTLSConfig(certFile, keyFile)
	if err != nil {
		return err
	}
	if pgTLSConfig != nil {
		logger.InfoContext(ctx, "TLS configured for PostgreSQL listener", "cert_file", certFile, "key_file", keyFile)
	}

	// Build the full gateway record. All info (hostname, ports) is available
	// after servenv.Init(). PidPrefix is assigned during registration below.
	multigateway := topoclient.NewMultiGateway(serviceID, cell, mg.senv.GetHostname())
	multigateway.PortMap["grpc"] = int32(mg.grpcServer.Port())
	multigateway.PortMap["http"] = int32(mg.senv.GetHTTPPort())
	multigateway.PortMap["postgres"] = int32(mg.pgPort.Get())
	if replicaPort := mg.pgReplicaPort.Get(); replicaPort > 0 {
		multigateway.PortMap["postgres_replica"] = int32(replicaPort)
	}

	// Reuse existing PID prefix on re-registration.
	existingGW, err := mg.ts.GetMultiGateway(context.TODO(), multigateway.Id)
	if err == nil && existingGW != nil && existingGW.GetPidPrefix() > 0 {
		multigateway.PidPrefix = existingGW.GetPidPrefix()
	}

	// Register gateway in topo with a unique PID prefix for cross-gateway
	// cancel routing. The register function assigns the prefix, registers the
	// full record, and verifies no collision. On collision, RegisterSynchronous
	// retries with jitter until two racing gateways converge on different prefixes.
	ownIDStr := topoclient.MultiGatewayIDString(multigateway.Id)
	regCtx, regCancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer regCancel()
	mg.tr, err = toporeg.RegisterSynchronous(regCtx,
		func(ctx context.Context) error {
			if multigateway.PidPrefix == 0 {
				prefix, err := mg.findUnusedPrefix(ctx)
				if err != nil {
					return fmt.Errorf("finding unused prefix: %w", err)
				}
				multigateway.PidPrefix = prefix
			}
			if err := mg.ts.RegisterMultiGateway(ctx, multigateway, true); err != nil {
				return err
			}
			if mg.hasPrefixCollision(ctx, multigateway.PidPrefix, ownIDStr) {
				multigateway.PidPrefix = 0 // Reset for next retry.
				return errors.New("PID prefix collision detected")
			}
			return nil
		},
		func(ctx context.Context) error { return mg.ts.UnregisterMultiGateway(ctx, multigateway.Id) },
	)
	if err != nil {
		return fmt.Errorf("failed to register gateway: %w", err)
	}
	pidPrefix := multigateway.PidPrefix
	logger.InfoContext(ctx, "registered gateway", "pid_prefix", pidPrefix)

	// Construct the per-query-shape metrics registry. Shared across primary
	// and replica handlers so a query hitting either listener aggregates to
	// the same stats bucket.
	mg.queryRegistry = queryregistry.New(queryregistry.Config{
		MaxMemoryBytes: mg.queryMetricsMemory.Get(),
		MaxSQLLength:   mg.queryMetricsSQLMaxBytes.Get(),
	})
	if err := mg.queryRegistry.RegisterMetrics(); err != nil {
		logger.WarnContext(ctx, "failed to register query info metric", "error", err)
	}

	// Create and start PostgreSQL protocol listener
	mg.pgHandler = handler.NewMultiGatewayHandler(mg.executor, logger, mg.statementTimeout.Get())
	mg.pgHandler.SetQueryRegistry(mg.queryRegistry)

	// Wire LISTEN/NOTIFY notification manager.
	// Uses a lazy client getter that resolves the primary pooler connection
	// from the load balancer at subscribe time (after pooler discovery).
	notifMetrics, notifMetricsErr := poolergateway.NewNotificationMetrics()
	if notifMetricsErr != nil {
		logger.WarnContext(ctx, "failed to initialise some notification metrics", "error", notifMetricsErr)
	}
	notifMgr := poolergateway.NewGRPCNotificationManager(
		func() multipoolerpb.MultiPoolerServiceClient {
			conn, err := loadBalancer.GetConnection(&querypb.Target{
				PoolerType: clustermetadatapb.PoolerType_PRIMARY,
				TableGroup: constants.DefaultTableGroup,
				Shard:      constants.DefaultShard,
			})
			if err != nil || conn == nil {
				return nil
			}
			return conn.ServiceClient()
		},
		logger,
		notifMetrics,
	)
	mg.pgHandler.SetNotificationManager(notifMgr, notifMetrics.NotificationDropped)
	pgAddr := fmt.Sprintf("%s:%d", mg.pgBindAddress.Get(), mg.pgPort.Get())
	mg.pgListener, err = server.NewListener(server.ListenerConfig{
		Address:      pgAddr,
		Handler:      mg.pgHandler,
		GatewayID:    pidPrefix,
		HashProvider: hashProvider,
		TLSConfig:    pgTLSConfig,
		Logger:       logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL listener on port %d: %w", mg.pgPort.Get(), err)
	}

	// Optionally create a second listener for replica-reads connections.
	var replicaCancelFn func(pid, secret uint32) bool
	if replicaPort := mg.pgReplicaPort.Get(); replicaPort > 0 {
		replicaHandler := handler.NewMultiGatewayHandler(mg.executor, logger, mg.statementTimeout.Get())
		replicaHandler.SetTargetReplica(true)
		replicaHandler.SetQueryRegistry(mg.queryRegistry)
		replicaAddr := fmt.Sprintf("%s:%d", mg.pgBindAddress.Get(), replicaPort)
		mg.pgReplicaListener, err = server.NewListener(server.ListenerConfig{
			Address:      replicaAddr,
			Handler:      replicaHandler,
			GatewayID:    pidPrefix,
			HashProvider: hashProvider,
			TLSConfig:    pgTLSConfig,
			Logger:       logger,
		})
		if err != nil {
			return fmt.Errorf("failed to create replica PostgreSQL listener on port %d: %w", replicaPort, err)
		}
		replicaCancelFn = mg.pgReplicaListener.CancelLocalConnection
		if lowLagMs > 0 || highToleranceMs > 0 {
			logger.InfoContext(ctx, "replica replication lag thresholds configured",
				"low_lag_ms", lowLagMs, "high_tolerance_ms", highToleranceMs)
		}
	}

	// Register client connection metrics.
	gatewayMetrics, err := NewGatewayMetrics()
	if err != nil {
		logger.WarnContext(ctx, "failed to initialize gateway metrics", "error", err)
	}
	if gatewayMetrics != nil {
		var replicaConnCount func() int
		if mg.pgReplicaListener != nil {
			replicaConnCount = mg.pgReplicaListener.ConnectionCount
		}
		if err := gatewayMetrics.RegisterClientConnectionsCallback(mg.pgListener.ConnectionCount, replicaConnCount); err != nil {
			logger.WarnContext(ctx, "failed to register client connections callback", "error", err)
		}
	}

	// Set up cross-gateway cancel request handling.
	// The cancel manager routes to the correct listener based on the connection
	// type (primary vs replica) carried in the cancel request / gRPC forward.
	mg.cancelManager = NewCancelManager(
		mg.pgListener.CancelLocalConnection,
		replicaCancelFn,
		pidPrefix,
		mg.ts,
		logger,
		poolerTransportCreds,
	)
	mg.pgListener.SetCancelHandler(mg.cancelManager.ForListener(false))
	if mg.pgReplicaListener != nil {
		mg.pgReplicaListener.SetCancelHandler(mg.cancelManager.ForListener(true))
	}
	// Register the gRPC service via OnRun because grpcServer.Server is only
	// created in servenv.Run() (after Create()), which runs after Init().
	mg.senv.OnRun(func() {
		mg.cancelManager.RegisterWithGRPCServer(mg.grpcServer.Server)
	})

	// Start the PostgreSQL listener in a goroutine
	go func() {
		logger.Info("PostgreSQL listener starting", "port", mg.pgPort.Get())
		if err := mg.pgListener.Serve(); err != nil {
			logger.Error("PostgreSQL listener error", "error", err)
		}
	}()

	// Start the replica listener if configured.
	if mg.pgReplicaListener != nil {
		go func() {
			replicaPort := mg.pgReplicaPort.Get()
			logger.Info("replica PostgreSQL listener starting", "port", replicaPort)
			if err := mg.pgReplicaListener.Serve(); err != nil {
				logger.Error("replica PostgreSQL listener error", "error", err)
			}
		}()
	}

	logger.InfoContext(ctx, "multigateway starting up",
		"cell", mg.cell.Get(),
		"service_id", mg.serviceID.Get(),
		"http_port", mg.senv.GetHTTPPort(),
		"grpc_port", mg.grpcServer.Port(),
		"pg_port", mg.pgPort.Get(),
		"pg_replica_port", mg.pgReplicaPort.Get(),
		"pid_prefix", pidPrefix,
	)

	mg.senv.HTTPHandleFunc("/", mg.handleIndex)
	mg.senv.HTTPHandleFunc("/ready", mg.handleReady)
	mg.senv.HTTPHandleFunc("/debug/consolidator", mg.handleConsolidatorDebug)
	mg.senv.HTTPHandleFunc("/debug/queries", mg.handleQueriesDebug)

	mg.senv.OnClose(func() {
		mg.Shutdown()
	})
	return nil
}

func (mg *MultiGateway) RunDefault() error {
	return mg.senv.RunDefault(mg.grpcServer)
}

func (mg *MultiGateway) CobraPreRunE(cmd *cobra.Command) error {
	return mg.senv.CobraPreRunE(cmd)
}

func (mg *MultiGateway) Shutdown() {
	mg.senv.GetLogger().Info("multigateway shutting down")

	// Cancel the service-lifetime context first so health stream goroutines
	// stop promptly, before we close the underlying gRPC connections.
	if mg.shutdownCancel != nil {
		mg.shutdownCancel()
	}

	// Stop PostgreSQL listener
	if mg.pgListener != nil {
		if err := mg.pgListener.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing PostgreSQL listener", "error", err)
		} else {
			mg.senv.GetLogger().Info("PostgreSQL listener stopped")
		}
	}

	// Stop replica PostgreSQL listener (if running)
	if mg.pgReplicaListener != nil {
		if err := mg.pgReplicaListener.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing replica PostgreSQL listener", "error", err)
		} else {
			mg.senv.GetLogger().Info("replica PostgreSQL listener stopped")
		}
	}

	// Close cancel manager's gRPC connections
	if mg.cancelManager != nil {
		mg.cancelManager.Close()
	}

	// Close executor (plan cache cleanup)
	if mg.executor != nil {
		mg.executor.Close()
	}

	// Close per-query-shape metrics registry.
	if mg.queryRegistry != nil {
		mg.queryRegistry.Close()
	}

	// Stop failover buffer
	if mg.buffer != nil {
		mg.buffer.Shutdown()
	}

	// Close pooler gateway connections
	if mg.poolerGateway != nil {
		if err := mg.poolerGateway.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing pooler gateway", "error", err)
		} else {
			mg.senv.GetLogger().Info("Pooler gateway closed")
		}
	}

	// Stop pooler discovery
	if mg.poolerDiscovery != nil {
		mg.poolerDiscovery.Stop()
		mg.senv.GetLogger().Info("Pooler discovery stopped")
	}

	mg.tr.Unregister()
	mg.ts.Close()
}

// findUnusedPrefix scans all cells for used PID prefixes and returns a random
// unused one. Randomization reduces the chance of two gateways starting
// simultaneously and picking the same prefix.
func (mg *MultiGateway) findUnusedPrefix(ctx context.Context) (uint32, error) {
	usedPrefixes := make(map[uint32]bool)
	cells, err := mg.ts.GetCellNames(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting cell names: %w", err)
	}

	for _, c := range cells {
		gateways, err := mg.ts.GetMultiGatewaysByCell(ctx, c)
		if err != nil {
			continue // Cell may not have gateways yet.
		}
		for _, gw := range gateways {
			if p := gw.GetPidPrefix(); p > 0 {
				usedPrefixes[p] = true
			}
		}
	}

	// Collect all unused prefixes and pick one at random.
	unused := make([]uint32, 0, pid.MaxPrefix-len(usedPrefixes))
	for prefix := uint32(1); prefix <= pid.MaxPrefix; prefix++ {
		if !usedPrefixes[prefix] {
			unused = append(unused, prefix)
		}
	}
	if len(unused) == 0 {
		return 0, fmt.Errorf("no available PID prefix (all %d prefixes in use)", pid.MaxPrefix)
	}
	return unused[rand.IntN(len(unused))], nil
}

// hasPrefixCollision checks if any other gateway in topo has the same PID prefix.
func (mg *MultiGateway) hasPrefixCollision(ctx context.Context, prefix uint32, ownIDStr string) bool {
	cells, err := mg.ts.GetCellNames(ctx)
	if err != nil {
		return false
	}

	for _, c := range cells {
		gateways, err := mg.ts.GetMultiGatewaysByCell(ctx, c)
		if err != nil {
			continue
		}
		for _, gw := range gateways {
			if gw.GetPidPrefix() == prefix && topoclient.MultiGatewayIDString(gw.GetId()) != ownIDStr {
				return true
			}
		}
	}
	return false
}

// buildPGTLSConfig validates TLS flag combinations and loads the certificate.
// Returns nil if neither cert nor key file is configured (plaintext mode).
func buildPGTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" {
		return nil, errors.New("--pg-tls-key-file requires --pg-tls-cert-file")
	}
	if keyFile == "" {
		return nil, errors.New("--pg-tls-cert-file requires --pg-tls-key-file")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{protocol.ALPNProtocol}, // PG 17 ALPN forward compatibility
	}, nil
}
