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
	"sync/atomic"
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

type Multigateway struct {
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
	// pgRequireSSL rejects plaintext client connections; requires cert + key.
	pgRequireSSL viperutil.Value[bool]
	// slotBasedReplicationEnabled gates admitting non-temporary logical failover
	// slots in the replication preamble (default off, dynamic/reloadable).
	slotBasedReplicationEnabled viperutil.Value[bool]
	// keepTransactionOnGatewayRejection, when enabled, leaves an open explicit
	// transaction in-block after a gateway policy rejection (feature_not_supported)
	// instead of aborting it. Off by default so clients see PostgreSQL's contract
	// (any wire error aborts the transaction); opt-in for pg_regress and associated
	// suites (default off, dynamic/reloadable).
	keepTransactionOnGatewayRejection viperutil.Value[bool]
	// poolerGateway manages connections to poolers and owns the lifecycle
	// of the underlying pooler cache (topology watch + per-pooler health
	// streams + per-pooler connection riders).
	poolerGateway *poolergateway.PoolerGateway
	// grpcServer is the grpc server
	grpcServer *servenv.GrpcServer
	// pgListener is the PostgreSQL protocol listener
	pgListener *server.Listener
	// pgHandler is the PostgreSQL protocol handler
	pgHandler *handler.MultigatewayHandler
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
	// authenticationTimeout bounds the PG startup phase (SSL handshake,
	// StartupMessage, SCRAM exchange). Equivalent to PostgreSQL's
	// authentication_timeout GUC.
	authenticationTimeout viperutil.Value[time.Duration]
	// planCacheMemory is the maximum memory (bytes) for the plan cache (0 disables)
	planCacheMemory viperutil.Value[int]
	// queryMetricsMemory is the maximum memory (bytes) for per-query-shape metrics
	// tracking (0 disables fingerprint labeling and the registry RPCs).
	queryMetricsMemory viperutil.Value[int]
	// queryMetricsSQLMaxBytes is the maximum bytes of representative normalized
	// SQL stored per tracked fingerprint.
	queryMetricsSQLMaxBytes viperutil.Value[int]
	// queryLogSampleRate is the 1/N sampling rate for normal-path query logs.
	queryLogSampleRate viperutil.Value[uint64]
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
	// prefixLost is set when a re-assertion finds this gateway's PID
	// prefix claim held by another gateway (possible only after the claim
	// expired during a topology outage). A live gateway cannot safely
	// renumber, so the loss is fatal: it fails the readiness check (to
	// drain new connections) and triggers shutdownOnPrefixLoss.
	prefixLost atomic.Bool
	// shutdownOnPrefixLoss initiates the gateway's graceful shutdown when
	// its PID prefix claim is lost; the supervisor then restarts the
	// process, which claims a fresh prefix. A field so tests can observe
	// the trigger instead of terminating the test process.
	shutdownOnPrefixLoss func()
	// shutdownCtx is cancelled during Shutdown to propagate cancellation
	// to all long-running goroutines (health streams, discovery, etc.)
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func NewMultigateway() *Multigateway {
	reg := viperutil.NewRegistry()
	mg := &Multigateway{
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
		authenticationTimeout: viperutil.Configure(reg, "authentication-timeout", viperutil.Options[time.Duration]{
			Default:  60 * time.Second,
			FlagName: "authentication-timeout",
			Dynamic:  false,
			EnvVars:  []string{"MT_AUTHENTICATION_TIMEOUT"},
		}),
		planCacheMemory: viperutil.Configure(reg, "plan-cache-memory", viperutil.Options[int]{
			Default:  4 * 1024 * 1024, // 4 MB
			FlagName: "plan-cache-memory",
			Dynamic:  false,
			EnvVars:  []string{"MT_PLAN_CACHE_MEMORY"},
		}),
		queryMetricsMemory: viperutil.Configure(reg, "query-metrics-memory", viperutil.Options[int]{
			Default:  8 * 1024 * 1024, // 8 MB; 0 disables per-query tracking
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
		queryLogSampleRate: viperutil.Configure(reg, "query-log-sample-rate", viperutil.Options[uint64]{
			Default:  0,
			FlagName: "query-log-sample-rate",
			Dynamic:  false,
			EnvVars:  []string{"MT_QUERY_LOG_SAMPLE_RATE"},
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
		pgRequireSSL: viperutil.Configure(reg, "pg-require-ssl", viperutil.Options[bool]{
			Default:  false,
			FlagName: "pg-require-ssl",
			Dynamic:  false,
			EnvVars:  []string{"MT_PG_REQUIRE_SSL"},
		}),
		slotBasedReplicationEnabled: viperutil.Configure(reg, "enable-slot-based-replication", viperutil.Options[bool]{
			Default:  false,
			FlagName: "enable-slot-based-replication",
			Dynamic:  true,
			EnvVars:  []string{"MT_ENABLE_SLOT_BASED_REPLICATION"},
		}),
		keepTransactionOnGatewayRejection: viperutil.Configure(reg, "keep-transaction-on-gateway-rejection", viperutil.Options[bool]{
			Default:  false,
			FlagName: "keep-transaction-on-gateway-rejection",
			Dynamic:  true,
			EnvVars:  []string{"MT_KEEP_TRANSACTION_ON_GATEWAY_REJECTION"},
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
			},
		},
	}
	mg.shutdownOnPrefixLoss = func() { mg.senv.InitiateShutdown() }

	return mg
}

// Executor returns the query executor for this multigateway.
func (mg *Multigateway) Executor() *executor.Executor {
	return mg.executor
}

// ServEnv returns the serving environment for this multigateway.
func (mg *Multigateway) ServEnv() *servenv.ServEnv {
	return mg.senv
}

func (mg *Multigateway) RegisterFlags(fs *pflag.FlagSet) {
	fs.String("cell", mg.cell.Default(), "cell to use")
	fs.String("service-id", mg.serviceID.Default(), "optional service ID (if empty, a random ID will be generated)")
	fs.Int("pg-port", mg.pgPort.Default(), "PostgreSQL protocol listen port")
	fs.String("pg-bind-address", mg.pgBindAddress.Default(), "address to bind the PostgreSQL listener to")
	fs.Duration("statement-timeout", mg.statementTimeout.Default(), "Default statement execution timeout. 0 disables.")
	fs.Duration("authentication-timeout", mg.authenticationTimeout.Default(), "Maximum time allowed to complete client authentication (SSL handshake, startup message, SCRAM). Negative disables; 0 uses the protocol default of 60s.")
	fs.String("pg-tls-cert-file", mg.pgTLSCertFile.Default(), "path to TLS certificate file for PostgreSQL SSL connections")
	fs.String("pg-tls-key-file", mg.pgTLSKeyFile.Default(), "path to TLS private key file for PostgreSQL SSL connections")
	fs.Bool("pg-require-ssl", mg.pgRequireSSL.Default(), "require TLS for all client PostgreSQL connections; multigateway fails to start if no cert/key is configured. CancelRequest still permitted over plaintext.")
	fs.Bool("enable-slot-based-replication", mg.slotBasedReplicationEnabled.Default(), "admit non-temporary logical replication slots registered for failover (slot-based replication). Default off.")
	fs.Bool("keep-transaction-on-gateway-rejection", mg.keepTransactionOnGatewayRejection.Default(), "leave an open explicit transaction in-block after a gateway policy rejection (feature_not_supported) instead of aborting it. Off by default so clients see PostgreSQL's contract that any wire error aborts the transaction; intended for pg_regress and compatibility test suites.")
	fs.Int("pg-replica-port", mg.pgReplicaPort.Default(), "optional port for replica-reads connections; 0 disables the replica listener")
	fs.Int("low-replication-lag-ms", mg.pgReplicaLowLagMs.Default(), "replicas at or below this lag (milliseconds) are preferred; 0 treats all replicas equally")
	fs.Int("high-replication-lag-tolerance-ms", mg.pgReplicaHighLagToleranceMs.Default(), "absolute max lag (milliseconds) for replicas; 0 means no upper bound")
	fs.Int("plan-cache-memory", mg.planCacheMemory.Default(), "maximum memory in bytes for the query plan cache; 0 disables caching")
	fs.Int("query-metrics-memory", mg.queryMetricsMemory.Default(), "memory budget (bytes) for per-query-shape metrics tracking; 0 disables per-query metrics and the registry RPC")
	fs.Int("query-metrics-sql-max-bytes", mg.queryMetricsSQLMaxBytes.Default(), "maximum bytes of representative normalized SQL stored per tracked fingerprint")
	fs.Uint64("query-log-sample-rate", mg.queryLogSampleRate.Default(), "1/N sampling rate for normal-path per-query logs. Normal queries log at DEBUG, so visibility also requires --log-level=debug. 0 disables sampling (level alone governs); 1 emits every query; N>1 emits every Nth.")
	viperutil.BindFlags(
		fs,
		mg.cell,
		mg.serviceID,
		mg.pgPort,
		mg.pgBindAddress,
		mg.statementTimeout,
		mg.authenticationTimeout,
		mg.pgTLSCertFile,
		mg.pgTLSKeyFile,
		mg.pgRequireSSL,
		mg.slotBasedReplicationEnabled,
		mg.keepTransactionOnGatewayRejection,
		mg.pgReplicaPort,
		mg.pgReplicaLowLagMs,
		mg.pgReplicaHighLagToleranceMs,
		mg.planCacheMemory,
		mg.queryMetricsMemory,
		mg.queryMetricsSQLMaxBytes,
		mg.queryLogSampleRate,
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
func (mg *Multigateway) Init(ctx context.Context) error {
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

	if err := mg.bufferConfig.Validate(); err != nil {
		return fmt.Errorf("buffer config: %w", err)
	}
	if mg.bufferConfig.Enabled.Get() {
		mg.buffer = buffer.New(mg.shutdownCtx, mg.bufferConfig, logger)
		logger.InfoContext(ctx, "failover buffering enabled")
	}

	// Build transport credentials for multipooler gRPC connections.
	poolerTransportCreds, err := mg.connConfig.TransportCredentials(logger)
	if err != nil {
		return fmt.Errorf("failed to configure multipooler TLS: %w", err)
	}

	mg.poolerGateway = poolergateway.NewPoolerGateway(poolergateway.PoolerGatewayOpts{
		Ctx:           mg.shutdownCtx,
		Source:        mg.ts,
		LocalCell:     mg.cell.Get(),
		Logger:        logger,
		DialOpt:       poolerTransportCreds,
		Buffer:        mg.buffer,
		LowLag:        time.Duration(mg.pgReplicaLowLagMs.Get()) * time.Millisecond,
		HighTolerance: time.Duration(mg.pgReplicaHighLagToleranceMs.Get()) * time.Millisecond,
	})
	logger.InfoContext(ctx, "pooler cache started", "local_cell", mg.cell.Get())

	// Initialize ScatterConn for query coordination
	mg.scatterConn = scatterconn.NewScatterConn(mg.poolerGateway, logger)

	// Initialize the executor for query routing
	// Pass ScatterConn as the IExecute implementation
	mg.executor = executor.NewExecutor(mg.scatterConn, logger, mg.planCacheMemory.Get())

	// Initialize gateway-wide OTel metrics up front so the credential
	// provider and listener can share the same sink. Failures here are
	// non-fatal: the auth and TLS code paths tolerate a nil/noop recorder
	// and we don't want metric init to block startup.
	gatewayMetrics, err := NewGatewayMetrics()
	if err != nil {
		logger.WarnContext(ctx, "failed to initialize gateway metrics", "error", err)
	}

	// Create the credential provider for SCRAM authentication and the
	// replication-role gate. A single GetAuthCredentials RPC feeds both,
	// so admitting a replication connection never costs two pooler hops.
	credentialProvider := auth.NewPoolerCredentialProvider(mg.poolerGateway, gatewayMetrics)

	// Build TLS config if cert and key files are provided.
	certFile := mg.pgTLSCertFile.Get()
	keyFile := mg.pgTLSKeyFile.Get()
	requireSSL := mg.pgRequireSSL.Get()
	pgTLSConfig, err := buildPGTLSConfig(certFile, keyFile)
	if err != nil {
		return err
	}
	if requireSSL && pgTLSConfig == nil {
		return errors.New("--pg-require-ssl=true requires --pg-tls-cert-file and --pg-tls-key-file")
	}
	if pgTLSConfig != nil {
		logger.InfoContext(ctx, "TLS configured for Postgres listener",
			"cert_file", certFile, "key_file", keyFile, "require_ssl", requireSSL)
	}

	// Build the full gateway record. All info (hostname, ports) is available
	// after servenv.Init(). PidPrefix is assigned during registration below.
	multigateway := topoclient.NewMultigateway(serviceID, cell, mg.senv.GetHostname())
	multigateway.PortMap["grpc"] = int32(mg.grpcServer.Port())
	multigateway.PortMap["http"] = int32(mg.senv.GetHTTPPort())
	multigateway.PortMap["postgres"] = int32(mg.pgPort.Get())
	if replicaPort := mg.pgReplicaPort.Get(); replicaPort > 0 {
		multigateway.PortMap["postgres_replica"] = int32(replicaPort)
	}

	// Register gateway in topo with a unique PID prefix for cross-gateway
	// cancel routing. The prefix is claimed atomically before the record is
	// written; the claim file, not the record, is what makes the prefix
	// exclusively ours. Each process claims fresh — a restarted gateway has
	// no live connections whose cancel routing could be worth preserving.
	regCtx, regCancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer regCancel()
	mg.tr, err = toporeg.RegisterSynchronous(
		regCtx,
		func(ctx context.Context) error {
			if multigateway.PidPrefix == 0 {
				prefix, err := mg.claimUnusedPrefix(ctx, multigateway.Id)
				if err != nil {
					return err
				}
				multigateway.PidPrefix = prefix
			} else if err := mg.refreshPrefixClaim(ctx, multigateway.PidPrefix, multigateway.Id); err != nil {
				return err
			}
			return mg.ts.RegisterMultigateway(ctx, multigateway, true)
		},
		// The prefix claim is deliberately NOT released here: lease expiry
		// is its only release path, so a gateway can never delete a claim
		// that has passed to another gateway (e.g. when a prefix-lost
		// restart shuts this process down after a competitor legitimately
		// claimed the prefix).
		func(ctx context.Context) error { return mg.ts.UnregisterMultigateway(ctx, multigateway.Id) },
		toporeg.WithReassert(),
	)
	if err != nil {
		return fmt.Errorf("failed to register gateway: %w", err)
	}
	pidPrefix := multigateway.PidPrefix
	logger.InfoContext(ctx, "registered gateway", "pid_prefix", pidPrefix)

	// Construct the per-query-shape metrics registry. Shared across primary
	// and replica handlers so a query hitting either listener aggregates to
	// the same stats bucket.
	// Start from DefaultConfig so SampleInterval / TrendWindowSamples are
	// populated; only the operator-tunable size knobs come from flags.
	registryCfg := queryregistry.DefaultConfig()
	registryCfg.MaxMemoryBytes = mg.queryMetricsMemory.Get()
	registryCfg.MaxSQLLength = mg.queryMetricsSQLMaxBytes.Get()
	mg.queryRegistry = queryregistry.New(registryCfg)
	if err := mg.queryRegistry.RegisterMetrics(); err != nil {
		logger.WarnContext(ctx, "failed to register query info metric", "error", err)
	}

	queryLogSampleRate := mg.queryLogSampleRate.Get()

	// Create and start PostgreSQL protocol listener
	mg.pgHandler = handler.NewMultigatewayHandler(mg.executor, logger, mg.statementTimeout.Get())
	mg.pgHandler.SetQueryRegistry(mg.queryRegistry)
	mg.pgHandler.SetNormalQueryLogSampleRate(queryLogSampleRate)
	mg.pgHandler.SetSlotBasedReplicationEnabled(mg.slotBasedReplicationEnabled.Get)
	mg.pgHandler.SetKeepTransactionOnGatewayRejection(mg.keepTransactionOnGatewayRejection.Get)

	// Wire LISTEN/NOTIFY notification manager.
	// Uses a lazy client getter that resolves the primary pooler connection
	// from the load balancer at subscribe time (after pooler discovery).
	notifMetrics, notifMetricsErr := poolergateway.NewNotificationMetrics()
	if notifMetricsErr != nil {
		logger.WarnContext(ctx, "failed to initialise some notification metrics", "error", notifMetricsErr)
	}
	notifMgr := poolergateway.NewGRPCNotificationManager(
		func() multipoolerpb.MultipoolerServiceClient {
			conn, err := mg.poolerGateway.GetConnection(&querypb.Target{
				ShardKey: &clustermetadatapb.ShardKey{
					Database:   constants.DefaultPostgresDatabase,
					TableGroup: constants.DefaultTableGroup,
					Shard:      constants.DefaultShard,
				},
				Mode: querypb.Mode_MODE_WRITABLE,
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
		Address:               pgAddr,
		Handler:               mg.pgHandler,
		GatewayID:             pidPrefix,
		CredentialProvider:    credentialProvider,
		TLSConfig:             pgTLSConfig,
		RequireTLS:            requireSSL,
		AuthenticationTimeout: mg.authenticationTimeout.Get(),
		AuthMetrics:           gatewayMetrics,
		Logger:                logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL listener on port %d: %w", mg.pgPort.Get(), err)
	}

	// Optionally create a second listener for replica-reads connections.
	var replicaCancelFn func(pid, secret uint32) bool
	if replicaPort := mg.pgReplicaPort.Get(); replicaPort > 0 {
		replicaHandler := handler.NewMultigatewayHandler(mg.executor, logger, mg.statementTimeout.Get())
		replicaHandler.SetTargetReplica(true)
		replicaHandler.SetQueryRegistry(mg.queryRegistry)
		replicaHandler.SetNormalQueryLogSampleRate(queryLogSampleRate)
		replicaHandler.SetSlotBasedReplicationEnabled(mg.slotBasedReplicationEnabled.Get)
		replicaHandler.SetKeepTransactionOnGatewayRejection(mg.keepTransactionOnGatewayRejection.Get)
		replicaAddr := fmt.Sprintf("%s:%d", mg.pgBindAddress.Get(), replicaPort)
		mg.pgReplicaListener, err = server.NewListener(server.ListenerConfig{
			Address:               replicaAddr,
			Handler:               replicaHandler,
			GatewayID:             pidPrefix,
			CredentialProvider:    credentialProvider,
			TLSConfig:             pgTLSConfig,
			RequireTLS:            requireSSL,
			AuthenticationTimeout: mg.authenticationTimeout.Get(),
			AuthMetrics:           gatewayMetrics,
			Logger:                logger,
		})
		if err != nil {
			return fmt.Errorf("failed to create replica PostgreSQL listener on port %d: %w", replicaPort, err)
		}
		replicaCancelFn = mg.pgReplicaListener.CancelLocalConnection
		lowLagMs := mg.pgReplicaLowLagMs.Get()
		highToleranceMs := mg.pgReplicaHighLagToleranceMs.Get()
		if lowLagMs > 0 || highToleranceMs > 0 {
			logger.InfoContext(ctx, "replica replication lag thresholds configured",
				"low_lag_ms", lowLagMs, "high_tolerance_ms", highToleranceMs)
		}
	}

	// Register client connection metrics. The gatewayMetrics instance was
	// constructed earlier so the credential provider and listeners share it.
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
	// Register gRPC services via OnRun because grpcServer.Server is only
	// created in servenv.Run() (after Create()), which runs after Init().
	managerServer := NewManagerServer(mg.queryRegistry, mg.pgHandler)
	mg.senv.OnRun(func() {
		mg.cancelManager.RegisterWithGRPCServer(mg.grpcServer.Server)
		managerServer.RegisterWithGRPCServer(mg.grpcServer.Server)
	})

	// Start the PostgreSQL listener in a goroutine
	go func() {
		logger.InfoContext(ctx, "Postgres listener starting", "port", mg.pgPort.Get()) //nolint:sloglint // message intentionally starts with an operation name or proper noun
		if err := mg.pgListener.Serve(); err != nil {
			logger.ErrorContext(ctx, "Postgres listener error", "error", err) //nolint:sloglint // message intentionally starts with an operation name or proper noun
		}
	}()

	// Start the replica listener if configured.
	if mg.pgReplicaListener != nil {
		go func() {
			replicaPort := mg.pgReplicaPort.Get()
			logger.InfoContext(ctx, "replica Postgres listener starting", "port", replicaPort)
			if err := mg.pgReplicaListener.Serve(); err != nil {
				logger.ErrorContext(ctx, "replica Postgres listener error", "error", err)
			}
		}()
	}

	logger.InfoContext(
		ctx, "multigateway starting up",
		"cell", mg.cell.Get(),
		"service_id", mg.serviceID.Get(),
		"http_port", mg.senv.GetHTTPPort(),
		"grpc_port", mg.grpcServer.Port(),
		"pg_port", mg.pgPort.Get(),
		"pg_replica_port", mg.pgReplicaPort.Get(),
		"pid_prefix", pidPrefix,
	)

	mg.senv.HTTPHandleFunc("/", mg.handleIndex)

	// The gateway is ready only when all conditions are met:
	// 1. No init errors (topology registration succeeded)
	// 2. At least one pooler has been discovered (can actually serve queries)
	// 3. It still holds its PID prefix claim. A lost claim also initiates
	//    the gateway's own graceful shutdown (see refreshPrefixClaim);
	//    failing readiness here drains new connections during that window.
	mg.senv.RegisterReadyCheck(func() error {
		mg.serverStatus.mu.Lock()
		defer mg.serverStatus.mu.Unlock()
		if len(mg.serverStatus.InitError) > 0 {
			return errors.New(mg.serverStatus.InitError)
		}
		if mg.poolerGateway.PoolerCount() == 0 {
			return errors.New("no poolers discovered")
		}
		if mg.prefixLost.Load() {
			return errors.New("PID prefix claim lost to another gateway; restart required to claim a fresh prefix")
		}
		return nil
	})

	mg.senv.OnClose(func() {
		mg.Shutdown()
	})
	return nil
}

func (mg *Multigateway) RunDefault() error {
	return mg.senv.RunDefault(mg.grpcServer)
}

func (mg *Multigateway) CobraPreRunE(cmd *cobra.Command) error {
	return mg.senv.CobraPreRunE(cmd)
}

func (mg *Multigateway) Shutdown() {
	mg.senv.GetLogger().Info("multigateway shutting down")

	// Cancel the service-lifetime context first so health stream goroutines
	// stop promptly, before we close the underlying gRPC connections.
	if mg.shutdownCancel != nil {
		mg.shutdownCancel()
	}

	// Stop PostgreSQL listener
	if mg.pgListener != nil {
		if err := mg.pgListener.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing Postgres listener", "error", err)
		} else {
			mg.senv.GetLogger().Info("Postgres listener stopped") //nolint:sloglint // message intentionally starts with an operation name or proper noun
		}
	}

	// Stop replica PostgreSQL listener (if running)
	if mg.pgReplicaListener != nil {
		if err := mg.pgReplicaListener.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing replica Postgres listener", "error", err)
		} else {
			mg.senv.GetLogger().Info("replica Postgres listener stopped")
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

	// Close pooler gateway: shuts down the cache, which in turn closes
	// per-pooler connections and cancels per-pooler health-stream goroutines.
	if mg.poolerGateway != nil {
		if err := mg.poolerGateway.Close(); err != nil {
			mg.senv.GetLogger().Error("error closing pooler gateway", "error", err)
		} else {
			mg.senv.GetLogger().Info("pooler gateway closed")
		}
	}

	mg.tr.Unregister()
	mg.ts.Close()
}

// claimUnusedPrefix atomically claims a PID prefix for this gateway and
// returns it. Ownership is decided by ClaimGatewayPrefix's atomic create —
// two gateways racing for the same candidate cannot both win, so there is no
// post-registration collision check.
//
// The scan of gateway records is a mixed-version courtesy: gateways from
// before prefix claims advertise a prefix without holding a claim file, so
// their prefixes are avoided by reading their records. Once the whole fleet
// writes claim files the scan narrows nothing — a claimed prefix fails the
// atomic claim regardless.
func (mg *Multigateway) claimUnusedPrefix(ctx context.Context, id *clustermetadatapb.ID) (uint32, error) {
	usedPrefixes := make(map[uint32]bool)
	cells, err := mg.ts.GetCellNames(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting cell names: %w", err)
	}

	for _, c := range cells {
		gateways, err := mg.ts.GetMultigatewaysByCell(ctx, c)
		if err != nil {
			continue // Cell may not have gateways yet.
		}
		for _, gw := range gateways {
			if p := gw.GetPidPrefix(); p > 0 {
				usedPrefixes[p] = true
			}
		}
	}

	unused := make([]uint32, 0, pid.MaxPrefix-len(usedPrefixes))
	for prefix := uint32(1); prefix <= pid.MaxPrefix; prefix++ {
		if !usedPrefixes[prefix] {
			unused = append(unused, prefix)
		}
	}

	// Claim random candidates until one wins. Losing a claim race
	// (NodeExists) just means another gateway got there first — remove the
	// candidate and try another.
	for len(unused) > 0 {
		i := rand.IntN(len(unused))
		candidate := unused[i]
		unused[i] = unused[len(unused)-1]
		unused = unused[:len(unused)-1]

		err := mg.ts.ClaimGatewayPrefix(ctx, candidate, id)
		if err == nil {
			return candidate, nil
		}
		if !errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
			return 0, fmt.Errorf("claiming PID prefix %d: %w", candidate, err)
		}
	}
	return 0, fmt.Errorf("no available PID prefix (all %d prefixes in use)", pid.MaxPrefix)
}

// refreshPrefixClaim re-asserts ownership of the gateway's claimed PID
// prefix. NodeExists means the claim expired during a topology outage and
// another gateway took the prefix — that loss is fatal: a live gateway
// cannot renumber (its prefix is baked into the listeners and cancel
// manager, and clients hold PIDs stamped with it), so the only repair is a
// fresh process claiming a fresh prefix. On first detection the gateway
// fails its readiness check (draining new connections) and initiates its
// own graceful shutdown; the supervisor — Kubernetes restartPolicy,
// systemd, docker-compose — restarts the process regardless of probe
// configuration. Any other error is transient and left to the next
// re-assertion.
func (mg *Multigateway) refreshPrefixClaim(ctx context.Context, prefix uint32, id *clustermetadatapb.ID) error {
	err := mg.ts.ClaimGatewayPrefix(ctx, prefix, id)
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NodeExists}) {
		if mg.prefixLost.CompareAndSwap(false, true) {
			servenv.GetLogger().ErrorContext(ctx,
				"PID prefix claim lost to another gateway; shutting down so a fresh process claims a fresh prefix",
				"pid_prefix", prefix)
			mg.shutdownOnPrefixLoss()
		}
		return fmt.Errorf("PID prefix %d claim lost to another gateway", prefix)
	}
	return err
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
