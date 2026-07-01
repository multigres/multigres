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

package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/backup"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/servenv"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/heartbeat"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/actionlock"
	backupengine "github.com/multigres/multigres/go/services/multipooler/internal/manager/backup"
	"github.com/multigres/multigres/go/services/multipooler/internal/manager/consensus"
	"github.com/multigres/multigres/go/services/multipooler/internal/poolerserver"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
	"github.com/multigres/multigres/go/tools/ctxutil"
	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/retry"
	"github.com/multigres/multigres/go/tools/telemetry"
	"github.com/multigres/multigres/go/tools/timer"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pgctldpb "github.com/multigres/multigres/go/pb/pgctldservice"
)

// ManagerState represents the state of the MultiPoolerManager
type ManagerState string

const (
	// ManagerStateStarting indicates the manager is starting and loading the multipooler record
	ManagerStateStarting ManagerState = "starting"
	// ManagerStateReady indicates the manager has successfully loaded the multipooler record
	ManagerStateReady ManagerState = "ready"
	// ManagerStateError indicates the manager failed to load the multipooler record
	ManagerStateError ManagerState = "error"
)

// MultiPoolerManager manages the pooler lifecycle and PostgreSQL operations
type MultiPoolerManager struct {
	logger     *slog.Logger
	metrics    *managerMetrics
	config     *Config
	topoClient topoclient.Store
	// Deprecated: use servicePoolerID instead.
	serviceID       *clustermetadatapb.ID
	servicePoolerID consensus.ReplicaID
	replTracker     *heartbeat.ReplTracker
	pubsubListener  *pubsub.Listener
	pgctldClient    pgctldpb.PgCtldClient

	// connPoolMgr manages all connection pools (admin, regular, reserved)
	connPoolMgr connpoolmanager.PoolManager

	// qsc is the query service controller
	// This controller handles query serving while the manager orchestrates lifecycle,
	// topology, consensus, and replication operations.
	qsc poolerserver.PoolerController

	// actionLock is there to run only one action at a time.
	// This lock can be held for long periods of time (hours),
	// like in the case of a restore. This lock must be obtained
	// first before other mutexes.
	actionLock *actionlock.ActionLock

	// Multipooler record from topology and startup state

	// mu is the mutex for the manager's state. It must be held for the
	// following:
	// - Reading state
	// - Changing state. This can cause the lock to be held for long periods
	//   of time, particularly when updating external systems (e.g. the topo)
	//   to match the manager's state.
	mu sync.Mutex

	isOpen bool
	// record owns the local MultiPooler topology entry: reads, writes, and
	// eventually-consistent publishing to etcd. Type and ServingStatus are
	// exclusively written through record.Mutate (today only by StateManager);
	// the remaining fields are immutable after construction and exposed via
	// typed accessors that read without locking.
	record       *poolerRecord
	state        ManagerState
	stateError   error
	consensusMgr *consensus.ConsensusManager
	topoLoaded   bool
	ctx          context.Context
	cancel       context.CancelFunc
	loadTimeout  time.Duration

	// shutdownCtx is cancelled at the end of GracefulShutdown to signal
	// long-lived subscribers (currently the health-stream gRPC handlers via
	// SubscribeHealth) that they should disconnect immediately rather than
	// wait for their own context to be cancelled. This unblocks
	// grpcServer.GracefulStop, which would otherwise wait for the stream
	// handlers to return on their own.
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// readyChan is closed when state becomes Ready or Error, to broadcast to all waiters.
	// Unbuffered is safe here because we only close() the channel (which never blocks
	// and broadcasts to all receivers) rather than sending to it.
	readyChan chan struct{}

	// pgpassPath is the path to the libpq password file written at startup.
	// Passed via PGPASSFILE to pgbackrest commands so the password is not
	// exposed in the process environment.
	pgpassPath string

	// pgMonitorRetryInterval is the interval between auto-restore retry attempts.
	// Defaults to 1 second. Can be set to a shorter duration for testing.
	pgMonitorRetryInterval time.Duration

	// initialized tracks whether this pooler has been fully initialized.
	// Once true, stays true for the lifetime of the manager.
	initialized bool

	// Leader-resignation and cohort-eligibility state now live on consensusMgr
	// (consensus.ConsensusManager), alongside the term promises and rule store.

	// pgMonitor manages the PostgreSQL monitoring loop.
	pgMonitor *timer.PeriodicRunner

	// promotionInProgress is set while pg_promote() has been called but postgres has not yet
	// transitioned to primary mode. Cleared when promotion completes (success or failure).
	// Reported in the health status so multiorch can suppress spurious PrimaryIsDead detection.
	promotionInProgress atomic.Bool

	// postgresRestartsDisabled suppresses auto-restart of a stopped PostgreSQL instance.
	// When set, the monitor continues to run and detect problems but skips the start action.
	// False by default (restarts enabled); tests and demos set it during controlled failovers.
	postgresRestartsDisabled atomic.Bool

	// walReceiverManuallyStopped is set by StopReplication when it clears
	// primary_conninfo (RECEIVER_ONLY / REPLAY_AND_RECEIVER modes). It tells
	// the postgres monitor not to "self-heal" the cleared conninfo back to
	// the recorded primary — the admin/test explicitly asked replication to
	// stop. While set, this pooler publishes COHORT_ELIGIBILITY_INELIGIBLE
	// in its status so the coordinator does not try to include it in the
	// cohort. Cleared by SetPrimary and demoteStalePrimaryLocked —
	// anything that re-establishes a primary
	// link is interpreted as the admin signal expiring. Not persisted; a
	// process restart implicitly clears it.
	walReceiverManuallyStopped atomic.Bool

	// pgMonitorLastLoggedReason tracks the last logged reason in the monitor to avoid duplicate logs.
	pgMonitorLastLoggedReason string

	// stateManager coordinates serving state transitions across components
	// (query service, heartbeat tracker) and updates the multipooler record.
	stateManager *StateManager

	// backup is the pgBackRest engine. The manager orchestrates lifecycle and
	// owns the gRPC handlers, delegating the pgBackRest steps to the engine.
	backup *backupengine.Engine

	// backupHealthEnabled records whether the service layer opted this manager
	// into the background backup-health poller (via StartBackupHealth). When
	// set, openLocked (re)launches the poller on every open so it survives
	// Pause/resume cycles, which recreate pm.ctx. RPC/consensus unit tests that
	// never call StartBackupHealth leave this false, so they don't spin up
	// background pg queries. Guarded by pm.mu.
	backupHealthEnabled bool

	// healthStreamer streams health state to subscribers.
	// Owns all health-related state and provides typed update methods.
	healthStreamer *healthStreamer
}

// promotionState tracks which parts of the promotion are complete
type promotionState struct {
	isPrimaryInPostgres bool
	isPrimaryInTopology bool
	currentLSN          string
}

// demotionState tracks which parts of the demotion are complete
type demotionState struct {
	isReplicaInTopology bool   // PoolerType == REPLICA
	isReadOnly          bool   // default_transaction_read_only = on
	finalLSN            string // Captured LSN before demotion
}

// NewMultiPoolerManager creates a new MultiPoolerManager instance
func NewMultiPoolerManager(logger *slog.Logger, multiPooler *clustermetadatapb.MultiPooler, config *Config) (*MultiPoolerManager, error) {
	return NewMultiPoolerManagerWithTimeout(logger, multiPooler, config, 5*time.Minute)
}

// NewMultiPoolerManagerWithTimeout creates a new MultiPoolerManager instance with a custom load timeout
func NewMultiPoolerManagerWithTimeout(logger *slog.Logger, multiPooler *clustermetadatapb.MultiPooler, config *Config, loadTimeout time.Duration) (*MultiPoolerManager, error) {
	return newMultiPoolerManager(logger, multiPooler, config, loadTimeout, overrides{})
}

// overrides carries test-only injections for the constructor core. Its zero
// value is the production path; tests populate it via NewMultiPoolerManagerForTesting.
type overrides struct {
	// qsc, when non-nil, replaces the query-pooler server the constructor would
	// otherwise build (so tests can supply a mock query service). The consensus
	// rule store is built over qsc.InternalQueryService(), so injecting a mock
	// here also points the rule store at the mock.
	qsc poolerserver.PoolerController
	// consensusMgr, when non-nil, replaces the ConsensusManager the constructor
	// would otherwise build (so tests can supply one with a fake rule store).
	consensusMgr *consensus.ConsensusManager
}

// newMultiPoolerManager is the constructor core shared by the production
// constructors and the test constructor. ov is the zero value in production.
func newMultiPoolerManager(logger *slog.Logger, multiPooler *clustermetadatapb.MultiPooler, config *Config, loadTimeout time.Duration, ov overrides) (*MultiPoolerManager, error) {
	// Validate required multiPooler fields
	if multiPooler == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "multiPooler is required")
	}
	if multiPooler.GetShardKey().GetTableGroup() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "TableGroup is required")
	}
	if multiPooler.GetShardKey().GetShard() == "" {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "Shard is required")
	}
	if multiPooler.Id == nil {
		return nil, mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "MultiPooler.Id is required")
	}
	svcPoolerID, err := consensus.NewReplicaID(multiPooler.Id)
	if err != nil {
		return nil, mterrors.Wrap(err, "invalid MultiPooler.Id")
	}

	// MVP validation: fail fast if tablegroup/shard are not the MVP defaults
	if err := constants.ValidateMVPTableGroupAndShard(multiPooler.GetShardKey().GetTableGroup(), multiPooler.GetShardKey().GetShard()); err != nil {
		return nil, mterrors.Wrap(err, "MVP validation failed")
	}

	record, err := newPoolerRecord(logger, config.TopoClient, multiPooler)
	if err != nil {
		return nil, mterrors.Wrap(err, "invalid initial pooler record")
	}

	ctx, cancel := context.WithCancel(context.TODO())

	// Create pgctld gRPC client
	var pgctldClient pgctldpb.PgCtldClient
	if config.PgctldAddr != "" {
		conn, err := grpccommon.NewClient(config.PgctldAddr, grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())))
		if err != nil {
			logger.ErrorContext(ctx, "Failed to create pgctld gRPC client", "error", err, "addr", config.PgctldAddr)
			// Continue without client - operations that need it will fail gracefully
		} else {
			rawClient := pgctldpb.NewPgCtldClient(conn)
			pgctldClient = NewProtectedPgctldClient(rawClient)
			logger.InfoContext(ctx, "Created pgctld gRPC client", "addr", config.PgctldAddr)
		}
	}

	// Create connection pool manager from config
	var connPoolMgr connpoolmanager.PoolManager
	if config.ConnPoolConfig != nil {
		connPoolMgr = config.ConnPoolConfig.NewManager(logger)
	}

	monitorRetryInterval := 5 * time.Second
	monitorRunner := timer.NewPeriodicRunner(ctx, monitorRetryInterval)

	metrics, metricsErr := newManagerMetrics()
	if metricsErr != nil {
		logger.ErrorContext(ctx, "Failed to register manager metrics", "error", metricsErr)
	}

	pm := &MultiPoolerManager{
		logger:                 logger.With("pooler_name", multiPooler.Id.GetName()),
		metrics:                metrics,
		config:                 config,
		topoClient:             config.TopoClient,
		serviceID:              multiPooler.Id,
		servicePoolerID:        svcPoolerID,
		record:                 record,
		actionLock:             actionlock.NewActionLock(),
		state:                  ManagerStateStarting,
		loadTimeout:            loadTimeout,
		pgMonitorRetryInterval: monitorRetryInterval,
		pgctldClient:           pgctldClient,
		connPoolMgr:            connPoolMgr,
		readyChan:              make(chan struct{}),
		pgMonitor:              monitorRunner,
		healthStreamer:         newHealthStreamer(logger, multiPooler.Id, multiPooler.GetShardKey().GetTableGroup(), multiPooler.GetShardKey().GetShard()),
		// We create a dummy context because some unit tests need them.
		// These will be overwritten when Open gets called.
		ctx:    ctx,
		cancel: cancel,
	}

	// shutdownCtx is independent of ctx: ctx is recreated on every Open(),
	// while shutdownCtx exists for the lifetime of the manager and is
	// cancelled exactly once, by GracefulShutdown. Background root is
	// intentional: this ctx must outlive any Open()/Close() cycle so
	// long-lived stream subscribers can keep watching it.
	pm.shutdownCtx, pm.shutdownCancel = context.WithCancel(ctxutil.Detach(ctx))

	// Create the query service controller with the pool manager.
	// Get the drain grace period from connpool config (0 means use default).
	var drainGracePeriod time.Duration
	if config.ConnPoolConfig != nil {
		drainGracePeriod = config.ConnPoolConfig.DrainGracePeriod()
	}
	if ov.qsc != nil {
		pm.qsc = ov.qsc
	} else {
		pm.qsc = poolerserver.NewQueryPoolerServer(logger, connPoolMgr, multiPooler.Id, multiPooler.GetShardKey().GetTableGroup(), multiPooler.GetShardKey().GetShard(), pm, drainGracePeriod, config.VpidStampEnabled)
	}

	// ConsensusManager owns its own wiring (durable promise store + rule store +
	// sync-standby manager). LoadPromises loads the persisted term from disk on
	// the consensus-enabled path; a missing file means term=0 (new node), and
	// only an actual read/parse error fails the constructor. A test may inject a
	// pre-built ConsensusManager (e.g. with a fake rule store) instead.
	if ov.consensusMgr != nil {
		pm.consensusMgr = ov.consensusMgr
	} else {
		pm.consensusMgr, err = consensus.NewConsensusManager(consensus.Deps{
			Logger:       pm.logger,
			QueryService: pm.qsc.InternalQueryService(),
			PoolerDir:    pm.record.PoolerDir(),
			ID:           multiPooler.Id,
			Broadcaster:  pm.healthStreamer,
			LoadPromises: config.ConsensusEnabled,
		})
		if err != nil {
			cancel()
			return nil, err
		}
	}

	// The health streamer must wait for the query server to update its type before
	// broadcasting SERVING transitions, so the gateway doesn't discover the new
	// primary before the pooler is ready to accept requests for that type.
	pm.healthStreamer.SetQueryServer(pm.qsc)

	// Create the serving state manager with the query service and health streamer as initial components.
	// The ReplTracker is registered later when heartbeat is started.
	pm.stateManager = NewStateManager(logger, pm.record, pm.consensusMgr.CachedConsensusStatus, pm.qsc, pm.healthStreamer)

	// Construct the pgBackRest engine. It owns all pgBackRest interaction and its
	// own metrics. The pgbackrest.conf path, pgpass file, and repo config are
	// supplied later (SetConfigPath / SetPgpassPath / SetBackupConfig) once
	// topology and the backup location have loaded.
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{
		PgpassPath: pm.pgpassPath,
		PgDataDir:  postgresDataDir(),
	})
	// Wire the backup-health poller to the manager's pg connection: role
	// (only a primary archives WAL), pg_stat_archiver (WAL archive lag), and
	// the backup-relevant settings (archive/restore config).
	pm.backup.SetRoleProvider(pm.isPrimary)
	pm.backup.SetArchiverStatsProvider(pm.archiverStats)
	pm.backup.SetPGSettingsProvider(pm.backupSettings)

	return pm, nil
}

// internalQueryService returns the InternalQueryService for executing queries via the connection pool.
func (pm *MultiPoolerManager) internalQueryService() executor.InternalQueryService {
	if pm.qsc == nil {
		return nil
	}
	return pm.qsc.InternalQueryService()
}

// DoUpdateRule applies a rule update to the manager's rule store and returns
// the new position. This is used by the gRPC handler to implement rule updates
// via the API.
func (pm *MultiPoolerManager) DoUpdateRule(ctx context.Context, update *consensus.RuleUpdateBuilder) (*clustermetadatapb.PoolerPosition, error) {
	ruleWriteCtx, ruleWriteSpan := telemetry.Tracer().Start(ctx, "consensus/rule-write")
	defer ruleWriteSpan.End()
	return pm.consensusMgr.Rules().UpdateRule(ruleWriteCtx, update)
}

// query executes a query using the internal query service and returns the result.
// This is a convenience method for internal manager operations.
func (pm *MultiPoolerManager) query(ctx context.Context, sql string) (*sqltypes.Result, error) {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return nil, errors.New("internal query service not available")
	}
	return queryService.Query(ctx, sql)
}

// exec executes a command that doesn't return rows.
// This is a convenience method for internal manager operations.
func (pm *MultiPoolerManager) exec(ctx context.Context, sql string) error {
	_, err := pm.query(ctx, sql)
	return err
}

// queryArgs executes a parameterized query using the internal query service and returns the result.
// This is a convenience method for internal manager operations that helps prevent SQL injection.
func (pm *MultiPoolerManager) queryArgs(ctx context.Context, sql string, args ...any) (*sqltypes.Result, error) {
	queryService := pm.internalQueryService()
	if queryService == nil {
		return nil, errors.New("internal query service not available")
	}
	return queryService.QueryArgs(ctx, sql, args...)
}

// execArgs executes a parameterized command that doesn't return rows.
// This is a convenience method for internal manager operations that helps prevent SQL injection.
func (pm *MultiPoolerManager) execArgs(ctx context.Context, sql string, args ...any) error {
	_, err := pm.queryArgs(ctx, sql, args...)
	return err
}

// Open opens the database connections and starts background operations, then
// transitions the pooler to SERVING. This is the entry point for first-time
// startup; resume-from-Pause goes through openLocked directly so it can
// restore the pre-Pause serving state rather than blindly setting SERVING.
//
// This operation is infallible — if connection pools fail to open, queries
// will fail gracefully at query time rather than preventing the manager
// from opening. Open is idempotent and safe to call multiple times.
//
// ctx must carry an action lock. The state transition publishes through
// pm.record.Mutate, which asserts the lock.
func (pm *MultiPoolerManager) Open(ctx context.Context) {
	pm.openLocked(ctx, clustermetadatapb.PoolerServingStatus_SERVING)
}

// openLocked is the shared implementation of Open and Pause's resume. It
// brings up connections and background goroutines, then transitions to the
// caller-supplied target serving status. Open uses SERVING; resume passes
// the serving status that was current immediately before the Pause so the
// pre-Pause state is restored faithfully (important for stale-primary
// demote, which Pauses while the pooler is intentionally DISABLED and
// must not be flipped back to SERVING by resume).
func (pm *MultiPoolerManager) openLocked(ctx context.Context, targetServingStatus clustermetadatapb.PoolerServingStatus) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.isOpen {
		return
	}

	pm.ctx, pm.cancel = context.WithCancel(context.TODO())

	pm.openConnectionsLocked()
	pm.logger.InfoContext(pm.ctx, "MultiPoolerManager opened database connection")

	// Start background PostgreSQL monitoring and auto-recovery. prevState
	// persists between ticks so that broadcastHealth fires only on transitions
	// in postgres running state, not every tick.
	prevState := postgresState{}
	// WithFastStart runs the first iteration immediately rather than after one
	// interval, so we promptly detect anything that changed in postgres while we
	// were disconnected (e.g. recovery mode flipping, or a restore from backup
	// rewriting rules) instead of advertising stale state for a full interval.
	pm.pgMonitor.StartWithOptions(func(ctx context.Context) {
		if newState, err := pm.monitorPostgresIteration(ctx); err == nil {
			// Broadcast postgres health transitions so orchestrators learn
			// about changes immediately without waiting for the next 30-second
			// heartbeat. This is especially important for:
			//   - postgres going down: allows PrimaryIsDeadAnalyzer to detect failure promptly
			//   - postgres coming back up: allows FixReplication to see IsInitialized=true quickly
			if !postgresStateEqual(newState, prevState) {
				pm.logger.InfoContext(ctx, "MonitorPostgres: postgres state changed, broadcasting health",
					"postgres_running", newState.postgresRunning)
				pm.broadcastHealth()
			}
			// Transition lifecycle STARTING → ACTIVE once postgres is up
			// and responding. markPoolerActive is idempotent (short-circuits
			// when already ACTIVE) so calling it every tick is cheap, and
			// it retries the topology write on the next tick if it fails.
			// The transition is monotonic per boot: once ACTIVE, postgres
			// going down doesn't flip the lifecycle back to STARTING —
			// runtime health is communicated via Status.PostgresReady /
			// PostgresStatus, not via Lifecycle.
			if newState.postgresRunning {
				pm.markPoolerActive(ctx)
			}
			prevState = newState
		}
	}, timer.WithFastStart())
	pm.logger.InfoContext(pm.ctx, "MonitorPostgres enabled successfully")

	pm.isOpen = true

	// Relaunch the backup-health poller on the fresh context when the service
	// layer has opted in (see StartBackupHealth). closeLocked cancels pm.ctx,
	// which stops the previous poller goroutine; without this, a Pause/resume
	// cycle (pg_rewind, restart-as-standby, stale-primary demote) would leave
	// the passive backup-health gauges frozen for the rest of the process.
	if pm.backupHealthEnabled {
		pm.startBackupHealthPollerLocked()
	}

	// Start health heartbeat goroutine and transition to the target status.
	// Mutate notifies all components (query service, heartbeat, health streamer)
	// and Mutates the record. The publisher (if running, started by
	// StartTopoRegistration) picks it up and writes to etcd. Only the serving
	// status changes here; the role is left as the record already holds it.
	go pm.runHealthHeartbeat(pm.ctx, timeouts.DefaultHealthHeartbeatInterval)
	if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
		s.ServingStatus = targetServingStatus
	}); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to transition serving status on open", "target", targetServingStatus, "error", err)
	}
}

// Pause temporarily closes the manager for maintenance operations that require
// exclusive database access (e.g., pg_rewind). Returns a resume function that
// MUST be called to reopen the manager.
//
// The resume function is safe to call multiple times (idempotent). This allows
// using both defer and explicit calls:
//
//	resume := pm.Pause(ctx)
//	defer resume(ctx)  // Guarantees cleanup
//	// ... perform maintenance ...
//	resume(ctx)        // Explicit resume at the right time
//	// defer will call resume(ctx) again safely
//
// ctx passed to Pause must carry an action lock; the close state transition
// publishes through pm.record.Mutate. The ctx passed to resume must also
// carry an action lock (typically the same handler's lock — it does not
// have to be the same context value, but the lock must still be held).
//
// Pause records the serving status at the time of the call and resume
// restores it. This matters during stale-primary demote, which Pauses
// while the pooler is intentionally DISABLED: a hard-coded
// "resume → SERVING" would publish a transient (PRIMARY, SERVING) to etcd
// between the Pause and the subsequent ChangeType to REPLICA, which the
// gateway would briefly cache as the new primary.
//
// Note: Pause() cancels the manager's context, but openLocked creates a fresh one.
func (pm *MultiPoolerManager) Pause(ctx context.Context) (resume func(context.Context)) {
	// TODO: AssertActionLockHeld will itself panic once we add panic recovery in
	// key places; until then, panic here rather than silently proceeding without
	// the lock.
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		panic(err)
	}

	preServingStatus := pm.record.ServingStatus()
	if !pm.closeLocked(ctx, "paused") {
		pm.logger.ErrorContext(ctx, "MultiPoolerManager: Pause() called on already-closed manager")
	}

	return func(resumeCtx context.Context) {
		pm.openLocked(resumeCtx, preServingStatus)
	}
}

// ShutdownForTest tears down the manager. Test-only: production shutdown
// flows through MultiPooler.Shutdown → StopTopoRegistration, which holds
// the action lock from the surrounding senv lifecycle. Tests construct
// managers ad hoc and need a one-call cleanup.
//
// Cleans up in the same order production does: stops the publisher and
// cancels toporeg retries (no-op if StartTopoRegistration was never
// called), then closes the manager. This prevents the publisher
// goroutine from outliving the topo client and panicking on its next
// publish.
//
// ctx must NOT be cancelled — the action-lock acquire short-circuits on a
// cancelled ctx. From t.Cleanup (where t.Context() is already cancelled),
// pass context.Background() or ctxutil.Detach(t.Context()) instead.
//
// Safe to call multiple times and safe to call even if never opened.
func (pm *MultiPoolerManager) ShutdownForTest(ctx context.Context) {
	pm.StopTopoRegistration(ctx)

	lockCtx, err := pm.actionLock.Acquire(ctx, "ShutdownForTest")
	if err != nil {
		pm.logger.ErrorContext(ctx, "ShutdownForTest: action lock acquire failed", "error", err)
		return
	}
	defer pm.actionLock.Release(lockCtx)

	pm.pgMonitor.Stop()
	pm.closeLocked(lockCtx, "shutdown")
}

// closeLocked performs the actual close operation.
// Returns true if the manager was open and is now closed, false if it was already closed.
// Caller should NOT hold pm.mu - this function acquires it.
// Always cancels the context - Open() will create a fresh one if reopened.
//
// closeLocked does NOT stop the postgres monitor: Pause keeps it running (the
// action lock neuters it for the maintenance window), and a terminal teardown
// stops it directly before calling closeLocked (ShutdownForTest), outside pm.mu,
// since Stop() joins a callback that needs pm.mu.
//
// ctx must carry an action lock. The state transition (DISABLED) publishes
// through pm.record.Mutate.
func (pm *MultiPoolerManager) closeLocked(ctx context.Context, logMessage string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.isOpen {
		return false
	}

	// Transition to DISABLED before closing resources. This notifies all
	// components: query service rejects queries, heartbeat stops, health
	// streamer broadcasts DISABLED to subscribers. The publisher (if
	// running) picks up the Mutate and writes DISABLED to etcd —
	// pausing the manager intentionally still reflects in topology so
	// callers see the pooler is not serving queries.
	if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	}); err != nil {
		pm.logger.WarnContext(ctx, "Failed to transition to DISABLED during close", "error", err)
	}

	pm.closeConnectionsLocked(false /* forReopen */)
	pm.cancel()
	pm.isOpen = false
	pm.logger.InfoContext(ctx, "MultiPoolerManager: "+logMessage)
	return true
}

// startHeartbeat starts the replication tracker and syncs it to the current serving state.
// The heartbeat writer/reader mode is determined by the multipooler record (the source of truth),
// not by querying the database directly. If the type later changes (e.g., via promotion or
// topology load), StateManager.Mutate will notify the replTracker.
func (pm *MultiPoolerManager) startHeartbeat(ctx context.Context, shardID []byte, poolerID string) error {
	// Create the replication tracker using the executor's InternalQueryService
	pm.replTracker = heartbeat.NewReplTracker(pm.qsc.InternalQueryService(), pm.logger, shardID, poolerID, pm.config.HeartbeatIntervalMs)

	// Register with StateManager and sync to current state. This ensures the
	// heartbeat writer starts for PRIMARY or the reader starts for REPLICA,
	// based on whatever state the StateManager currently holds.
	return pm.stateManager.RegisterAndSync(ctx, pm.replTracker)
}

// startPubSubListener creates the shared LISTEN/NOTIFY listener and registers
// it with the state manager. The listener runs only when PRIMARY+SERVING.
func (pm *MultiPoolerManager) startPubSubListener(ctx context.Context) error {
	if pm.connPoolMgr == nil {
		return nil
	}
	pubsubMetrics, err := pubsub.NewPubSubMetrics()
	if err != nil {
		pm.logger.WarnContext(ctx, "failed to initialise some pubsub metrics", "error", err)
	}
	pm.pubsubListener = pubsub.NewListener(pm.connPoolMgr, pm.logger, pubsubMetrics)
	pm.qsc.SetPubSubListener(pm.pubsubListener)
	return pm.stateManager.RegisterAndSync(ctx, pm.pubsubListener)
}

// QueryServiceControl returns the query service controller.
// This follows the TabletManager pattern of exposing the controller.
func (pm *MultiPoolerManager) QueryServiceControl() poolerserver.PoolerController {
	return pm.qsc
}

// openConnectionsLocked opens database connections and initializes connection-related components.
// This operation is infallible - connection pool failures are handled gracefully at query time.
// Caller must hold pm.mu.
// This is symmetric to closeConnectionsLocked and used by both Open() and reopenConnections().
func (pm *MultiPoolerManager) openConnectionsLocked() {
	// Open connection pool manager

	// Open connection pool manager
	if pm.connPoolMgr != nil {
		pgPort := int(pm.record.Port("postgres"))
		connConfig := &connpoolmanager.ConnectionConfig{
			SocketFile: pm.config.SocketFilePath,
			Port:       pgPort,
			Database:   pm.record.ShardKey().GetDatabase(),
		}
		// When no Unix socket is configured, fall back to a TCP dial against
		// the multipooler's own hostname. Postgres is colocated with pgctld on
		// the same host, so the multipooler's hostname always points at it.
		if connConfig.SocketFile == "" {
			connConfig.Host = pm.record.Hostname()
		}
		// Apply libpq-style TLS settings on the multipooler → postgres leg.
		// TLS is honored only on TCP dials; Unix-socket connections always run
		// plaintext, matching libpq behavior.
		//
		// Both ParseSSLMode and BuildTLSConfig already ran successfully during
		// startup validation (multipooler.Init → ConnPoolConfig.ValidatePGSSL),
		// so any error here would indicate the cert files were tampered with
		// after startup. Treat that as fatal-by-strict: keep the requested
		// sslMode but leave TLSConfig nil, which makes every dial fail
		// explicitly at negotiateSSL with "TLS config is nil but sslmode
		// requested SSL" rather than silently downgrading to plaintext.
		if connConfig.SocketFile == "" {
			sslMode, err := pm.config.ConnPoolConfig.PgSSLMode()
			if err != nil {
				pm.logger.ErrorContext(pm.ctx, "invalid --pg-client-sslmode at pool open; dials will fail", "error", err)
				connConfig.SSLMode = client.SSLModeVerifyFull // strict sentinel; any TCP dial errors out
				connConfig.TLSConfig = nil
			} else {
				tlsCfg, err := client.BuildTLSConfig(sslMode, pm.config.ConnPoolConfig.PgSSLRootCert(), connConfig.Host)
				if err != nil {
					pm.logger.ErrorContext(pm.ctx, "failed to build PG client TLS config at pool open; dials will fail in TLS-required modes", "error", err, "sslmode", sslMode)
					tlsCfg = nil
				}
				connConfig.SSLMode = sslMode
				connConfig.TLSConfig = tlsCfg
			}
			// libpq-style sslnegotiation (postgres | direct). Validated during
			// startup (ValidatePGSSL); a post-startup parse failure here keeps
			// the default (postgres) and the per-dial ValidateSSLNegotiation
			// check in client.startup surfaces any residual inconsistency.
			sslNegotiation, err := pm.config.ConnPoolConfig.PgSSLNegotiation()
			if err != nil {
				pm.logger.ErrorContext(pm.ctx, "invalid --pg-client-sslnegotiation at pool open; using default \"postgres\"", "error", err)
				sslNegotiation = client.SSLNegotiationPostgres
			}
			connConfig.SSLNegotiation = sslNegotiation
		}
		pm.connPoolMgr.Open(pm.ctx, connConfig)
		pm.logger.Info("Connection pool manager opened")
	}

	// Create sidecar schema and start heartbeat before opening query service controller
	// This ensures the schema exists before queries can be served
	if pm.replTracker == nil {
		pm.logger.Info("MultiPoolerManager: Starting database heartbeat")
		ctx := context.TODO()
		// TODO: populate shard ID
		shardID := []byte("0") // default shard ID

		// Use the multipooler name from serviceID as the pooler ID
		poolerID := pm.serviceID.Name

		// Schema creation is now handled by multiorch during bootstrap initialization
		// Do not auto-create schema when connecting to postgres

		if err := pm.startHeartbeat(ctx, shardID, poolerID); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to start heartbeat", "error", err)
			// Don't fail the connection if heartbeat fails
		}
	}

	// Start PubSub listener for LISTEN/NOTIFY support.
	if pm.pubsubListener == nil {
		if err := pm.startPubSubListener(context.TODO()); err != nil {
			pm.logger.Error("Failed to start PubSub listener", "error", err)
		}
	}
}

// closeConnectionsLocked closes the connection pool manager and query service controller
// without canceling the main context. Caller must hold pm.mu.
// This is used by reopenConnections() during auto-restore to avoid canceling
// the startup context that WaitUntilReady is waiting on.
//
// When forReopen is true the pool manager is closed via CloseForReopen, which
// marks the close as transient so connection requests racing the immediately
// following openConnectionsLocked wait for it and retry instead of failing with
// a closed-pool error.
func (pm *MultiPoolerManager) closeConnectionsLocked(forReopen bool) {
	// Close resources (safe to call even if nil/never opened)
	if pm.replTracker != nil {
		pm.replTracker.Close()
		pm.replTracker = nil
	}

	if pm.pubsubListener != nil {
		pm.pubsubListener.Stop()
		pm.pubsubListener = nil
	}

	// Close connection pool manager
	if pm.connPoolMgr != nil {
		if forReopen {
			pm.connPoolMgr.CloseForReopen()
		} else {
			pm.connPoolMgr.Close()
		}
	}
}

// reopenConnections closes and reopens database connections without canceling
// the manager's context. This is used to refresh stale connection pool file
// descriptors after PostgreSQL has been restarted, without disrupting contexts
// derived from pm.ctx (e.g., during auto-restore at startup).
func (pm *MultiPoolerManager) reopenConnections(_ context.Context) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// forReopen=true: CloseForReopen marks a reopen window so in-flight
	// connection requests wait for openConnectionsLocked below and retry,
	// instead of leaking a transient closed-pool error to clients.
	pm.closeConnectionsLocked(true /* forReopen */)
	pm.openConnectionsLocked()
}

// GetState returns the current state of the manager
func (pm *MultiPoolerManager) GetState() ManagerState {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.state
}

// GetStateAndError returns the current manager state and error (used for testing)
func (pm *MultiPoolerManager) GetStateAndError() (ManagerState, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.state, pm.stateError
}

// getPgCtldClient returns the pgctld gRPC client
func (pm *MultiPoolerManager) getPgCtldClient() pgctldpb.PgCtldClient {
	return pm.pgctldClient
}

// getPoolerType returns the pooler type from the multipooler record
func (pm *MultiPoolerManager) getPoolerType() clustermetadatapb.PoolerType {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.record.Type()
}

// shardKey returns a ShardKey identifying this pooler's shard.
func (pm *MultiPoolerManager) shardKey() *clustermetadatapb.ShardKey {
	return pm.record.ShardKey()
}

// BackupStatusSnapshot returns a consistent snapshot of the backup-health
// tracker for the status page.
func (pm *MultiPoolerManager) BackupStatusSnapshot() backupengine.Snapshot {
	return pm.backup.Health().Snapshot()
}

// checkReady returns an error if the manager is not in Ready state
func (pm *MultiPoolerManager) checkReady() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	switch pm.state {
	case ManagerStateReady:
		return nil
	case ManagerStateStarting:
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE, "manager is still starting up")
	case ManagerStateError:
		return mterrors.Wrap(pm.stateError, "manager is in error state")
	default:
		return mterrors.New(mtrpcpb.Code_INTERNAL, fmt.Sprintf("manager is in unknown state: %s", pm.state))
	}
}

// checkPoolerType verifies that the pooler matches the expected type
func (pm *MultiPoolerManager) checkPoolerType(expectedType clustermetadatapb.PoolerType, operationName string) error {
	pm.mu.Lock()
	poolerType := pm.record.Type()
	pm.mu.Unlock()

	if poolerType != expectedType {
		pm.logger.Error(operationName+" called on incorrect pooler type",
			"service_id", pm.serviceID.String(),
			"pooler_type", poolerType.String(),
			"expected_type", expectedType.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: pooler type is %s, must be %s (service_id: %s)",
				poolerType.String(), expectedType.String(), pm.serviceID.String()))
	}
	return nil
}

// checkReplicaGuardrails verifies that the pooler is a REPLICA and PostgreSQL is in recovery mode
// This is a common guardrail for replication-related operations on standby servers
func (pm *MultiPoolerManager) checkReplicaGuardrails(ctx context.Context) error {
	// Guardrail: Check pooler type - only REPLICA poolers can perform replication operations
	if err := pm.checkPoolerType(clustermetadatapb.PoolerType_REPLICA, "Replication operation"); err != nil {
		return err
	}

	// Guardrail: Check if the PostgreSQL instance is in recovery (standby mode)
	isInRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if !isInRecovery {
		pm.logger.ErrorContext(ctx, "Replication operation called on non-standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is not in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	return nil
}

// checkPrimaryGuardrails verifies that PostgreSQL is not in recovery mode.
// This is the canonical guardrail for primary-only operations.
func (pm *MultiPoolerManager) checkPrimaryGuardrails(ctx context.Context) error {
	isInRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check if instance is in recovery", "error", err)
		return mterrors.Wrap(err, "failed to check recovery status")
	}

	if isInRecovery {
		pm.logger.ErrorContext(ctx, "Primary operation called on standby instance", "service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("operation not allowed: the PostgreSQL instance is in standby mode (service_id: %s)", pm.serviceID.String()))
	}

	return nil
}

// setStateError sets the manager state to error with the given error message
// Must be called without holding the mutex
func (pm *MultiPoolerManager) setStateError(err error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.state = ManagerStateError
	pm.stateError = err
	pm.logger.Error("Manager state changed", "state", ManagerStateError, "error", err.Error())

	// Signal that we've reached a terminal state
	select {
	case <-pm.readyChan:
		// Already closed
	default:
		close(pm.readyChan)
	}
}

// checkAndSetReady checks if all required resources are loaded and sets state to ready if so
// Must be called without holding the mutex
func (pm *MultiPoolerManager) checkAndSetReady() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.topoLoaded {
		pm.state = ManagerStateReady
		pm.logger.Info("Manager state changed", "state", ManagerStateReady, "service_id", pm.serviceID.String())

		// Signal that we've reached ready state
		select {
		case <-pm.readyChan:
			// Already closed
		default:
			close(pm.readyChan)
		}
	}
}

// loadConfigFromTopo loads the global database entry (for backup location)
// from topology and wires up the local backup config. The cell-local
// multipooler record is owned by pm.record — registration of our own entry
// is handled by StartTopoRegistration, so this function does not re-read it.
func (pm *MultiPoolerManager) loadShardConfigFromGlobalTopo() {
	if pm.serviceID == nil {
		pm.setStateError(errors.New("ServiceID cannot be nil"))
		return
	}
	database := pm.record.ShardKey().GetDatabase()
	if database == "" {
		pm.setStateError(errors.New("database name not set in multipooler"))
		return
	}

	timeoutCtx, timeoutCancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
	defer timeoutCancel()

	r := retry.New(100*time.Millisecond, 30*time.Second)
	for _, err := range r.Attempts(timeoutCtx) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				pm.setStateError(fmt.Errorf("timeout loading database %s from topology after %v", database, pm.loadTimeout))
			} else {
				pm.setStateError(errors.New("manager context cancelled while loading database from topology"))
			}
			return
		}

		ctx, cancel := context.WithTimeout(pm.ctx, 5*time.Second)
		db, err := pm.topoClient.GetDatabase(ctx, database)
		cancel()
		if err != nil {
			continue // Will retry with backoff
		}

		// Validate and parse backup configuration
		backupConfig, err := backup.NewConfig(db.BackupLocation)
		if err != nil {
			pm.setStateError(fmt.Errorf("invalid backup_location: %w", err))
			return
		}

		// Verify we can compute the full backup path
		_, err = backupConfig.FullPath(database, pm.record.ShardKey().GetTableGroup(), pm.record.ShardKey().GetShard())
		if err != nil {
			pm.setStateError(fmt.Errorf("failed to compute backup path: %w", err))
			return
		}

		// Generate pgbackrest client config now that we have backup location.
		// pgctld already validates the pgbackrest version at startup; we don't
		// need to repeat the check here, since pgctld and multipooler are
		// co-located and share the same pgbackrest binary.
		pgPort := int(pm.record.Port("postgres"))
		socketDir := filepath.Join(pm.record.PoolerDir(), "pg_sockets")
		pg1User := constants.DefaultPostgresUser
		pg1Password := os.Getenv(constants.PgPasswordEnvVar)
		if pm.connPoolMgr != nil {
			pg1User = pm.connPoolMgr.PgUser()
			// PgPassword() returns the password resolved at startup (file →
			// env) and an ok flag. !ok means ResolvePgPassword never ran
			// successfully — surface that via setStateError so the manager
			// goroutine bails out cleanly instead of writing an empty pgpass
			// file that fails auth later.
			pw, ok := pm.connPoolMgr.PgPassword()
			if !ok {
				pm.setStateError(errors.New("pgbackrest pgpass: postgres password not resolved (ResolvePgPassword must run before pgbackrest setup)"))
				return
			}
			pg1Password = pw
		}
		configPath, err := backup.WriteClientConfig(backup.ClientConfigOpts{
			PoolerDir:     pm.record.PoolerDir(),
			Pg1Port:       pgPort,
			Pg1SocketPath: socketDir,
			Pg1Path:       postgresDataDir(),
			Pg1User:       pg1User,
		}, backupConfig)
		if err != nil {
			pm.setStateError(fmt.Errorf("failed to generate pgbackrest client config: %w", err))
			return
		}
		pm.logger.Info("Generated pgbackrest client config", "path", configPath)

		// Write a pgpass file so pgbackrest can authenticate against PostgreSQL
		// without exposing the password via PGPASSWORD in the process
		// environment. The pgbackrest/ directory is guaranteed to exist after
		// WriteClientConfig. A pgpass file is required for pgbackrest to work
		// with password authentication, and we cannot use a ephemeral file in a
		// temp directory because pgbackrest needs to be able to read it after
		// we exec (and the temp file would be cleaned up when closed).
		pgpassPath := filepath.Join(pm.record.PoolerDir(), "pgbackrest", "pgbackrest.pgpass")
		pgpassContent := fmt.Sprintf("*:*:*:%s:%s\n", pg1User, pg1Password)
		// #nosec G703 -- pgpassPath is built from the pooler's own PoolerDir, not external input.
		if err := os.WriteFile(pgpassPath, []byte(pgpassContent), 0o600); err != nil {
			pm.setStateError(fmt.Errorf("failed to write pgbackrest pgpass file: %w", err))
			return
		}

		pm.mu.Lock()
		pm.pgpassPath = pgpassPath
		pm.topoLoaded = true
		pm.mu.Unlock()

		// Feed the resolved config to the backup engine.
		pm.backup.SetBackupConfig(backupConfig)
		pm.backup.SetConfigPath(configPath)
		pm.backup.SetPgpassPath(pgpassPath)

		// Note: restoring from backup (for replicas) happens in a separate goroutine

		pm.checkAndSetReady()
		return
	}
}

// checkDemotionState checks the current state to determine what steps remain
func (pm *MultiPoolerManager) checkDemotionState(ctx context.Context) (*demotionState, error) {
	state := &demotionState{}

	// Check topology state
	pm.mu.Lock()
	poolerType := pm.record.Type()
	servingStatus := pm.record.ServingStatus()
	pm.mu.Unlock()

	state.isReplicaInTopology = (poolerType == clustermetadatapb.PoolerType_REPLICA)

	// Check if PostgreSQL is in recovery mode (canonical way to check if read-only)
	isPrimary, err := pm.isPrimary(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}
	state.isReadOnly = !isPrimary

	// Capture current LSN
	state.finalLSN, err = pm.getWALPosition(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get LSN", "error", err)
		return nil, mterrors.Wrap(err, "failed to get LSN")
	}

	pm.logger.InfoContext(ctx, "Checked demotion state",
		"is_replica_in_topology", state.isReplicaInTopology,
		"is_read_only", state.isReadOnly,
		"is_primary", isPrimary,
		"pooler_type", poolerType,
		"serving_status", servingStatus)

	return state, nil
}

// restartPostgresAsStandby restarts PostgreSQL as a standby server
// This creates standby.signal and restarts PostgreSQL via pgctld
//
// TODO: require callers to declare whether this restart is a "clean" or
// "unexpected" demote. A clean demote (graceful failover handoff, known
// consistent WAL) should leave suspectedDivergence=false. An unexpected demote
// (crash, external pg_demote, monitor-driven restart after an unknown
// shutdown) should set suspectedDivergence=true so that the next standby-side
// operation (SetPrimary's standby branch, remedialActionFixPrimaryConnInfo,
// or self-rewind detection) routes through pg_rewind dry-run before
// trusting local WAL. Today the only setter is demoteToStandbyLocked,
// which leaves several transition paths under-defended.
func (pm *MultiPoolerManager) restartPostgresAsStandby(ctx context.Context, state *demotionState) error {
	if state.isReadOnly {
		pm.logger.InfoContext(ctx, "PostgreSQL already running as standby, skipping")
		return nil
	}

	if pm.pgctldClient == nil {
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION, "pgctld client not initialized")
	}

	pm.logger.InfoContext(ctx, "Restarting PostgreSQL as standby")
	req := &pgctldpb.RestartRequest{
		Mode:      "fast",
		Timeout:   nil, // Use default timeout
		Port:      0,   // Use default port
		ExtraArgs: nil,
		AsStandby: true, // Create standby.signal before restart
	}

	resp, err := pm.pgctldClient.Restart(ctx, req)
	if err != nil {
		return mterrors.Wrap(err, "failed to restart as standby")
	}

	// Reopen connections after restart
	pm.reopenConnections(ctx)

	// Wait for database connection to be ready after restart
	if err := pm.waitForDatabaseConnection(ctx); err != nil {
		return mterrors.Wrap(err, "failed to connect to database after restart")
	}

	// Verify server is in recovery mode (standby)
	inRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		return mterrors.Wrap(err, "failed to verify standby status")
	}

	if !inRecovery {
		return mterrors.New(mtrpcpb.Code_INTERNAL, "server not in recovery mode after restart as standby")
	}

	pm.logger.InfoContext(ctx, "PostgreSQL is now running as a standby",
		"pid", resp.Pid,
		"message", resp.Message)

	return nil
}

// getActiveWriteConnections returns connections that are performing write operations
func (pm *MultiPoolerManager) getActiveWriteConnections(ctx context.Context) ([]int32, error) {
	// Query for connections doing write operations
	// Note: this is temporary, we can refactor this once we
	// have the query pool. Thinking that we should have a
	// specific user for the write pool and we can kill all connections
	// associated with that user.
	sql := `
		SELECT pid
		FROM pg_stat_activity
		WHERE pid != pg_backend_pid()
		  AND datname IS NOT NULL
		  AND backend_type = 'client backend'
		  AND state = 'active'
		  AND query NOT ILIKE 'SELECT%'
		  AND query NOT ILIKE 'SHOW%'
		  AND query NOT ILIKE 'BEGIN%'
		  AND query NOT ILIKE 'COMMIT%'
		  AND query NOT ILIKE 'ROLLBACK%'
		  AND query != '<IDLE>'`

	result, err := pm.query(ctx, sql)
	if err != nil {
		return nil, err
	}

	var pids []int32
	if result != nil {
		for _, row := range result.Rows {
			pid, err := executor.GetInt32(row, 0)
			if err != nil {
				return nil, fmt.Errorf("failed to parse pid: %w", err)
			}
			pids = append(pids, pid)
		}
	}

	return pids, nil
}

// terminateWriteConnections terminates connections performing write operations
func (pm *MultiPoolerManager) terminateWriteConnections(ctx context.Context) (int32, error) {
	pids, err := pm.getActiveWriteConnections(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to get active write connections", "error", err)
		return 0, mterrors.Wrap(err, "failed to get active write connections")
	}

	if len(pids) == 0 {
		pm.logger.InfoContext(ctx, "No active write connections to terminate")
		return 0, nil
	}

	pm.logger.WarnContext(ctx, "Terminating connections still performing writes after drain",
		"count", len(pids),
		"pids", pids)

	// Terminate each write connection
	for _, pid := range pids {
		if err := pm.execArgs(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
			pm.logger.WarnContext(ctx, "Failed to terminate write connection", "pid", pid, "error", err)
		}
	}

	return int32(len(pids)), nil
}

// drainWriteActivity monitors for write activity during emergency demotion.
// During the drain, it monitors for write activity every 100ms.
// If 2 consecutive checks show no writes, exits early.
func (pm *MultiPoolerManager) drainWriteActivity(ctx context.Context, drainTimeout time.Duration) error {
	// Monitor for write activity during drain
	pm.logger.InfoContext(ctx, "Monitoring for write activity during drain", "duration", drainTimeout)
	drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
	defer cancel()

	monitorTicker := time.NewTicker(100 * time.Millisecond)
	defer monitorTicker.Stop()

	consecutiveNoWrites := 0
	drainComplete := false

	for !drainComplete {
		select {
		case <-drainCtx.Done():
			pm.logger.InfoContext(ctx, "Drain timeout completed")
			drainComplete = true

		case <-monitorTicker.C:
			// Check for write activity
			pids, err := pm.getActiveWriteConnections(ctx)
			if err != nil {
				pm.logger.WarnContext(ctx, "Failed to check for write activity during drain", "error", err)
				consecutiveNoWrites = 0 // Reset on error
			} else if len(pids) > 0 {
				pm.logger.WarnContext(ctx, "Detected write activity during drain",
					"count", len(pids),
					"pids", pids)
				consecutiveNoWrites = 0 // Reset counter
			} else {
				// No writes detected
				consecutiveNoWrites++
				if consecutiveNoWrites >= 2 {
					pm.logger.InfoContext(ctx, "No write activity detected for 2 consecutive checks, exiting drain early")
					drainComplete = true
				}
			}
		}
	}

	return nil
}

// checkPromotionState checks the current state to determine what steps remain
func (pm *MultiPoolerManager) checkPromotionState(ctx context.Context) (*promotionState, error) {
	state := &promotionState{}

	// Check PostgreSQL promotion state
	isInRecovery, err := pm.isInRecovery(ctx)
	if err != nil {
		pm.logger.ErrorContext(ctx, "Failed to check recovery status", "error", err)
		return nil, mterrors.Wrap(err, "failed to check recovery status")
	}

	state.isPrimaryInPostgres = !isInRecovery

	if state.isPrimaryInPostgres {
		// Get current primary LSN
		state.currentLSN, err = pm.getPrimaryLSN(ctx)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to get current LSN", "error", err)
			return nil, err
		}
	}

	// Check topology state
	pm.mu.Lock()
	poolerType := pm.record.Type()
	pm.mu.Unlock()

	state.isPrimaryInTopology = (poolerType == clustermetadatapb.PoolerType_PRIMARY)

	pm.logger.InfoContext(ctx, "Checked promotion state",
		"is_primary_in_postgres", state.isPrimaryInPostgres,
		"is_primary_in_topology", state.isPrimaryInTopology)

	return state, nil
}

// promoteStandbyToPrimary calls pg_promote() and waits for promotion to complete.
// coordinatorTerm is the term being promoted to; it scopes the async
// post-promotion checkpoint's rewind-ready mark so a later re-promotion can't
// inherit a stale mark.
func (pm *MultiPoolerManager) promoteStandbyToPrimary(ctx context.Context, state *promotionState, coordinatorTerm int64) error {
	// Return early if already promoted
	if state.isPrimaryInPostgres {
		pm.logger.InfoContext(ctx, "PostgreSQL already promoted, skipping")
		return nil
	}

	ctx, span := telemetry.Tracer().Start(ctx, "consensus/pg-promote")
	defer span.End()

	// Call pg_promote() to promote standby to primary
	pm.logger.InfoContext(ctx, "PostgreSQL promotion needed")
	pm.logger.InfoContext(ctx, "Calling pg_promote() to promote standby to primary")
	pm.promotionInProgress.Store(true)

	// Broadcast immediately so subscribers (multiorch) see PROMOTING server
	// status before the periodic health stream interval fires. Without this,
	// the flag may be set and cleared within a single interval, making it
	// invisible to subscribers. We reset the promotionInProgress flag and
	// broadcast again after promotion completes to ensure the full window is
	// visible.
	pm.broadcastHealth()

	// TODO: this defer fires before configureReplicationAfterPromotion and
	// rules.UpdateRule in the caller, so multiorch sees PRIMARY status while sync
	// replication is not yet configured and rule history has not been written. If
	// we want the PROMOTING window to cover the full promotion path (including the
	// sync standby ack gate), the defer should move to the caller instead.
	defer func() {
		pm.promotionInProgress.Store(false)
		pm.broadcastHealth()
	}()

	if err := pm.exec(ctx, "SELECT pg_promote()"); err != nil {
		pm.logger.ErrorContext(ctx, "Failed to call pg_promote()", "error", err)
		return mterrors.Wrap(err, "failed to promote standby")
	}

	// Wait for promotion to complete: pg_is_in_recovery()=false AND postgres_ready=true.
	// Keeping promotionInProgress set until postgres_ready ensures multiorch suppresses
	// PrimaryIsDead for the full window — including the gap between pg_is_in_recovery()=false
	// and postgres actually accepting connections.
	pm.logger.InfoContext(ctx, "Waiting for promotion to complete")
	if err := pm.waitForPromotionComplete(ctx); err != nil {
		return err
	}

	// Checkpoint onto the new timeline so the control file advertises it, making
	// this node a safe pg_rewind source. PostgreSQL's post-promotion checkpoint is
	// lazy (a background CHECKPOINT_END_OF_RECOVERY), so right after promotion the
	// control file's "Latest checkpoint's TimeLineID" still reflects the OLD
	// timeline; a follower that pg_rewinds against this node would copy that stale
	// TLI into its own minRecoveryPoint and FATAL on startup.
	//
	// Run it asynchronously: a checkpoint can be slow (proportional to dirty
	// buffers since the last one) and must not block the failover path. This is
	// safe because a diverged follower's rewind is gated on this node advertising
	// rewind_ready. On completion we mark rewind_ready for the term we promoted to
	// (the atomic term check skips it if a newer term has since replaced the
	// record); the postgres monitor is the backstop that marks within ~100ms once
	// it observes the completed checkpoint.
	checkpointCtx := pm.ctx
	go func() {
		if err := pm.exec(checkpointCtx, "CHECKPOINT"); err != nil {
			pm.logger.WarnContext(checkpointCtx, "Async post-promotion checkpoint failed; rewind-readiness will be delayed until PostgreSQL's own checkpoint completes", "error", err)
			return
		}
		if pm.consensusMgr.MarkSelfRewindReady(pm.serviceID, coordinatorTerm) {
			pm.logger.InfoContext(checkpointCtx, "Post-promotion checkpoint complete; advertising rewind-ready", "coordinator_term", coordinatorTerm)
			pm.broadcastHealth()
		}
	}()

	// Promotion supersedes any pending rewind from a prior emergency demotion:
	// the consensus protocol picked this node as the new leader at a higher
	// term, so its WAL is by definition the rule going forward. Clear the
	// flag so the postgres monitor and other operations resume.
	if changed, err := pm.consensusMgr.SetSuspectedDivergence(ctx, false); err != nil {
		pm.logger.ErrorContext(ctx, "failed to clear suspected divergence before promotion", "error", err)
	} else if changed {
		pm.logger.InfoContext(ctx, "Cleared suspectedDivergence before promotion")
	}

	// Clear primary_conninfo after promotion to prevent accidental replication on restart
	if err := pm.resetPrimaryConnInfo(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear primary_conninfo after promotion", "error", err)
		// Log but don't fail - promotion already succeeded
	}

	// After a failover PostgreSQL resets unlogged tables to empty. Best-effort drop
	// them so clients hit a clear "relation does not exist" error and rebuild from
	// scratch, rather than silently reading an empty table. Runs here, after the node
	// is a writable primary but before the topology flips to PRIMARY/SERVING, so clients
	// never observe the empty intermediate state. See dropUnloggedTablesAfterPromotion.
	pm.dropUnloggedTablesAfterPromotion(ctx)

	return nil
}

// dropUnloggedTablesAfterPromotion best-effort drops every unlogged table on the
// freshly promoted primary.
//
// Unlogged table data is never replicated to standbys, so on promotion PostgreSQL
// resets these tables to empty. Leaving them in place would silently present an empty
// table to clients; dropping them instead surfaces a clear "relation does not exist"
// error that signals clients to rebuild the table (and everything derived from it)
// from scratch.
//
// The drop is best effort and deliberately avoids CASCADE: a table referenced by a
// view or function cannot be dropped without CASCADE, and we never want to destroy
// dependent user objects. Such tables are left as-is (empty), and every failure is
// logged but never fails the promotion.
func (pm *MultiPoolerManager) dropUnloggedTablesAfterPromotion(ctx context.Context) {
	// format('%I.%I', ...) returns a properly quoted, fully qualified identifier, so
	// the name is safe to interpolate into the DROP statement below.
	const listSQL = `
		SELECT format('%I.%I', n.nspname, c.relname)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relpersistence = 'u'
		  AND c.relkind = 'r'
		  AND n.nspname NOT IN ('pg_catalog', 'information_schema')`

	result, err := pm.query(ctx, listSQL)
	if err != nil {
		pm.logger.WarnContext(ctx, "Failed to list unlogged tables after promotion; skipping drop", "error", err)
		return
	}
	if result == nil || len(result.Rows) == 0 {
		return
	}

	for _, row := range result.Rows {
		name, err := executor.GetString(row, 0)
		if err != nil {
			pm.logger.WarnContext(ctx, "Failed to parse unlogged table name after promotion; skipping", "error", err)
			continue
		}
		if err := pm.exec(ctx, "DROP TABLE "+name); err != nil {
			pm.logger.WarnContext(ctx, "Best-effort drop of unlogged table after promotion failed; table left empty",
				"table", name, "error", err)
			continue
		}
		pm.logger.InfoContext(ctx, "Dropped unlogged table after promotion", "table", name)
	}
}

// waitForPromotionComplete polls until the node has left recovery mode AND postgres
// is accepting connections. Both conditions are required: pg_is_in_recovery()=false
// confirms the WAL-level promotion, and postgres_ready=true confirms clients can
// connect. Clearing promotionInProgress only when both are true ensures multiorch's
// PrimaryIsDeadAnalyzer suppression window matches the full visibility gap.
func (pm *MultiPoolerManager) waitForPromotionComplete(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	promotionTimeout := 30 * time.Second
	promotionCtx, cancel := context.WithTimeout(ctx, promotionTimeout)
	defer cancel()

	promotedFromRecovery := false
	for {
		select {
		case <-promotionCtx.Done():
			pm.logger.ErrorContext(ctx, "Timeout waiting for promotion to complete")
			return mterrors.New(mtrpcpb.Code_DEADLINE_EXCEEDED,
				fmt.Sprintf("timeout waiting for promotion to complete after %v", promotionTimeout))

		case <-ticker.C:
			if !promotedFromRecovery {
				isInRecovery, err := pm.isInRecovery(promotionCtx)
				if err != nil {
					pm.logger.ErrorContext(ctx, "Failed to check recovery status during promotion", "error", err)
					return mterrors.Wrap(err, "failed to check recovery status")
				}
				if !isInRecovery {
					pm.logger.InfoContext(ctx, "Postgres left recovery mode, waiting for connections to be accepted")
					promotedFromRecovery = true
				}
			}

			if promotedFromRecovery && pm.isPostgresReady(promotionCtx) {
				pm.logger.InfoContext(ctx, "Promotion completed successfully - node is now primary and accepting connections")
				return nil
			}
		}
	}
}

// updateTopologyAfterPromotion updates the pooler type in topology to PRIMARY
// and transitions the serving state to SERVING.
//
// The serving-state transition must always run, even when the topology already
// reports PRIMARY. That can happen on re-promotion of the same pooler at a
// higher term: demoteToStandbyLocked left the topology Type=PRIMARY (only
// stale-primary demote updates topology) but transitioned serving status to
// DRAINING. Skipping the Mutate call left the pooler stuck at
// PRIMARY/DRAINING, which prevented the multigateway buffer from draining
// after the failover.
func (pm *MultiPoolerManager) updateTopologyAfterPromotion(ctx context.Context, state *promotionState, rule *clustermetadatapb.ShardRule) error {
	if state.isPrimaryInTopology {
		pm.logger.InfoContext(ctx, "Topology type already PRIMARY; ensuring serving status is SERVING")
	} else {
		pm.logger.InfoContext(ctx, "Updating pooler type in topology to PRIMARY")
	}

	// Promotion has already waited for postgres to leave recovery AND for the new
	// rule to commit (DoUpdateRule blocks on the sync-standby ack before this
	// runs), so the consensus snapshot now names this pooler the active committed
	// leader: poking PostgresPrimary here derives routing role PRIMARY and its
	// leadership observation, letting the heartbeat writer / LISTEN and the gateway
	// start immediately rather than waiting for the next monitor tick. Mutate is
	// idempotent — if already at this state it short-circuits.
	if err := pm.stateManager.Mutate(ctx, func(s *servingStateMutation) {
		s.PostgresPrimary = true
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_SERVING
	}); err != nil {
		return mterrors.Wrap(err, "failed to set serving state for promotion")
	}

	return nil
}

// ReplicationLag returns the current replication lag from the heartbeat reader
func (pm *MultiPoolerManager) ReplicationLag(ctx context.Context) (time.Duration, error) {
	if err := pm.checkReady(); err != nil {
		return 0, err
	}

	if pm.replTracker == nil {
		return 0, mterrors.New(mtrpcpb.Code_UNAVAILABLE, "replication tracker not initialized")
	}

	return pm.replTracker.HeartbeatReader().Status()
}

// Start initializes the MultiPoolerManager
func (pm *MultiPoolerManager) Start(senv *servenv.ServEnv) {
	// Open performs a state transition that publishes through pm.record.Mutate,
	// which requires an action lock. At startup no other actions are running,
	// so Acquire should always succeed; an error here would indicate a coding
	// bug (typically re-entrance) and we surface it loudly.
	// TODO: This should be managed by a proper state manager (like tm_state.go)
	lockCtx, err := pm.actionLock.Acquire(pm.ctx, "Start")
	if err != nil {
		pm.logger.ErrorContext(pm.ctx, "Failed to acquire action lock for Start — re-entrance bug?", "error", err)
		return
	}
	pm.Open(lockCtx)
	pm.actionLock.Release(lockCtx)

	// Register the SIGTERM-driven graceful shutdown sequence. Runs as an
	// OnTermSync hook so it is bounded by the lameduck window and completes
	// before OnClose hooks (topology unregister) fire.
	senv.OnTermSync(func() {
		pm.GracefulShutdown(pm.ctx)
	})

	// Start loading multipooler record from topology asynchronously
	go pm.loadShardConfigFromGlobalTopo()

	senv.OnRunE(func() error {
		// Block until manager is ready or error before registering gRPC services
		// Use load timeout from manager configuration
		waitCtx, cancel := context.WithTimeout(pm.ctx, pm.loadTimeout)
		defer cancel()

		pm.logger.Info("Waiting for manager to reach ready state before registering gRPC services")
		if err := pm.WaitUntilReady(waitCtx); err != nil {
			pm.logger.Error("Manager failed to reach ready state during startup", "error", err)
			return fmt.Errorf("manager failed to reach ready state: %w", err)
		}
		pm.logger.Info("Manager reached ready state, will register gRPC services")

		pm.logger.Info("MultiPoolerManager started")
		pm.qsc.RegisterGRPCServices()
		pm.logger.Info("Query service controller registered")

		// Register manager gRPC services
		pm.registerGRPCServices()
		pm.logger.Info("MultiPoolerManager gRPC services registered")
		return nil
	})
}

// StartBackupHealth opts this manager into the background backup-health poller
// and launches it. Once the manager reaches its ready state, it captures an
// initial snapshot so the gauges/status page reflect post-bootstrap state
// without waiting a poll interval.
//
// The poller is bound to the open/close lifecycle (pm.ctx), not the manager
// lifetime: closeLocked cancels pm.ctx and openLocked recreates it, so the
// poller is paused during maintenance (e.g. pg_rewind, when the connection
// pool is closed) and relaunched on resume. Setting backupHealthEnabled makes
// openLocked restart the poller on every subsequent open.
//
// This is invoked by the multipooler service rather than from Start() so that
// manager RPC/consensus unit tests, which drive Start() directly against a
// strict mock DB, do not spin up background health queries (pg_is_in_recovery,
// pg_settings, pg_stat_archiver) that would race with their query expectations.
//
// Idempotent: only the first call takes effect. A second call is a no-op so a
// double wire-up cannot leak a duplicate poller goroutine (openLocked owns
// relaunch from here on).
func (pm *MultiPoolerManager) StartBackupHealth() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.backupHealthEnabled {
		return
	}
	pm.backupHealthEnabled = true
	pm.startBackupHealthPollerLocked()
}

// startBackupHealthPollerLocked launches the backup-health poller goroutine on
// the current pm.ctx, plus a one-shot goroutine that captures an initial
// snapshot once the manager is ready. Both stop when pm.ctx is cancelled.
//
// Caller must hold pm.mu (read of pm.ctx). Invoked from StartBackupHealth (first
// launch) and from openLocked on every reopen when backupHealthEnabled is set.
func (pm *MultiPoolerManager) startBackupHealthPollerLocked() {
	ctx := pm.ctx
	go pm.backup.RunHealthPoller(ctx, 0)
	go func() {
		if err := pm.WaitUntilReady(ctx); err != nil {
			return // ctx cancelled before ready (shutdown); nothing to refresh
		}
		pm.backup.RefreshHealthNow(ctx)
	}()
}

// StartTopoRegistration starts the publisher goroutine and kicks off the
// pooler's initial topology registration via toporeg.Register (with async
// retry + alarm). The publisher runs from here until StopTopoRegistration —
// manager open/close cycles (Pause / resume) do not affect it, so the
// topology entry continues to reflect state changes throughout.
//
// Wire alarm to the service's status page so registration failures are
// surfaced to operators. Idempotent: only the first call takes effect.
func (pm *MultiPoolerManager) StartTopoRegistration(alarm func(string)) {
	pm.record.Register(pm.shutdownCtx, alarm)
}

// StopTopoRegistration transitions the pooler to its shutdown topology
// state (Type=UNKNOWN, ServingStatus=DISABLED, LifecycleStatus=SHUTDOWN),
// stops the publisher with a final publish, and cancels the toporeg retry
// goroutine. Safe to call even if StartTopoRegistration was never invoked.
//
// Caller controls the deadline via ctx. ctx should NOT inherit from
// pm.shutdownCtx, which GracefulShutdown cancels — by the time OnClose
// fires, that would block the shutdown write. Pass a detached, bounded ctx.
//
// Type is set to UNKNOWN. LifecycleStatus=LIFECYCLE_SHUTDOWN
// is the authoritative shutdown signal — administrative views and the
// orchestrator key off it, not off Type. It is what the orchestrator's pooler
// watcher reacts to in order to close the per-pooler health stream; without
// it, the orchestrator would dial the dead address for ~4 h until
// forgetLongUnseenInstances tore down the cache entry. The entry itself is
// left in place; the 4 h bookkeeping handles eventual cleanup. On restart the
// pooler re-registers with PoolerType_REPLICA and
// LifecycleStatus=LIFECYCLE_STARTING, and the orchestrator promotes as
// usual.
//
// Does not require the manager's action lock — record.Unregister stops the
// publisher before applying the finalize callback, so no other goroutine
// can publish over our shutdown state regardless of locking.
func (pm *MultiPoolerManager) StopTopoRegistration(ctx context.Context) {
	pm.record.Unregister(ctx, func(s *MutablePoolerRecordState) {
		s.Type = clustermetadatapb.PoolerType_UNKNOWN
		s.SelfLeadership = nil
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
		s.LifecycleStatus = &clustermetadatapb.PoolerLifecycle{
			Status:  clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			Reason:  "pooler shutdown",
			Updated: timestamppb.Now(),
		}
	})
}

// WaitUntilReady blocks until the manager reaches Ready or Error state, or
// the context is cancelled. Returns nil if Ready, or an error if Error state
// or context cancelled. This should be called after Start() to ensure
// initialization is complete before accepting RPC requests.
//
// Thread-safety: This method waits on a channel that is closed when the state
// changes to Ready or Error, allowing efficient notification without polling.
func (pm *MultiPoolerManager) WaitUntilReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for manager ready cancelled: %w", ctx.Err())
	case <-pm.readyChan:
		// State has changed to Ready or Error, check which one
		pm.mu.Lock()
		state := pm.state
		stateError := pm.stateError
		pm.mu.Unlock()

		switch state {
		case ManagerStateReady:
			pm.logger.InfoContext(ctx, "Manager is ready")
			return nil
		case ManagerStateError:
			pm.logger.ErrorContext(ctx, "Manager failed to initialize", "error", stateError)
			return fmt.Errorf("manager is in error state: %w", stateError)
		default:
			// This shouldn't happen - channel was closed but state isn't terminal
			return fmt.Errorf("unexpected state after ready signal: %s", state)
		}
	}
}
