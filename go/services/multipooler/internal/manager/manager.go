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
	commonconsensus "github.com/multigres/multigres/go/common/consensus"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
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
	record         *poolerRecord
	state          ManagerState
	stateError     error
	consensusState *consensus.ConsensusState
	topoLoaded     bool
	rules          consensus.RuleStorer
	ctx            context.Context
	cancel         context.CancelFunc
	loadTimeout    time.Duration

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

	// resignedLeaderAtTerm is set when this node voluntarily resigns as primary
	// (via Recruit's emergency-demote path or graceful shutdown). The value is
	// the consensus term at which the primary resigned. A non-zero value signals
	// the coordinator to trigger an immediate election. Protected by mu. Cleared
	// when this node is elected primary again.
	resignedLeaderAtTerm int64

	// cohortEligibility is this node's self-reported willingness to be a member
	// of the consensus cohort. Defaults to ELIGIBLE; published in every health
	// snapshot via AvailabilityStatus.CohortEligibilityStatus. Protected by mu.
	cohortEligibility clustermetadatapb.CohortEligibilitySignal

	// pgMonitor manages the PostgreSQL monitoring loop.
	pgMonitor *timer.PeriodicRunner

	// rewindPending suppresses the postgres monitor after emergency demotion until a
	// rewind completes successfully. Set by emergencyDemoteLocked, cleared by RewindToSource.
	rewindPending atomic.Bool

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

	// servingState coordinates serving state transitions across components
	// (query service, heartbeat tracker) and updates the multipooler record.
	servingState *StateManager

	// backup is the pgBackRest engine. The manager orchestrates lifecycle and
	// owns the gRPC handlers, delegating the pgBackRest steps to the engine.
	backup *backupengine.Engine

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
	isNotServing        bool   // PoolerServingStatus == NOT_SERVING (includes heartbeat stopped)
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

	pm := &MultiPoolerManager{
		logger:                 logger.With("pooler_name", multiPooler.Id.GetName()),
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
		cohortEligibility:      clustermetadatapb.CohortEligibilitySignal_COHORT_ELIGIBILITY_SIGNAL_ELIGIBLE,
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

	// Load consensus state from disk. Missing file means term=0 (new node), which is fine.
	// Only actual read/parse errors fail the constructor.
	pm.consensusState = consensus.NewConsensusState(pm.record.PoolerDir(), pm.serviceID)
	if config.ConsensusEnabled {
		if _, err := pm.consensusState.Load(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to load consensus state from disk: %w", err)
		}
	}

	// Create the query service controller with the pool manager.
	// Get the drain grace period from connpool config (0 means use default).
	var drainGracePeriod time.Duration
	if config.ConnPoolConfig != nil {
		drainGracePeriod = config.ConnPoolConfig.DrainGracePeriod()
	}
	pm.qsc = poolerserver.NewQueryPoolerServer(logger, connPoolMgr, multiPooler.Id, multiPooler.GetShardKey().GetTableGroup(), multiPooler.GetShardKey().GetShard(), pm, drainGracePeriod, config.VpidStampEnabled)
	pm.rules = consensus.NewRuleStore(pm.logger, pm.qsc.InternalQueryService(), consensus.NewSyncStandbyManager(pm.logger, pm.qsc.InternalQueryService(), multiPooler.Id))

	// The health streamer must wait for the query server to update its type before
	// broadcasting SERVING transitions, so the gateway doesn't discover the new
	// primary before the pooler is ready to accept requests for that type.
	pm.healthStreamer.SetQueryServer(pm.qsc)

	// Create the serving state manager with the query service and health streamer as initial components.
	// The ReplTracker is registered later when heartbeat is started.
	pm.servingState = NewStateManager(logger, pm.record, pm.qsc, pm.healthStreamer)

	// Construct the pgBackRest engine. It owns all pgBackRest interaction and its
	// own metrics. The pgbackrest.conf path, pgpass file, and repo config are
	// supplied later (SetConfigPath / SetPgpassPath / SetBackupConfig) once
	// topology and the backup location have loaded.
	pm.backup = backupengine.NewEngine(pm.logger, pm.runLongCommand, pm.record, backupengine.Settings{
		PgpassPath: pm.pgpassPath,
		PgDataDir:  postgresDataDir(),
	})

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
	return pm.rules.UpdateRule(ruleWriteCtx, update)
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
// demote, which Pauses while the pooler is intentionally NOT_SERVING and
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
	pm.pgMonitor.Start(func(ctx context.Context) {
		if newState, err := pm.monitorPostgresIteration(ctx); err == nil {
			// Broadcast postgres health transitions so orchestrators learn
			// about changes immediately without waiting for the next 30-second
			// heartbeat. This is especially important for:
			//   - postgres going down: allows PrimaryIsDeadAnalyzer to detect failure promptly
			//   - postgres coming back up: allows FixReplication to see IsInitialized=true quickly
			if !postgresStateEqual(newState, prevState) {
				pm.logger.InfoContext(ctx, "MonitorPostgres: postgres state changed, broadcasting health",
					"postgres_running", newState.postgresRunning,
					"postgres_ready", newState.postgresReady)
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
	}, nil)
	pm.logger.InfoContext(pm.ctx, "MonitorPostgres enabled successfully")

	pm.isOpen = true

	// Start health heartbeat goroutine and transition to the target status.
	// SetState notifies all components (query service, heartbeat, health
	// streamer) and Mutates the record. The publisher (if running, started
	// by StartTopoRegistration) picks it up and writes to etcd.
	go pm.runHealthHeartbeat(pm.ctx, timeouts.DefaultHealthHeartbeatInterval)
	if err := pm.servingState.SetState(ctx, pm.record.Type(), pm.record.SelfLeadership(), targetServingStatus); err != nil {
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
// while the pooler is intentionally NOT_SERVING: a hard-coded
// "resume → SERVING" would publish a transient (PRIMARY, SERVING) to etcd
// between the Pause and the subsequent ChangeType to REPLICA, which the
// gateway would briefly cache as the new primary.
//
// Note: Pause() cancels the manager's context, but openLocked creates a fresh one.
func (pm *MultiPoolerManager) Pause(ctx context.Context) (resume func(context.Context)) {
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
	pm.closeLocked(lockCtx, "shutdown")
}

// closeLocked performs the actual close operation.
// Returns true if the manager was open and is now closed, false if it was already closed.
// Caller should NOT hold pm.mu - this function acquires it.
// Always cancels the context - Open() will create a fresh one if reopened.
//
// ctx must carry an action lock. The state transition (NOT_SERVING) publishes
// through pm.record.Mutate.
func (pm *MultiPoolerManager) closeLocked(ctx context.Context, logMessage string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if !pm.isOpen {
		return false
	}

	// Transition to NOT_SERVING before closing resources. This notifies all
	// components: query service rejects queries, heartbeat stops, health
	// streamer broadcasts NOT_SERVING to subscribers. The publisher (if
	// running) picks up the Mutate and writes NOT_SERVING to etcd —
	// pausing the manager intentionally still reflects in topology so
	// callers see the pooler is not serving queries.
	if err := pm.servingState.SetState(ctx, pm.record.Type(), pm.record.SelfLeadership(), clustermetadatapb.PoolerServingStatus_NOT_SERVING); err != nil {
		pm.logger.WarnContext(ctx, "Failed to transition to NOT_SERVING during close", "error", err)
	}

	pm.pgMonitor.Stop()
	pm.closeConnectionsLocked(false /* forReopen */)
	pm.cancel()
	pm.isOpen = false
	pm.logger.InfoContext(ctx, "MultiPoolerManager: "+logMessage)
	return true
}

// startHeartbeat starts the replication tracker and syncs it to the current serving state.
// The heartbeat writer/reader mode is determined by the multipooler record (the source of truth),
// not by querying the database directly. If the type later changes (e.g., via promotion or
// topology load), StateManager.SetState will notify the replTracker.
func (pm *MultiPoolerManager) startHeartbeat(ctx context.Context, shardID []byte, poolerID string) error {
	// Create the replication tracker using the executor's InternalQueryService
	pm.replTracker = heartbeat.NewReplTracker(pm.qsc.InternalQueryService(), pm.logger, shardID, poolerID, pm.config.HeartbeatIntervalMs)

	// Register with StateManager and sync to current state. This ensures the
	// heartbeat writer starts for PRIMARY or the reader starts for REPLICA,
	// based on whatever state the StateManager currently holds.
	return pm.servingState.RegisterAndSync(ctx, pm.replTracker)
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
	return pm.servingState.RegisterAndSync(ctx, pm.pubsubListener)
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

// leaderObs builds the LeaderObservation this pooler records when it is the
// leader under rule. The caller supplies the rule it knows names this pooler as
// leader — the proposed rule on promotion, the recorded rule in the monitor —
// so the source is explicit rather than read implicitly here.
func (pm *MultiPoolerManager) leaderObs(rule *clustermetadatapb.ShardRule) *clustermetadatapb.LeaderObservation {
	return &clustermetadatapb.LeaderObservation{
		LeaderId:         rule.GetLeaderId(),
		LeaderRuleNumber: rule.GetRuleNumber(),
	}
}

// shardKey returns a ShardKey identifying this pooler's shard.
func (pm *MultiPoolerManager) shardKey() *clustermetadatapb.ShardKey {
	return pm.record.ShardKey()
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

// getCurrentTermNumber returns the current consensus term number.
func (pm *MultiPoolerManager) getCurrentTermNumber(ctx context.Context) (int64, error) {
	return pm.consensusState.GetCurrentTermNumber(ctx)
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

		// Set initial leader observation from the highest known rule.
		// commonconsensus.LeaderTerm returns 0 unless the rule names us as the
		// leader, so publishing serviceID here is safe.
		if cs, err := pm.getInconsistentConsensusStatus(pm.ctx); err == nil {
			if primaryTerm := commonconsensus.LeaderTerm(cs); primaryTerm > 0 {
				pm.healthStreamer.UpdateLeaderObservation(&poolerserver.LeaderObservation{
					LeaderID:   pm.serviceID,
					LeaderTerm: primaryTerm,
				})
			}
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

// validateAndUpdateTerm validates the request term against the current term following Consensus rules.
// Returns an error if the request term is stale (less than current term).
// If the request term is higher, it updates the term in pgctld and the cache.
// If force is true, validation is skipped.
func (pm *MultiPoolerManager) validateAndUpdateTerm(ctx context.Context, requestTerm int64, force bool) error {
	if force {
		return nil // Skip validation if force is set
	}

	currentTerm, err := pm.getCurrentTermNumber(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current term: %w", err)
	}

	// If request term == current term: ACCEPT (same term, execute)
	// If request term < current term: REJECT (stale request)
	// If request term > current term: UPDATE term and ACCEPT (new term discovered)
	if requestTerm < currentTerm {
		// Request has stale term, reject
		pm.logger.ErrorContext(ctx, "Consensus term too old, rejecting request",
			"request_term", requestTerm,
			"current_term", currentTerm,
			"service_id", pm.serviceID.String())
		return mterrors.New(mtrpcpb.Code_FAILED_PRECONDITION,
			fmt.Sprintf("consensus term too old: request term %d is less than current term %d (use force=true to bypass)",
				requestTerm, currentTerm))
	} else if requestTerm > currentTerm {
		// Request has newer term, update our term
		pm.logger.InfoContext(ctx, "Discovered newer term, updating",
			"request_term", requestTerm,
			"old_term", currentTerm,
			"service_id", pm.serviceID.String())

		// Update term atomically (resets accepted leader)
		if err := pm.consensusState.UpdateTermAndSave(ctx, requestTerm); err != nil {
			pm.logger.ErrorContext(ctx, "Failed to update term", "error", err)
			return mterrors.Wrap(err, "failed to update consensus term")
		}

		pm.logger.InfoContext(ctx, "Consensus term updated successfully", "new_term", requestTerm)
	}
	// If requestTerm == currentCachedTerm, just continue (same term is OK)
	return nil
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
	state.isNotServing = (servingStatus == clustermetadatapb.PoolerServingStatus_NOT_SERVING)

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
		"is_not_serving", state.isNotServing,
		"is_replica_in_topology", state.isReplicaInTopology,
		"is_read_only", state.isReadOnly,
		"is_primary", isPrimary,
		"pooler_type", poolerType,
		"serving_status", servingStatus)

	return state, nil
}

// setNotServing transitions the pooler to NOT_SERVING status during demotion.
// This uses the StateManager to coordinate:
//   - Query service rejects all queries
//   - Heartbeat writer is stopped
//   - Multipooler record is updated
//
// After this, topology is synced to persist the state change.
func (pm *MultiPoolerManager) setNotServing(ctx context.Context, state *demotionState) error {
	if state.isNotServing {
		pm.logger.InfoContext(ctx, "Already in NOT_SERVING state, skipping")
		return nil
	}

	pm.logger.InfoContext(ctx, "Transitioning to NOT_SERVING")

	// Use the serving state manager to transition components.
	// This updates query service, heartbeat, and the pooler record. Mutate
	// inside StateManager schedules an async publish to topology.
	if err := pm.servingState.SetState(ctx, pm.record.Type(), pm.record.SelfLeadership(), clustermetadatapb.PoolerServingStatus_NOT_SERVING); err != nil {
		return mterrors.Wrap(err, "failed to transition to NOT_SERVING")
	}

	pm.logger.InfoContext(ctx, "Transitioned to NOT_SERVING successfully")
	return nil
}

// restartPostgresAsStandby restarts PostgreSQL as a standby server
// This creates standby.signal and restarts PostgreSQL via pgctld
//
// TODO: require callers to declare whether this restart is a "clean" or
// "unexpected" demote. A clean demote (graceful failover handoff, known
// consistent WAL) should leave rewindPending=false. An unexpected demote
// (crash, external pg_demote, monitor-driven restart after an unknown
// shutdown) should set rewindPending=true so that the next standby-side
// operation (SetPrimary's standby branch, remedialActionFixPrimaryConnInfo,
// or self-rewind detection) routes through pg_rewind dry-run before
// trusting local WAL. Today the only setter is emergencyDemoteLocked,
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

// promoteStandbyToPrimary calls pg_promote() and waits for promotion to complete
func (pm *MultiPoolerManager) promoteStandbyToPrimary(ctx context.Context, state *promotionState) error {
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

	// Promotion supersedes any pending rewind from a prior emergency demotion:
	// the consensus protocol picked this node as the new leader at a higher
	// term, so its WAL is by definition the rule going forward. Clear the
	// flag so the postgres monitor and other operations resume.
	if pm.rewindPending.Swap(false) {
		pm.logger.InfoContext(ctx, "Cleared rewindPending before promotion")
	}

	// Clear primary_conninfo after promotion to prevent accidental replication on restart
	if err := pm.resetPrimaryConnInfo(ctx); err != nil {
		pm.logger.WarnContext(ctx, "Failed to clear primary_conninfo after promotion", "error", err)
		// Log but don't fail - promotion already succeeded
	}

	return nil
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
// higher term: emergencyDemoteLocked left the topology Type=PRIMARY (only
// stale-primary demote updates topology) but transitioned serving status to
// NOT_SERVING. Skipping the SetState call left the pooler stuck at
// PRIMARY/NOT_SERVING, which prevented the multigateway buffer from draining
// after the failover.
func (pm *MultiPoolerManager) updateTopologyAfterPromotion(ctx context.Context, state *promotionState, rule *clustermetadatapb.ShardRule) error {
	if state.isPrimaryInTopology {
		pm.logger.InfoContext(ctx, "Topology type already PRIMARY; ensuring serving status is SERVING")
	} else {
		pm.logger.InfoContext(ctx, "Updating pooler type in topology to PRIMARY")
	}

	// rule is the rule this pooler was just promoted under; it names this pooler
	// as leader, so the leadership observation built from it is authoritative.
	// SetState is idempotent — if already at PRIMARY/SERVING it short-circuits.
	if err := pm.servingState.SetState(ctx, clustermetadatapb.PoolerType_PRIMARY, pm.leaderObs(rule), clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
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
// state (Type=UNKNOWN, ServingStatus=NOT_SERVING, LifecycleStatus=SHUTDOWN),
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
		s.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
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

// postgresState represents the state of PostgreSQL for monitoring.
//
// postgresReady is true when postgres is RUNNING and pg_isready passes —
// i.e. it is accepting connections. A node whose postgres is not ready has
// no replication running and cannot participate in write quorum.
type postgresState struct {
	pgctldAvailable          bool
	dirInitialized           bool
	postgresRunning          bool
	postgresReady            bool
	backupsAvailable         bool
	isPrimary                bool
	bootstrapSentinelPresent bool
	// primaryTerm is the coordinator term at which this pooler is the primary
	// per the highest known rule. 0 if we are not the primary for that rule.
	primaryTerm int64
}

// postgresStateEqual reports whether two postgresState values are identical.
func postgresStateEqual(a, b postgresState) bool {
	return a.pgctldAvailable == b.pgctldAvailable &&
		a.dirInitialized == b.dirInitialized &&
		a.postgresRunning == b.postgresRunning &&
		a.postgresReady == b.postgresReady &&
		a.backupsAvailable == b.backupsAvailable &&
		a.isPrimary == b.isPrimary &&
		a.bootstrapSentinelPresent == b.bootstrapSentinelPresent &&
		a.primaryTerm == b.primaryTerm
}

// remedialAction represents actions the postgres monitor can take
type remedialAction int

const (
	remedialActionNone remedialAction = iota
	remedialActionStartPostgres
	remedialActionRestoreFromBackup
	remedialActionAdjustTypeToPrimary
	remedialActionAdjustTypeToReplica
	remedialActionCreateFirstBackup
	remedialActionReconcileGUC
	// remedialActionFixPrimaryConnInfo means postgres is in recovery and the
	// topology says REPLICA, but primary_conninfo doesn't match the primary
	// recorded in consensus.ConsensusState.ReplicationPrimary (the most recent
	// SetPrimary/Promote). Reconciles the GUC so this replica points at the right
	// primary regardless of how it got out of sync (failed SetPrimary apply,
	// hand edit, snapshot restore, etc.).
	remedialActionFixPrimaryConnInfo
)

// monitorPostgresIteration performs one iteration of PostgreSQL monitoring.
// Returns the discovered postgres state on success, or an error if the state
// could not be determined. The caller is responsible for transition detection
// and broadcasting health updates.
func (pm *MultiPoolerManager) monitorPostgresIteration(ctx context.Context) (postgresState, error) {
	const (
		reasonPgctldUnavailable = "pgctld_unavailable"
		reasonPostgresRunning   = "postgres_running"
	)

	// Wait for manager to be ready
	if err := pm.checkReady(); err != nil {
		pm.logger.InfoContext(pm.ctx, "MonitorPostgres: manager not ready yet")
		return postgresState{}, err
	}

	// Skip all action while awaiting a rewind after emergency demotion.
	if pm.rewindPending.Load() {
		pm.logger.InfoContext(ctx, "MonitorPostgres: skipping, awaiting rewind after demotion")
		return postgresState{}, nil
	}

	// Discover current state
	currentState, err := pm.discoverPostgresState(ctx)
	if err != nil {
		// Log and skip this tick; the next iteration will retry. A persistent
		// failure keeps the error loud rather than silently triggering the wrong
		// remediation. pgctld unavailability gets its dedicated reason code so
		// the monitor's log-dedup path behaves as before.
		if !currentState.pgctldAvailable {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: pgctld unavailable", "error", err)
			pm.pgMonitorLastLoggedReason = reasonPgctldUnavailable
		} else {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to discover state; skipping tick", "error", err)
		}
		return postgresState{}, err
	}

	// Determine what remediation is needed
	action := pm.determineRemedialAction(ctx, currentState)
	if action == remedialActionNone {
		// No action needed - just log status
		if currentState.postgresRunning {
			pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		}
		return currentState, nil
	}

	// Acquire action lock before taking remedial action
	lockCtx, err := pm.actionLock.Acquire(ctx, "MonitorPostgres")
	if err != nil {
		pm.logger.InfoContext(ctx, "MonitorPostgres: failed to acquire action lock", "error", err)
		return postgresState{}, err
	}
	defer pm.actionLock.Release(lockCtx)

	// Re-check rewindPending after acquiring the lock: Recruit sets this flag
	// while holding the lock, so we may have been waiting while it demoted the node.
	if pm.rewindPending.Load() {
		pm.logger.InfoContext(ctx, "MonitorPostgres: skipping after lock acquire, rewind now pending")
		return postgresState{}, nil
	}

	// Re-verify state after acquiring lock (conditions may have changed)
	currentState, err = pm.discoverPostgresState(lockCtx)
	if err != nil {
		if !currentState.pgctldAvailable {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: pgctld unavailable after lock acquire", "error", err)
			pm.pgMonitorLastLoggedReason = reasonPgctldUnavailable
		} else {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to re-discover state after lock acquire; skipping tick", "error", err)
		}
		return postgresState{}, err
	}

	// Re-determine action based on current state
	action = pm.determineRemedialAction(lockCtx, currentState)

	// Take remedial action with lock held
	pm.takeRemedialAction(lockCtx, action, currentState)

	return currentState, nil
}

// discoverPostgresState discovers the current state of PostgreSQL. Returns an
// error only when a probe fails in a way that leaves the state genuinely
// ambiguous (e.g. a sentinel stat failing for reasons other than NotExist);
// callers must refuse to take remedial action in that case. pgctld being
// unavailable is not such a case — it is represented as pgctldAvailable=false.
func (pm *MultiPoolerManager) discoverPostgresState(ctx context.Context) (postgresState, error) {
	state := postgresState{}

	// Check if pgctld client is available
	if pm.pgctldClient == nil {
		return state, nil // All fields remain false
	}
	state.pgctldAvailable = true

	// Get status from pgctld. On failure return both the state (with
	// pgctldAvailable=false) and the error so the caller can log the
	// underlying cause while still reading the unavailability flag.
	statusResp, err := pm.pgctldClient.Status(ctx, &pgctldpb.StatusRequest{})
	if err != nil {
		state.pgctldAvailable = false
		return state, fmt.Errorf("pgctld status: %w", err)
	}

	// Check if directory is initialized
	state.dirInitialized = (statusResp.Status != pgctldpb.ServerStatus_NOT_INITIALIZED)

	// Check if Postgres is running and ready to accept connections.
	state.postgresRunning = (statusResp.Status == pgctldpb.ServerStatus_RUNNING)
	state.postgresReady = state.postgresRunning && statusResp.Ready
	if state.postgresRunning {
		var err error
		state.isPrimary, err = pm.isPrimary(ctx)
		if err != nil {
			pm.logger.ErrorContext(ctx, "Failed to determine primary status", "error", err)
		}
		// Lock-free first pass from the monitor: use the inconsistent read,
		// which falls back to the cached rule when postgres is unreachable.
		if cs, err := pm.getInconsistentConsensusStatus(ctx); err == nil {
			state.primaryTerm = commonconsensus.LeaderTerm(cs)
		}
	}

	// Check if backups are available (only if directory not initialized)
	if !state.dirInitialized {
		state.backupsAvailable = pm.hasCompleteBackups(ctx)
	}

	sentinelPresent, err := pm.hasBootstrapSentinel()
	if err != nil {
		// We can't tell whether a stale sentinel needs handling. Surface as an
		// error so the monitor skips this tick; the operator should investigate.
		return state, fmt.Errorf("check bootstrap sentinel: %w", err)
	}
	state.bootstrapSentinelPresent = sentinelPresent

	return state, nil
}

// primaryTermLocked returns the coordinator term of the pooler's current
// committed rule if this pooler is the primary per that rule. Uses a
// consistent consensus status read and therefore requires the action lock.
// Returns (0, nil) when the rule does not name this pooler as primary.
// Returns (0, err) only when the consensus status cannot be read at all —
// callers that need to distinguish "confirmed not primary" from "unknown"
// must inspect the error.
//
// Callers that cannot hold the action lock should read
// getInconsistentConsensusStatus themselves and pass the result to
// consensus.LeaderTerm.
func (pm *MultiPoolerManager) primaryTermLocked(ctx context.Context) (int64, error) {
	cs, err := pm.getConsensusStatus(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to read consensus status: %w", err)
	}
	return commonconsensus.LeaderTerm(cs), nil
}

// setMonitorReason sets the current monitor state reason and logs on state changes.
// This avoids log spam during repeated monitor iterations with the same state.
func (pm *MultiPoolerManager) setMonitorReason(ctx context.Context, reason, message string) {
	if pm.pgMonitorLastLoggedReason != reason {
		pm.logger.InfoContext(ctx, message)
		pm.pgMonitorLastLoggedReason = reason
	}
}

// primaryConnInfoDiffersFromRecorded returns true when this pooler has been
// informed about a primary (via SetPrimary or Promote) and the live primary_conninfo
// in postgres doesn't match the recorded primary's contact info. Returns false
// when there's nothing to compare against, when we couldn't read the GUC, or
// when the recorded primary names this pooler itself.
//
// Returns false when the recorded primary's rule is revoked by our recorded
// revocation: a Recruit that's already advanced revoked_below_term has
// deliberately cleared primary_conninfo, and the cached "recorded primary" is
// stale until the next SetPrimary/Promote updates it. Without this gate, the
// monitor would race the Recruit/Promote flow by restoring conninfo to the
// just-revoked primary, which then causes Promote to refuse with
// "primary_conninfo is set".
//
// Used by the postgres monitor to decide whether to trigger
// remedialActionFixPrimaryConnInfo on each tick.
func (pm *MultiPoolerManager) primaryConnInfoDiffersFromRecorded(_ postgresState) bool {
	// Don't detect drift we can't fix: when StopReplication has set the
	// manual-stop flag, setPrimaryConnInfoLocked refuses every conninfo
	// rewrite until StartReplication clears it. Detecting drift anyway would
	// fire the reconciliation action on every tick and log a noisy
	// FAILED_PRECONDITION error each time.
	if pm.walReceiverManuallyStopped.Load() {
		return false
	}
	rp := pm.consensusState.GetReplicationPrimary()
	if rp == nil {
		return false
	}
	target := rp.GetPrimary()
	if target == nil {
		return false
	}
	// Skip if the recorded rule names this pooler as the leader — that's the
	// primary-side case, out of scope for replica-conninfo reconciliation.
	if leader := rp.GetRule().GetLeaderId(); leader != nil &&
		leader.GetCell() == pm.serviceID.GetCell() && leader.GetName() == pm.serviceID.GetName() {
		return false
	}
	// Skip if the recorded rule is revoked. The cached primary is from before
	// the current revocation took effect; restoring conninfo to it would race
	// the Recruit/Promote flow that's mid-flight (see function doc).
	rev, err := pm.consensusState.GetInconsistentRevocation()
	if err != nil || commonconsensus.IsRuleRevoked(rp.GetRule(), rev) {
		return false
	}
	targetHost := target.GetHost()
	targetPort := target.GetPostgresPort()
	if targetHost == "" || targetPort == 0 {
		return false
	}

	// Use a short deadline so a slow query never blocks the monitor tick.
	ctx, cancel := context.WithTimeout(pm.ctx, 500*time.Millisecond)
	defer cancel()
	connInfoStr, err := pm.readPrimaryConnInfo(ctx)
	if err != nil {
		// Conservative: don't trigger reconciliation we can't verify.
		return false
	}
	if connInfoStr == "" {
		return true
	}
	parsed, err := parseAndRedactPrimaryConnInfo(connInfoStr)
	if err != nil || parsed == nil {
		// Unparsable conninfo is itself drift worth fixing.
		return true
	}
	return parsed.GetHost() != targetHost || parsed.GetPort() != targetPort
}

// determineRemedialAction decides what action to take based on discovered state.
// This is pure decision logic with no side effects.
func (pm *MultiPoolerManager) determineRemedialAction(ctx context.Context, currentState postgresState) remedialAction {
	// Pgctld unavailable: No action possible
	if !currentState.pgctldAvailable {
		return remedialActionNone
	}

	// Postgres is running: Check if pooler type needs adjustment
	if currentState.postgresRunning {
		if currentState.isPrimary && pm.getPoolerType() != clustermetadatapb.PoolerType_PRIMARY && proto.Equal(pm.rules.CachedPosition().GetRule().GetLeaderId(), pm.record.Id()) {
			return remedialActionAdjustTypeToPrimary
		}
		if !currentState.isPrimary && pm.getPoolerType() == clustermetadatapb.PoolerType_PRIMARY {
			return remedialActionAdjustTypeToReplica
		}
		// Postgres is standby and type is already REPLICA, but check if the resignation
		// signal needs to be (re-)published. This handles the case where the signal was
		// lost after a process restart following emergency demotion.
		if !currentState.isPrimary {
			pm.mu.Lock()
			resigned := pm.resignedLeaderAtTerm
			pm.mu.Unlock()
			if currentState.primaryTerm != 0 && resigned == 0 {
				return remedialActionAdjustTypeToReplica
			}
			// Drift check: replica is otherwise healthy but its primary_conninfo
			// may not match what we've been told via SetPrimary/Promote. Reconcile
			// to the recorded ReplicationPrimary if there's a mismatch.
			if pm.primaryConnInfoDiffersFromRecorded(currentState) {
				return remedialActionFixPrimaryConnInfo
			}
			// TODO: self-rewind detection. A replica may need pg_rewind even
			// when it was never primary — phantom transactions can leak in
			// (e.g. via a brief sync-replication ack that got rolled back on
			// the primary). The old consensus flow let orch re-issue an
			// explicit rewind whenever it suspected one; the new flow's
			// SetPrimary is idempotent and won't repeat work, so the monitor
			// needs to self-detect stuck replication. Check
			// pg_stat_wal_receiver.status: if not "streaming" for
			// >stuckThreshold AND we have a recorded primary, treat as
			// suspected divergence — set rewindPending=true so the next
			// remedialActionFixPrimaryConnInfo iteration runs
			// pg_rewind dry-run before re-establishing replication. Cheap
			// when no divergence, conclusive when there is.
		}
		// Pooler type already matches; check for a stale GUC that needs re-applying.
		// Only reconcile the GUC when actually running as primary: synchronous_standby_names
		// has no effect on a standby, and setting it there leaks state.
		if currentState.isPrimary && pm.rules.HasInconsistentGUC(ctx) {
			return remedialActionReconcileGUC
		}
		return remedialActionNone
	}

	// A sentinel from a prior first-backup attempt means bootstrap crashed
	// mid-flight. Any on-disk pg_data is stale — force the create-first-backup
	// path so createFirstBackupAndInitializeLocked can clean up and retry.
	if currentState.bootstrapSentinelPresent {
		return remedialActionCreateFirstBackup
	}

	// Postgres not running: Try to start or restore
	if currentState.dirInitialized {
		return remedialActionStartPostgres
	}

	if currentState.backupsAvailable {
		return remedialActionRestoreFromBackup
	}

	// Directory not initialized and no backups: try to create the first backup
	return remedialActionCreateFirstBackup
}

// takeRemedialAction executes the specified remedial action.
// Caller must hold the action lock.
func (pm *MultiPoolerManager) takeRemedialAction(ctx context.Context, action remedialAction, state postgresState) {
	// Assert that the action lock is held
	if err := actionlock.AssertActionLockHeld(ctx); err != nil {
		pm.logger.ErrorContext(ctx, "takeRemedialAction called without action lock", "error", err)
		return
	}

	const (
		reasonPostgresRunning            = "postgres_running"
		reasonStartingPostgres           = "starting_postgres"
		reasonRestoringFromBackup        = "restoring_from_backup"
		reasonCreatingFirstBackup        = "creating_first_backup"
		reasonWaitingForFirstBackupLease = "waiting_for_first_backup_lease"
	)

	switch action {
	case remedialActionNone:
		// No action to take
		return

	case remedialActionAdjustTypeToPrimary:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		pm.logger.InfoContext(ctx, "MonitorPostgres: PostgreSQL is running and primary")
		pm.logger.InfoContext(ctx, "MonitorPostgres: Changing pooler type to primary")
		// A PRIMARY must carry a leadership observation. The monitor has no rule
		// handed to it here, so it observes the rule the node committed when it
		// promoted (its cached position).
		// TODO(consensus-authoritative): reconcile against the authoritative
		// consensus rule rather than the locally-committed one.
		obs := pm.leaderObs(pm.rules.CachedPosition().GetRule())
		if err := pm.servingState.SetState(ctx, clustermetadatapb.PoolerType_PRIMARY, obs, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to change pooler type to primary", "error", err)
		}

	case remedialActionAdjustTypeToReplica:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		pm.logger.InfoContext(ctx, "MonitorPostgres: PostgreSQL is running but not primary")
		pm.logger.InfoContext(ctx, "MonitorPostgres: Changing pooler type to replica")
		// Signal voluntary resignation so the coordinator can trigger an immediate
		// election. Use the primary_term (the term at which we were elected) since
		// the coordinator uses this to decide whether the signal is still active.
		if state.primaryTerm != 0 {
			if err := pm.setResignedLeaderAtTerm(ctx, state.primaryTerm); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set resigned primary term", "error", err)
				return
			}
		}
		// A REPLICA pooler record carries no self leadership observation.
		if err := pm.servingState.SetState(ctx, clustermetadatapb.PoolerType_REPLICA, nil, clustermetadatapb.PoolerServingStatus_SERVING); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to change pooler type to replica", "error", err)
		}

	case remedialActionFixPrimaryConnInfo:
		rp := pm.consensusState.GetReplicationPrimary()
		target := rp.GetPrimary()
		targetHost := target.GetHost()
		targetPort := target.GetPostgresPort()
		pm.logger.InfoContext(ctx, "MonitorPostgres: primary_conninfo drift detected; rewriting to recorded primary",
			"target_primary", target.GetId().GetName(),
			"target_host", targetHost,
			"target_port", targetPort)
		// TODO: when rewindPending=true (an unexpected demotion happened
		// without a follow-up pg_rewind), just setting primary_conninfo is
		// not enough — the WAL receiver will fail to start due to timeline
		// divergence. Route through demoteStalePrimaryLocked instead, which
		// runs pg_rewind dry-run (cheap when there's no divergence) before
		// re-establishing replication. This would let the monitor self-heal
		// a stuck-replica scenario without waiting for orch's
		// FixReplicationAction to issue a RewindToSource RPC.
		if err := pm.setPrimaryConnInfoLocked(ctx, targetHost, targetPort,
			true /* stopReplicationBefore */, true /* startReplicationAfter */); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to reconcile primary_conninfo", "error", err)
		}

	case remedialActionStartPostgres:
		// Honour the in-memory flag set by tests and demos to suppress auto-restart
		// during controlled failovers.
		if pm.postgresRestartsDisabled.Load() {
			pm.logger.InfoContext(ctx, "MonitorPostgres: skipping start, postgres restarts disabled")
			return
		}
		pm.setMonitorReason(ctx, reasonStartingPostgres, "MonitorPostgres: PostgreSQL initialized but not running, starting PostgreSQL")
		if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_STARTING); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
		}
		if err := pm.startPostgres(ctx); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to start PostgreSQL, will retry", "error", err)
		}

	case remedialActionRestoreFromBackup:
		pm.setMonitorReason(ctx, reasonRestoringFromBackup, "MonitorPostgres: directory not initialized but backups available, restoring from backup")
		if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
		}
		if err := pm.restoreAndStartPostgres(ctx); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to restore from backup, will retry", "error", err)
		}

	case remedialActionCreateFirstBackup:
		pm.setMonitorReason(ctx, reasonCreatingFirstBackup, "MonitorPostgres: no backup found, attempting to create one")
		if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_CREATING_FIRST_BACKUP); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
		}
		busy, backupFound, err := pm.createFirstBackupAndInitializeLocked(ctx)
		if busy {
			pm.setMonitorReason(ctx, reasonWaitingForFirstBackupLease, "MonitorPostgres: backup lease held by another pooler, waiting")
		} else if err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to create first backup, will retry", "error", err)
		} else if backupFound {
			// Another pooler created the backup just before we acquired the lease.
			// Restore immediately rather than waiting for the next monitor iteration.
			pm.setMonitorReason(ctx, reasonRestoringFromBackup, "MonitorPostgres: first backup found; restoring")
			if err := pm.actionLock.SetAction(ctx, multipoolermanagerdatapb.PostgresAction_POSTGRES_ACTION_RESTORING_FROM_BACKUP); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to set action", "error", err)
			}
			if err := pm.restoreAndStartPostgres(ctx); err != nil {
				pm.logger.ErrorContext(ctx, "MonitorPostgres: failed to restore from backup, will retry", "error", err)
			}
		}

	case remedialActionReconcileGUC:
		pm.setMonitorReason(ctx, reasonPostgresRunning, "MonitorPostgres: PostgreSQL is running")
		pm.logger.InfoContext(ctx, "MonitorPostgres: re-applying stale GUC")
		if err := pm.rules.ReconcileGUC(ctx, !state.isPrimary); err != nil {
			pm.logger.ErrorContext(ctx, "MonitorPostgres: GUC reconciliation failed", "error", err)
		}
	}
}

// hasCompleteBackups checks if there are any complete backups available
func (pm *MultiPoolerManager) hasCompleteBackups(ctx context.Context) bool {
	// Get list of backups
	backups, err := pm.backup.ListBackups(ctx)
	if err != nil {
		return false
	}

	// Filter to only complete backups
	for _, b := range backups {
		if b.Status == multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			return true
		}
	}

	return false
}

// startPostgres starts PostgreSQL via pgctld
//
// TODO: preemptive-rewind safety. A monitor-driven restart can't know what
// happened between the previous run and now — postgres may have crashed
// mid-write as primary, or may have been killed externally. The safe default
// is to come back as a standby with rewindPending=true so that the next
// SetPrimary/Promote/standby-conninfo path routes through demoteStalePrimaryLocked
// (which runs pg_rewind dry-run; cheap when there's no divergence) before
// trusting local WAL. This bears on the broader self-rewind plan:
//   - replicas with phantom transactions: orch sends an explicit
//     RewindToSource RPC today; a future change should let the local monitor
//     detect stuck replication and self-heal without needing orch in the loop.
//   - primaries demoted unexpectedly (crash, SIGKILL, external pg_demote):
//     the restart-as-standby helper should require callers to declare
//     "clean" vs "unexpected" so an unexpected transition can set
//     rewindPending up front, increasing the odds of fast convergence once
//     a new leader is announced.
func (pm *MultiPoolerManager) startPostgres(ctx context.Context) error {
	pm.logger.InfoContext(ctx, "MonitorPostgres: Attempting to restart PostgreSQL")
	if pm.pgctldClient == nil {
		return errors.New("pgctld client not available")
	}

	_, err := pm.pgctldClient.Start(ctx, &pgctldpb.StartRequest{})
	if err != nil {
		return fmt.Errorf("MonitorPostgres: failed to start PostgreSQL: %w", err)
	}

	pm.logger.InfoContext(ctx, "MonitorPostgres: PostgreSQL started successfully")

	// Reopen connections after postgres restart to replace stale socket FDs.
	// Only when connection pool is initialized (the manager may not have
	// connection infrastructure in unit tests with minimal setup).
	if pm.connPoolMgr != nil {
		pm.reopenConnections(ctx)

		// Wait for database connection to be ready
		if err := pm.waitForDatabaseConnection(ctx); err != nil {
			return fmt.Errorf("MonitorPostgres: database not ready after restart: %w", err)
		}
	}

	return nil
}

// restoreAndStartPostgres restores from backup and starts PostgreSQL.
// This is used by MonitorPostgres for auto-restore functionality.
// Caller must hold the action lock.
func (pm *MultiPoolerManager) restoreAndStartPostgres(ctx context.Context) error {
	// Re-check status to ensure conditions haven't changed
	// (e.g., another process may have initialized or started postgres while we waited for lock)
	if pm.pgctldClient != nil {
		statusResp, err := pm.pgctldClient.Status(ctx, &pgctldpb.StatusRequest{})
		if err == nil {
			// If directory is now initialized, skip restore
			if statusResp.Status != pgctldpb.ServerStatus_NOT_INITIALIZED {
				pm.logger.InfoContext(ctx, "MonitorPostgres: directory became initialized after acquiring lock, skipping restore")
				return nil
			}
		}
		// If status check fails, continue with restore attempt
	}

	// Get the latest complete backup
	backups, err := pm.backup.ListBackups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	// Filter to only complete backups
	var completeBackups []*multipoolermanagerdatapb.BackupMetadata
	for _, b := range backups {
		if b.Status == multipoolermanagerdatapb.BackupMetadata_COMPLETE {
			completeBackups = append(completeBackups, b)
		}
	}

	if len(completeBackups) == 0 {
		return errors.New("no complete backups available")
	}

	// Use the latest complete backup (last in the list)
	latestBackup := completeBackups[len(completeBackups)-1]

	pm.logger.InfoContext(ctx, "MonitorPostgres: restoring from backup",
		"backup_id", latestBackup.BackupId)

	// Perform the restore
	if err := telemetry.WithSpan(ctx, "monitor-postgres-restore", func(ctx context.Context) error {
		return pm.restoreFromBackupLocked(ctx, latestBackup.BackupId)
	}); err != nil {
		return fmt.Errorf("failed to restore from backup: %w", err)
	}

	revokedBelowTerm, _ := pm.consensusState.GetInconsistentCurrentTermNumber()
	pm.logger.InfoContext(ctx, "MonitorPostgres: successfully restored from backup",
		"backup_id", latestBackup.BackupId,
		"shard", pm.getShardID(),
		"term", revokedBelowTerm)

	return nil
}
