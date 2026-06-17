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

// Package poolerserver implements the multipooler gRPC server
package poolerserver

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/queryservice"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multipooler/internal/connpoolmanager"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/pubsub"
	"github.com/multigres/multigres/go/services/multipooler/internal/replication"
	"github.com/multigres/multigres/go/services/multipooler/internal/servingstate"
)

// QueryPoolerServer is the core pooler implementation for query serving.
// It encapsulates the components required to manage query execution
// (e.g. pooling, execution, transactions).
//
// In the future, components like the transaction engine and query engine
// should be added here. The current executor is temporary until those
// engines are introduced.
//
// The lifecycle of the pooler is managed by the MultiPoolerManager.
// The connection pool manager (connpoolmanager) handles all database connections,
// including per-user connection pools with trust/peer authentication.
type QueryPoolerServer struct {
	logger      *slog.Logger
	poolManager connpoolmanager.PoolManager
	executor    *executor.Executor

	// tableGroup and shard identify which tablegroup/shard this pooler serves.
	// Set at construction time and immutable.
	tableGroup string
	shard      string

	mu sync.Mutex
	// routingRole is the write-safety role from OnStateChange: PRIMARY iff this
	// pooler is the writable leader (out of recovery AND the active — committed,
	// non-revoked, highest-known — consensus leader). Both leader-bound query
	// modes (WRITABLE and CONSISTENT) gate on it: writes and read-your-writes reads
	// route to the writable leader, closing the pg_promote()→WAL-commit window.
	routingRole    servingstate.RoutingRole
	servingStatus  clustermetadatapb.PoolerServingStatus
	healthProvider HealthProvider

	// pubsubListener is the shared LISTEN/NOTIFY listener, set by MultiPoolerManager.
	pubsubListener *pubsub.Listener

	// drainPhase tracks the graceful-drain stage during a not-serving transition.
	// See drainPhase constants and StartRequest for the admission rules per stage.
	drainPhase drainPhase

	// gracePeriod is how long OnStateChange waits for in-flight connections to drain
	// before force-closing reserved connections. Configured via --connpool-drain-grace-period.
	gracePeriod time.Duration

	// stateChanged is closed and re-created on every state transition.
	// AwaitStateChange blocks on this channel to be notified when the
	// pooler's type and serving status are updated.
	stateChanged chan struct{}

	// drainStats holds drain-observability metrics. Never nil.
	drainStats *drainStats

	// replMetrics holds replication-tunnel instruments, shared across all
	// StreamReplication RPCs. Never nil (instruments fall back to noop).
	replMetrics *replication.Metrics
}

// drainPhase is the stage of a graceful not-serving transition. The drain runs
// in two stages so single autocommit queries keep flowing while transactions
// finish, and the pooler reports not-serving only once nothing is in flight.
type drainPhase int

const (
	// drainNone: not draining. Normal serving (or already not-serving).
	drainNone drainPhase = iota
	// drainReserved: stage 1 — waiting for reserved connections (transactions)
	// to finish. New reservations are rejected; single queries are still served;
	// existing reserved connections continue.
	drainReserved
	// drainRegular: stage 2 — transactions are drained; now also reject single
	// queries and wait for the remaining in-flight ones to finish, so the cutover
	// happens with zero in-flight work.
	drainRegular
)

// RequestKind classifies an incoming request for admission control during a
// graceful drain. It is computed by the gRPC handlers from the request's
// reservation fields and passed to StartRequest.
type RequestKind int

const (
	// RequestExistingReserved is an op on an EXISTING reserved connection
	// (continuing a transaction, COMMIT/ROLLBACK, temp-table cleanup, etc.).
	// Always admitted; the reserved connection's existence is the real gate.
	RequestExistingReserved RequestKind = iota
	// RequestSingleQuery is a one-off autocommit query that holds no reserved
	// connection. Served during stage 1 of the drain; rejected from stage 2 on.
	RequestSingleQuery
	// RequestNewReservation starts a NEW reserved connection (BEGIN/first
	// transaction statement, new temp table, portal, COPY). Rejected for the
	// whole drain so no new transactions accumulate.
	RequestNewReservation
)

// NewQueryPoolerServer creates a new QueryPoolerServer instance with the given pool manager
// and health provider.
// The pool manager must already be opened before calling this function.
// The health provider is used by StreamPoolerHealth to provide health updates to clients.
// gracePeriod controls how long OnStateChange waits for in-flight connections to drain
// during not-serving transitions before force-closing reserved connections.
func NewQueryPoolerServer(logger *slog.Logger, poolManager connpoolmanager.PoolManager, poolerID *clustermetadatapb.ID, tableGroup, shard string, healthProvider HealthProvider, gracePeriod time.Duration, backendVpidTrackingEnabled bool) *QueryPoolerServer {
	var exec *executor.Executor
	if poolManager != nil {
		exec = executor.NewExecutor(logger, poolManager, poolerID, backendVpidTrackingEnabled)
	}

	replMetrics, replMetricsErr := replication.NewMetrics()
	if replMetricsErr != nil && logger != nil {
		// Non-fatal: instruments that failed fall back to noop.
		logger.Warn("failed to initialise some replication metrics", "error", replMetricsErr)
	}

	return &QueryPoolerServer{
		logger:         logger,
		poolManager:    poolManager,
		executor:       exec,
		tableGroup:     tableGroup,
		shard:          shard,
		servingStatus:  clustermetadatapb.PoolerServingStatus_DISABLED,
		healthProvider: healthProvider,
		gracePeriod:    gracePeriod,
		stateChanged:   make(chan struct{}),
		drainStats:     newDrainStats(),
		replMetrics:    replMetrics,
	}
}

// ReplicationMetrics returns the shared replication-tunnel instruments. Used by
// the gRPC StreamReplication handler to derive a per-stream recorder.
func (s *QueryPoolerServer) ReplicationMetrics() *replication.Metrics {
	return s.replMetrics
}

// OnStateChange transitions the query service to match the new serving state.
// Implements PoolerController interface.
//
// This method is only called by the StateManager, which serializes calls behind
// a mutex. Concurrent calls are not possible or expected.
//
// For not-serving transitions, this performs a two-stage graceful drain under a
// single gracePeriod deadline:
//  1. drainReserved — reject new transactions/reservations, keep serving single
//     autocommit queries, and wait for in-flight transactions (reserved
//     connections) to finish.
//  2. drainRegular — also reject single queries, and wait for the remaining
//     in-flight ones to finish, so the cutover happens with zero in-flight work
//     and the subsequent postgres demotion kills nothing.
//  3. Set servingStatus to the not-serving target.
//
// If the shared deadline expires in either stage, all reserved connections are
// force-closed (their transactions surface 40001) and the transition completes;
// any still-running single queries are killed by the postgres demotion that
// follows. On timeout, errors are acceptable — that is what the grace period
// bounds.
func (s *QueryPoolerServer) OnStateChange(ctx context.Context, state servingstate.State) error {
	routingRole := state.RoutingRole
	servingStatus := state.ServingStatus
	s.mu.Lock()

	s.logger.InfoContext(ctx, "Transitioning serving type",
		"routing_from", s.routingRole, "routing_to", routingRole,
		"status_from", s.servingStatus, "status_to", servingStatus)

	if servingStatus == clustermetadatapb.PoolerServingStatus_SERVING {
		s.routingRole = routingRole
		s.servingStatus = servingStatus
		s.drainPhase = drainNone
		s.notifyStateChangedLocked()
		s.mu.Unlock()
		return nil
	}

	// not-serving: begin stage 1 of the graceful drain.
	// The routing role is NOT updated yet — in-flight requests on reserved
	// connections (e.g., COMMIT after a demotion) must still see the old role
	// so that checkTargetLocked allows them to complete.
	s.drainPhase = drainReserved
	s.mu.Unlock()

	// Both stages share one deadline bounded by gracePeriod. If gracePeriod == 0
	// the wait is unbounded (each stage still terminates because it rejects the
	// new work that would otherwise keep its pool non-empty).
	if s.poolManager != nil {
		drainStart := time.Now()
		drainCtx := ctx
		if s.gracePeriod > 0 {
			var cancel context.CancelFunc
			drainCtx, cancel = context.WithTimeout(ctx, s.gracePeriod)
			defer cancel()
		}

		outcome := drainOutcomeGraceful
		// Stage 1: wait for transactions to finish while single queries flow.
		if err := s.poolManager.WaitForReservedDrain(drainCtx); err != nil {
			outcome = drainOutcomeForceClose
		} else {
			// Stage 2: stop serving single queries, then wait for the in-flight
			// ones to complete.
			s.setDrainPhase(drainRegular)
			if err := s.poolManager.WaitForDrain(drainCtx); err != nil {
				outcome = drainOutcomeForceClose
			}
		}

		if outcome == drainOutcomeForceClose {
			s.logger.WarnContext(ctx, "Graceful drain did not complete within grace period, force-closing reserved connections",
				"grace_period", s.gracePeriod)
			// Force-close all reserved connections so no transaction survives into
			// a non-serving state. In-flight single queries (if any) are killed by
			// the postgres demotion that follows the not-serving transition.
			killed := s.poolManager.CloseReservedConnections(ctx)
			s.drainStats.recordForceClosed(ctx, killed)
			if killed > 0 {
				s.logger.WarnContext(ctx, "Force-closed reserved connections after drain timeout",
					"killed", killed)
			}
		}
		s.drainStats.recordDrain(ctx, time.Since(drainStart).Seconds(), outcome)
	}

	// Complete the transition. The routing role is set here (after drain) so that
	// in-flight requests saw the old role throughout the drain phase.
	s.mu.Lock()
	s.routingRole = routingRole
	s.servingStatus = servingStatus
	s.drainPhase = drainNone
	s.notifyStateChangedLocked()
	s.mu.Unlock()

	return nil
}

// setDrainPhase updates the drain phase under the mutex. Used to advance from
// stage 1 (drainReserved) to stage 2 (drainRegular) mid-drain.
func (s *QueryPoolerServer) setDrainPhase(p drainPhase) {
	s.mu.Lock()
	s.drainPhase = p
	s.mu.Unlock()
}

// StartRequest checks whether a request should be admitted, based on its
// RequestKind and the current drain phase. Returns nil if allowed, or MTF01
// (which the gateway buffers + retries on the new primary) if rejected.
//
// Admission matrix:
//
//	kind \ state      | SERVING | drainReserved | drainRegular | not-serving
//	------------------+---------+---------------+--------------+------------
//	ExistingReserved  | allow   | allow         | allow        | allow¹
//	SingleQuery       | allow   | allow         | reject MTF01 | reject MTF01
//	NewReservation    | allow   | reject MTF01  | reject MTF01 | reject MTF01
//
// ¹ ExistingReserved is always admitted regardless of serving status or
// demotion — the reserved connection's existence is the real gate. If the
// connection was force-closed during the drain, the executor returns an honest
// 40001 (transaction aborted) rather than the misleading MTF01. Tablegroup/shard
// mismatches (MTD01) are still rejected for every kind.
//
// The two-stage drain lets single autocommit queries keep flowing while
// transactions finish (drainReserved), then stops them too so the cutover
// happens with zero in-flight work (drainRegular).
func (s *QueryPoolerServer) StartRequest(target *query.Target, kind RequestKind) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existingReserved := kind == RequestExistingReserved
	if err := s.checkTargetLocked(target, existingReserved); err != nil {
		return err
	}

	// In-flight operations on an existing reserved connection are admitted
	// regardless of serving status, demotion, or drain phase. The reserved
	// connection itself is the real gate: if it still exists the operation
	// completes on the live backend (postgres is demoted only after the drain
	// finishes); if it was force-closed because the drain exceeded its grace
	// period, the executor returns an honest 40001 so the client retries the
	// whole transaction — rather than the misleading MTF01 signal, which is
	// neither buffered nor retryable for a transaction that no longer exists.
	if existingReserved {
		return nil
	}

	switch s.drainPhase {
	case drainNone:
		// Not draining: admit while serving, reject once not-serving.
		if s.servingStatus != clustermetadatapb.PoolerServingStatus_SERVING {
			return mterrors.MTF01.New()
		}
		return nil
	case drainReserved:
		// Stage 1: keep serving single queries; reject new reservations.
		if kind == RequestSingleQuery {
			return nil
		}
		return mterrors.MTF01.New()
	default: // drainRegular
		// Stage 2: reject everything new (single queries and reservations).
		return mterrors.MTF01.New()
	}
}

// checkTargetLocked validates that the request's target matches this pooler's identity.
//
// Validation rules:
//   - TableGroup and Shard must always match. A mismatch is a routing bug (MTD01).
//   - Leadership: if the target is PRIMARY but this pooler is not the consensus
//     leader, the gateway has stale topology (likely a demotion happened). Returns
//     MTF01 to trigger buffering until the new primary is discovered. All other
//     combinations are allowed (a leader serves both PRIMARY and REPLICA traffic).
//
// existingReserved signals an in-flight operation on an existing reserved
// connection. Such operations bypass the demotion (leader→non-leader) check: the
// reserved connection's existence is the real gate (see StartRequest), so a
// demoted pooler whose reserved connection survived the drain can still conclude
// its transaction, and one whose connection was force-closed surfaces an honest
// 40001 from the executor rather than MTF01. Tablegroup/shard mismatches (real
// routing bugs) are still rejected.
//
// Caller must hold s.mu.
func (s *QueryPoolerServer) checkTargetLocked(target *query.Target, existingReserved bool) error {
	if target == nil {
		return nil
	}

	sk := target.GetShardKey()
	// TODO: add s.database and validate sk.GetDatabase() once
	// QueryPoolerServer is wired with the pooler's bound database.
	if s.tableGroup != "" && sk.GetTableGroup() != "" && sk.GetTableGroup() != s.tableGroup {
		return mterrors.MTD01.New("target tablegroup %q does not match pooler tablegroup %q", sk.GetTableGroup(), s.tableGroup)
	}

	if s.shard != "" && sk.GetShard() != "" && sk.GetShard() != s.shard {
		return mterrors.MTD01.New("target shard %q does not match pooler shard %q", sk.GetShard(), s.shard)
	}

	if existingReserved {
		return nil
	}

	// A leader-bound request (WRITABLE / CONSISTENT) hitting a non-writable pooler
	// means the gateway thinks this pooler is still the primary, but it is not the
	// writable leader (demoted, or not yet done promoting). Return MTF01 to trigger
	// buffering. Both modes gate on the routing role: writes must not land on an
	// older consensus timeline, and read-your-writes reads must not be served by a
	// not-yet-writable (still promoting) or superseded/deposed leader — they buffer
	// until a writable leader exists rather than returning a stale read.
	switch target.GetMode() {
	case query.Mode_MODE_WRITABLE, query.Mode_MODE_CONSISTENT:
		if s.routingRole != servingstate.RoutingRolePrimary {
			return mterrors.MTF01.New()
		}
	}

	return nil
}

// notifyStateChangedLocked closes the stateChanged channel to wake any
// AwaitStateChange callers, then replaces it with a fresh channel.
// Caller must hold s.mu.
func (s *QueryPoolerServer) notifyStateChangedLocked() {
	close(s.stateChanged)
	s.stateChanged = make(chan struct{})
}

// AwaitStateChange blocks until the pooler's routing role and serving status
// match the given targets, or ctx is cancelled. Used by the health streamer to
// ensure the query server is ready before broadcasting the new state.
func (s *QueryPoolerServer) AwaitStateChange(ctx context.Context, routingRole servingstate.RoutingRole, servingStatus clustermetadatapb.PoolerServingStatus) {
	for {
		s.mu.Lock()
		if s.routingRole == routingRole && s.servingStatus == servingStatus {
			s.mu.Unlock()
			return
		}
		ch := s.stateChanged
		s.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return
		}
	}
}

// IsServing returns true if currently serving queries.
// Implements PoolerController interface.
func (s *QueryPoolerServer) IsServing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.servingStatus == clustermetadatapb.PoolerServingStatus_SERVING
}

// IsHealthy checks if the controller is healthy.
// Implements PoolerController interface.
func (s *QueryPoolerServer) IsHealthy() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.executor == nil {
		return errors.New("executor not initialized")
	}

	if s.poolManager == nil {
		return errors.New("pool manager not initialized")
	}

	// The pool manager handles connection health internally.
	// If we have the executor and pool manager, we're healthy.
	return nil
}

// RegisterGRPCServices registers gRPC services (called by manager during startup).
// Implements PoolerController interface.
func (s *QueryPoolerServer) RegisterGRPCServices() {
	s.registerGRPCServices()
}

// Executor returns the executor instance for use by gRPC service handlers.
// Implements PoolerController interface.
// Returns error if the pooler is not initialized.
//
// Note: Callers must call StartRequest() before Executor() to check serving state.
// StartRequest returns MTF01 when not serving or shutting down, which the gateway
// uses to trigger failover buffering.
func (s *QueryPoolerServer) Executor() (queryservice.QueryService, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.executor == nil {
		return nil, errors.New("executor not initialized - pool manager was nil")
	}

	return s.executor, nil
}

// SetPubSubListener sets the PubSub listener on the pooler server.
// The listener is created and managed by the MultiPoolerManager.
func (s *QueryPoolerServer) SetPubSubListener(l *pubsub.Listener) {
	s.pubsubListener = l
}

// PubSubListener returns the shared PubSub listener (may be nil).
func (s *QueryPoolerServer) PubSubListener() *pubsub.Listener {
	return s.pubsubListener
}

// PoolManager returns the pool manager instance.
// This is used by GetAuthCredentials to query pg_authid using an admin connection,
// which works even before the executor is fully initialized during bootstrap.
func (s *QueryPoolerServer) PoolManager() connpoolmanager.PoolManager {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.poolManager
}

// InternalQueryService returns the executor as an InternalQueryService for internal queries.
// Implements PoolerController interface.
func (s *QueryPoolerServer) InternalQueryService() executor.InternalQueryService {
	// Explicit nil check required: returning a nil *Executor directly would produce a
	// non-nil interface value wrapping a nil pointer, causing callers' == nil checks to
	// pass but method calls on the interface to panic.
	if s.executor == nil {
		return nil
	}
	return s.executor
}

// HealthProvider returns the health provider for streaming health updates.
func (s *QueryPoolerServer) HealthProvider() HealthProvider {
	return s.healthProvider
}
