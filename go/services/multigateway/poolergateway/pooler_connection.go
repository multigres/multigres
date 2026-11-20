// Copyright 2026 Supabase, Inc.
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

package poolergateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	migratorpb "github.com/multigres/multigres/go/pb/migrator"
	multipoolermanagerpb "github.com/multigres/multigres/go/pb/multipoolermanager"
	"github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/retry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/proto"
)

// errPoolerUninitialized is the initial error before health stream connects.
var errPoolerUninitialized = errors.New("pooler health not initialized")

// The backoff between attempts to open a health stream. They are variables so
// tests can lengthen them, to tell a retry that was asked for from one the
// backoff allowed.
var (
	healthRetryDelay    = constants.DefaultHealthRetryDelay
	healthRetryMaxDelay = constants.DefaultHealthCheckTimeout
)

// poolerHealth represents the health state received from a multipooler.
// This is a snapshot of health state that can be safely passed around
// without synchronization.
type poolerHealth struct {
	// PoolerID identifies the multipooler instance.
	PoolerID *clustermetadatapb.ID

	// ServingStatus is the serving state reported by the pooler. Buffer drain
	// (notifyLeaderServingFromSummary) requires both SERVING and the broadcast's
	// RoutingState advertising role PRIMARY — see that function for the race the
	// dual check guards against.
	ServingStatus clustermetadatapb.PoolerServingStatus

	// RoutingState is the pooler's self-reported routing/HA role plus the rule
	// that qualifies it. role == PRIMARY marks the writable routing primary; the
	// rule number is used as the failover-overlap tiebreaker among primaries.
	RoutingState *clustermetadatapb.RoutingState

	// ReplicationLagNs is the replication lag in nanoseconds reported by the pooler.
	// Zero on the primary or when not yet measured.
	ReplicationLagNs int64

	// RoutingVersion and PlanEpoch are the sharding catalog's versions, which
	// only the catalog shard's primary reports. Zero from any other pooler.
	RoutingVersion int64
	PlanEpoch      int64

	// LastError is the most recent error from the health stream.
	LastError error

	// LastResponse is when we last received a health update.
	LastResponse time.Time
}

// isServing returns true if the pooler is serving traffic.
func (h *poolerHealth) isServing() bool {
	if h == nil {
		return false
	}
	return h.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING
}

// simpleCopy returns a shallow copy of the poolerHealth.
// This is not a deep copy: pointer fields (Target, PoolerID, LeaderObservation)
// reference the same underlying objects. This is safe because these proto objects
// are treated as immutable - they are never modified after creation.
// Returns a shallow copy that is safe to read concurrently.
func (h *poolerHealth) simpleCopy() *poolerHealth {
	if h == nil {
		return nil
	}
	return &poolerHealth{
		PoolerID:         h.PoolerID,
		ServingStatus:    h.ServingStatus,
		RoutingState:     h.RoutingState,
		ReplicationLagNs: h.ReplicationLagNs,
		RoutingVersion:   h.RoutingVersion,
		PlanEpoch:        h.PlanEpoch,
		LastError:        h.LastError,
		LastResponse:     h.LastResponse,
	}
}

// poolerConnection manages a single gRPC connection to a multipooler instance.
// It wraps a QueryService and provides access to pooler metadata.
//
// A poolerConnection exists if and only if we are actively connected to the pooler.
// The pooler cache's OnLive hook constructs poolerConnection instances; its OnGone
// hook calls Shutdown.
//
// The connection maintains a health stream to the multipooler and tracks serving state.
// Only connections that are serving should be used for query routing.
type poolerConnection struct {
	// poolerInfo contains the pooler metadata from discovery.
	// Accessed atomically to avoid data races between UpdatePoolerInfo and readers.
	poolerInfo atomic.Pointer[topoclient.MultipoolerInfo]

	// conn is the underlying gRPC connection; nil for a LocalPooler, which is
	// called directly. Production code reaches the connection only via pooler
	// (which owns its lifecycle: Shutdown calls pooler.Close()). The field is
	// retained so telemetry tests can inspect the dial's OpenTelemetry
	// attributes directly.
	conn *grpc.ClientConn

	// pooler is the pooler's query service, credential lookup, health and
	// notifications: over gRPC, or direct calls to a pooler in this process.
	pooler queryservice.Pooler

	// logger for debugging
	logger *slog.Logger

	// ctx and cancel control the health stream goroutine lifecycle.
	// cancel must be called before discarding poolerConnection to ensure
	// the checkConn goroutine terminates.
	ctx    context.Context
	cancel context.CancelFunc

	// checkConnDone is closed by checkConn when its loop exits. Useful for
	// tests that need to assert the loop has stopped after context cancel,
	// rather than guessing with a sleep.
	checkConnDone chan struct{}

	// healthMu protects the health field
	healthMu sync.Mutex

	// health contains the current health state from the health stream.
	// Updated atomically as a unit when new health responses arrive.
	health *poolerHealth

	// healthTimedOut indicates if the health stream has timed out.
	// Accessed atomically because there's a race between the timeout
	// goroutine and the stream processing.
	healthTimedOut atomic.Bool

	// onHealthUpdate is called when health state changes.
	// Used by loadBalancer to update its routing decisions.
	onHealthUpdate func(*poolerConnection)

	// waitMu protects cancelWait.
	waitMu sync.Mutex

	// cancelWait ends the backoff wait of checkConn's current retry episode,
	// which makes the loop start a new episode with the backoff reset and no
	// wait before its first attempt. It is nil before checkConn starts.
	cancelWait context.CancelFunc
}

// newPoolerConnection creates a new connection to a multipooler instance and
// starts health streaming automatically.
//
// The onHealthUpdate callback is invoked when health state changes. It may be
// nil if health updates don't need to be observed.
//
// Close must be called when the connection is no longer needed to stop the
// health stream goroutine and release resources.
func newPoolerConnection(
	ctx context.Context,
	pooler *clustermetadatapb.Multipooler,
	logger *slog.Logger,
	grpcDialOpt grpc.DialOption,
	local *LocalPooler,
	onHealthUpdate func(*poolerConnection),
) (*poolerConnection, error) {
	poolerInfo := &topoclient.MultipoolerInfo{Multipooler: pooler}
	poolerID := topoclient.ComponentIDString(pooler.Id)
	addr := poolerInfo.Addr()

	logger.DebugContext(ctx, "creating pooler connection",
		"pooler_id", poolerID,
		"addr", addr,
		"local", local != nil,
		"is_leader", pooler.GetRoutingState() != nil)

	// Call the pooler directly when it is in this process, over gRPC
	// otherwise.
	var conn *grpc.ClientConn
	var service queryservice.Pooler
	if local != nil {
		service = newDirectQueryService(local.Service)
	} else {
		var err error
		conn, err = grpccommon.NewClient(addr,
			grpccommon.WithAttributes(rpcclient.PoolerSpanAttributes(pooler.Id)...),
			grpccommon.WithDialOptions(grpcDialOpt),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC client for pooler %s at %s: %w", poolerID, addr, err)
		}
		service = newGRPCQueryService(conn, poolerID, logger)
	}

	// Derive a cancellable context from the service-lifetime context for the
	// health stream goroutine. This ensures proper shutdown propagation.
	ctx, cancel := context.WithCancel(ctx)

	// Initialize health state to DISABLED until the health stream provides data.
	// Shard identity is available via poolerInfo; the gateway derives role from
	// consensus observations, not the topology Type label.
	pc := &poolerConnection{
		conn:           conn,
		pooler:         service,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		checkConnDone:  make(chan struct{}),
		onHealthUpdate: onHealthUpdate,
		health: &poolerHealth{
			PoolerID:      pooler.Id,
			ServingStatus: clustermetadatapb.PoolerServingStatus_DISABLED,
			LastError:     errPoolerUninitialized,
		},
	}
	pc.poolerInfo.Store(poolerInfo)

	// Start health stream goroutine
	go pc.checkConn()

	logger.DebugContext(ctx, "pooler connection established",
		"pooler_id", poolerID,
		"addr", addr)

	return pc, nil
}

// ID returns the unique identifier for this pooler connection.
func (pc *poolerConnection) ID() topoclient.ComponentID {
	return topoclient.ComponentIDString(pc.poolerInfo.Load().Id)
}

// Cell returns the cell where this pooler is located.
func (pc *poolerConnection) Cell() string {
	return pc.poolerInfo.Load().Id.GetCell()
}

// UpdatePoolerInfo refreshes the pooler metadata (hostname, ports, shard) from
// a topology update. Leader identity is tracked separately via consensus
// observations (the load balancer's merged leader map), so a topology Type
// change here is not acted upon.
func (pc *poolerConnection) UpdatePoolerInfo(pooler *clustermetadatapb.Multipooler) {
	pc.poolerInfo.Store(&topoclient.MultipoolerInfo{Multipooler: pooler})
}

// applyTopologyUpdate takes in the connection's topology record after an update
// of it, and retries the health stream at once if the update is a restart of the
// pooler. A pooler that crashed never wrote SHUTDOWN, so its record stays and its
// connection with it, with the health stream backing off from the crash: the
// restart rewrites the record, and is the cue to retry.
func (pc *poolerConnection) applyTopologyUpdate(prev, curr *clustermetadatapb.Multipooler) {
	pc.UpdatePoolerInfo(curr)
	if poolerRestarted(prev, curr) {
		pc.RetryHealthNow()
	}
}

// poolerRestarted reports whether a topology update is the record of a pooler
// process that has just started or has just finished starting. Both write the
// record's lifecycle entry: a process writes STARTING when it registers and
// ACTIVE once it serves, each with the time of the write, and the record's other
// fields are overwritten by the new process as well. A pooler that crashed or was
// killed leaves its old record in place, so this is the only sign that it came
// back. A routine republish (serving status, routing state) leaves the lifecycle
// entry as it was, and so does every other update the pooler makes while it runs.
func poolerRestarted(prev, curr *clustermetadatapb.Multipooler) bool {
	lifecycle := curr.GetLifecycleStatus()
	switch lifecycle.GetStatus() {
	case clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_STARTING,
		clustermetadatapb.PoolerLifecycleStatus_LIFECYCLE_ACTIVE:
	default:
		return false
	}
	return !proto.Equal(prev.GetLifecycleStatus().GetUpdated(), lifecycle.GetUpdated())
}

// reconnectWait bounds how long RetryHealthNow waits for the gRPC channel to
// reconnect before it lets the health stream try anyway.
const reconnectWait = 2 * time.Second

// RetryHealthNow makes the health stream retry now, when it is not healthy:
// it is failing, or waiting out a backoff. It ends the wait and resets the
// backoff, so the loop's next attempt is immediate. It does nothing while the
// stream is healthy, and it never interrupts an attempt in progress. One call
// buys at most one immediate attempt: if that attempt fails the loop backs off
// from the start as usual.
//
// The gRPC channel has a backoff of its own, and after a crash it is in
// TRANSIENT_FAILURE, where a call fails at once without trying to connect. So
// the channel is told to reconnect now, and the retry waits, for at most
// reconnectWait, until it has.
func (pc *poolerConnection) RetryHealthNow() {
	if pc.Health().LastError == nil {
		return
	}
	pc.logger.Debug("retrying the health stream now", "pooler_id", pc.ID())
	if pc.conn == nil {
		pc.endBackoffWait()
		return
	}
	pc.conn.ResetConnectBackoff()
	pc.conn.Connect()
	go func() {
		pc.awaitReconnect()
		pc.endBackoffWait()
	}()
}

// awaitReconnect waits until the gRPC channel is ready, is shut down, or
// reconnectWait has passed, or the connection is closed.
func (pc *poolerConnection) awaitReconnect() {
	ctx, cancel := context.WithTimeout(pc.ctx, reconnectWait)
	defer cancel()
	for {
		state := pc.conn.GetState()
		if state == connectivity.Ready || state == connectivity.Shutdown {
			return
		}
		if !pc.conn.WaitForStateChange(ctx, state) {
			return
		}
	}
}

// endBackoffWait ends the backoff wait of the current retry episode.
func (pc *poolerConnection) endBackoffWait() {
	pc.waitMu.Lock()
	cancel := pc.cancelWait
	pc.waitMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// PoolerInfo returns the underlying pooler metadata.
func (pc *poolerConnection) PoolerInfo() *topoclient.MultipoolerInfo {
	return pc.poolerInfo.Load()
}

// Pooler returns the pooler's query service, credential lookup, health and
// notifications for this connection.
func (pc *poolerConnection) Pooler() queryservice.Pooler {
	return pc.pooler
}

// ManagerClient returns a pooler manager client over this connection's gRPC
// channel, which serves the manager service alongside the query service. The
// local pooler is called in process, with no channel, and has none.
func (pc *poolerConnection) ManagerClient() (multipoolermanagerpb.MultipoolerManagerClient, error) {
	if pc.conn == nil {
		return nil, fmt.Errorf("pooler %s has no gRPC connection for its manager service", pc.ID())
	}
	return multipoolermanagerpb.NewMultipoolerManagerClient(pc.conn), nil
}

// QueryService returns the query execution service for this connection.
func (pc *poolerConnection) QueryService() queryservice.QueryService {
	return pc.pooler
}

// MigratorClient returns a Migrator service client on the same gRPC
// connection. The Migrator service is registered on the multipooler's gRPC
// server, so this reaches the migration coordinator on the shard primary. Used
// by the gateway's migration DDL interface.
func (pc *poolerConnection) MigratorClient() migratorpb.MigratorClient {
	return migratorpb.NewMigratorClient(pc.conn)
}

// Shutdown stops the health stream goroutine and closes the underlying
// gRPC connection. One-shot: a poolerConnection cannot be reopened. Called
// from the pooler cache's OnGone hook when the pooler leaves the topology.
func (pc *poolerConnection) Shutdown() error {
	poolerID := pc.ID()
	pc.logger.Debug("shutting down pooler connection", "pooler_id", poolerID)

	// Cancel the health stream context to stop the checkConn goroutine
	if pc.cancel != nil {
		pc.cancel()
	}

	if err := pc.pooler.Close(); err != nil {
		return fmt.Errorf("failed to close query service for pooler %s: %w", poolerID, err)
	}
	return nil
}

// Health returns the current health state.
// The returned poolerHealth is a snapshot that can be safely used without
// synchronization. We don't deep-copy because the poolerHealth object is
// never modified after creation.
func (pc *poolerConnection) Health() *poolerHealth {
	pc.healthMu.Lock()
	defer pc.healthMu.Unlock()
	return pc.health
}

// checkConn performs health checking on the pooler connection.
// It continuously attempts to maintain a health stream, retrying with
// exponential backoff on failures. RetryHealthNow ends the backoff: the loop
// then starts a new episode, with a fresh backoff and an immediate first
// attempt.
func (pc *poolerConnection) checkConn() {
	defer close(pc.checkConnDone)
	poolerID := pc.ID()
	pc.logger.Debug("starting health check loop", "pooler_id", poolerID)

	for pc.ctx.Err() == nil {
		pc.healthEpisode()
	}
	pc.logger.Debug("health check loop exiting", "pooler_id", poolerID, "reason", pc.ctx.Err())
}

// healthEpisode runs the retry loop until the connection is closed or
// RetryHealthNow ends its backoff wait, whichever comes first. A wait that
// RetryHealthNow cancels while an attempt is running takes effect when that
// attempt ends, so the attempt itself is never interrupted.
func (pc *poolerConnection) healthEpisode() {
	poolerID := pc.ID()
	streamRetrier := retry.New(healthRetryDelay, healthRetryMaxDelay)

	// waitCtx is what the backoff waits on. The stream attempts run on pc.ctx.
	waitCtx, cancelWait := context.WithCancel(pc.ctx)
	defer cancelWait()
	pc.waitMu.Lock()
	pc.cancelWait = cancelWait
	pc.waitMu.Unlock()

	for attempt, waitErr := range streamRetrier.Attempts(waitCtx) {
		if waitErr != nil {
			// The connection is closing, or the backoff was cut short.
			pc.logger.Debug("health check backoff ended",
				"pooler_id", poolerID,
				"attempt", attempt,
				"reason", waitErr,
				"closing", pc.ctx.Err() != nil)
			return
		}

		if attempt > 1 {
			pc.logger.Debug("retrying health stream",
				"pooler_id", poolerID,
				"attempt", attempt)
		}

		// Create a separate context for this stream attempt.
		// This allows the staleness timer to cancel the stream independently.
		streamCtx, streamCancel := context.WithCancel(pc.ctx)

		// Stream health responses. This blocks until an error or context cancellation.
		err := pc.streamHealth(streamCtx, streamCancel, streamRetrier)

		// Always cancel the stream context to clean up resources.
		streamCancel()

		if err != nil {
			pc.setHealthError(err)
		}
	}
}

// streamHealth opens a health stream and processes responses until an error occurs.
// streamCancel is called by the staleness timer to unblock stream.Recv().
// On each successful message, streamRetrier is reset to use minimum backoff.
func (pc *poolerConnection) streamHealth(
	streamCtx context.Context,
	streamCancel context.CancelFunc,
	streamRetrier *retry.Retry,
) error {
	poolerID := pc.ID()

	// Reset healthTimedOut from any previous stream attempt so a shutdown during
	// this attempt isn't misclassified as a staleness timeout.
	pc.healthTimedOut.Store(false)

	// Open the health stream.
	recv, err := pc.pooler.SubscribeHealth(streamCtx)
	if err != nil {
		pc.logger.WarnContext(streamCtx, "failed to open health stream",
			"pooler_id", poolerID,
			"error", err)
		return fmt.Errorf("failed to open health stream: %w", err)
	}

	pc.logger.DebugContext(streamCtx, "health stream opened", "pooler_id", poolerID)

	// Set up staleness timer. If no message is received within the timeout,
	// the timer cancels the stream context to unblock stream.Recv().
	stalenessTimeout := constants.DefaultHealthCheckTimeout
	stalenessTimer := time.AfterFunc(stalenessTimeout, func() {
		pc.healthTimedOut.Store(true)
		pc.logger.WarnContext(streamCtx, "health stream timed out", "pooler_id", poolerID)
		streamCancel()
	})
	defer stalenessTimer.Stop()

	// Process responses from the stream.
	for {
		response, err := recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				pc.logger.DebugContext(streamCtx, "health stream closed by server", "pooler_id", poolerID)
				return io.EOF
			}
			if streamCtx.Err() != nil {
				// Stream context cancelled (either staleness timeout or shutdown).
				if pc.healthTimedOut.Load() {
					return errors.New("health stream timed out")
				}
				return nil
			}
			pc.logger.WarnContext(streamCtx, "health stream error",
				"pooler_id", poolerID,
				"error", err)
			return fmt.Errorf("health stream recv: %w", err)
		}

		// We received a message successfully.
		pc.healthTimedOut.Store(false)

		// Reset backoff since we got a successful message.
		streamRetrier.Reset()

		// Update staleness timeout from server recommendation if provided.
		if response.RecommendedStalenessTimeout != nil {
			newTimeout := response.RecommendedStalenessTimeout.AsDuration()
			if newTimeout > 0 {
				stalenessTimeout = newTimeout
			}
		}
		stalenessTimer.Reset(stalenessTimeout)

		// Process the health response.
		pc.processHealthResponse(response)
	}
}

// processHealthResponse updates the health state from a StreamPoolerHealthResponse.
// Creates a new immutable poolerHealth snapshot.
func (pc *poolerConnection) processHealthResponse(response *multipoolerservice.StreamPoolerHealthResponse) {
	poolerID := pc.ID()

	// Build new health snapshot from the response.
	newHealth := &poolerHealth{
		PoolerID:         response.PoolerId,
		ServingStatus:    response.ServingStatus,
		RoutingState:     response.RoutingState,
		ReplicationLagNs: response.ReplicationLagNs,
		RoutingVersion:   response.RoutingVersion,
		PlanEpoch:        response.PlanEpoch,
		LastError:        nil,
		LastResponse:     time.Now(),
	}

	pc.healthMu.Lock()
	prevHealth := pc.health
	pc.health = newHealth
	pc.healthMu.Unlock()

	// Log state changes.
	if prevHealth == nil || prevHealth.ServingStatus != newHealth.ServingStatus {
		pc.logger.Info("pooler health state changed",
			"pooler_id", poolerID,
			"serving_status", newHealth.ServingStatus.String(),
			"is_serving", newHealth.isServing())
	}

	// Notify listener of health update.
	if pc.onHealthUpdate != nil {
		pc.onHealthUpdate(pc)
	}
}

// setHealthError updates the health state to reflect an error while preserving
// existing metadata. Uses simpleCopy to create a new snapshot, then updates
// error-related fields. This ensures forward compatibility: any new fields
// added to poolerHealth will be automatically preserved.
func (pc *poolerConnection) setHealthError(err error) {
	pc.healthMu.Lock()
	newHealth := pc.health.simpleCopy()
	newHealth.ServingStatus = clustermetadatapb.PoolerServingStatus_DISABLED
	newHealth.LastError = err
	pc.health = newHealth
	pc.healthMu.Unlock()

	// Notify listener that health changed (pooler is now unhealthy).
	if pc.onHealthUpdate != nil {
		pc.onHealthUpdate(pc)
	}
}
