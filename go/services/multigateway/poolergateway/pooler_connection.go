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

// TODO: Add PoolerConnection streaming tests. This requires either:
// - A mock gRPC server using google.golang.org/grpc/test/bufconn for unit tests
// - Integration tests with a real multipooler instance
// Tests should cover: checkConn retry loop, streamHealth, staleness timeout detection
// Note: PoolerHealth tests exist in pooler_health_test.go (IsServing, SimpleCopy)

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/tools/grpccommon"
	"github.com/multigres/multigres/go/tools/retry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// defaultHealthRetryDelay is the initial delay before retrying a failed health stream.
	// Matches Vitess DefaultHealthCheckRetryDelay.
	defaultHealthRetryDelay = 5 * time.Second

	// defaultHealthCheckTimeout is the timeout for detecting a stale health stream.
	// If no message is received within this duration, the connection is marked unhealthy.
	// Matches Vitess DefaultHealthCheckTimeout.
	defaultHealthCheckTimeout = 1 * time.Minute
)

// errPoolerUninitialized is the initial error before health stream connects.
// Matches Vitess errUninitialized pattern.
var errPoolerUninitialized = errors.New("pooler health not initialized")

// PoolerHealth represents the health state received from a multipooler.
// This mirrors Vitess's TabletHealth pattern - a snapshot of health state
// that can be safely passed around without synchronization.
type PoolerHealth struct {
	// Target identifies the tablegroup, shard, and pooler type.
	Target *query.Target

	// PoolerID identifies the multipooler instance.
	PoolerID *clustermetadatapb.ID

	// ServingStatus is the serving state reported by the pooler.
	ServingStatus clustermetadatapb.PoolerServingStatus

	// PrimaryObservation contains the pooler's view of who the primary is.
	// Used for term-based primary reconciliation.
	PrimaryObservation *multipoolerpb.PrimaryObservation

	// LastError is the most recent error from the health stream.
	LastError error

	// LastResponse is when we last received a health update.
	LastResponse time.Time
}

// IsServing returns true if the pooler is serving traffic.
func (h *PoolerHealth) IsServing() bool {
	if h == nil {
		return false
	}
	return h.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING ||
		h.ServingStatus == clustermetadatapb.PoolerServingStatus_SERVING_RDONLY
}

// SimpleCopy returns a shallow copy of the PoolerHealth.
// This is not a deep copy: pointer fields (Target, PoolerID, PrimaryObservation)
// reference the same underlying objects. This is safe because these proto objects
// are treated as immutable - they are never modified after creation.
// Follows the Vitess TabletHealth.SimpleCopy pattern.
func (h *PoolerHealth) SimpleCopy() *PoolerHealth {
	if h == nil {
		return nil
	}
	return &PoolerHealth{
		Target:             h.Target,
		PoolerID:           h.PoolerID,
		ServingStatus:      h.ServingStatus,
		PrimaryObservation: h.PrimaryObservation,
		LastError:          h.LastError,
		LastResponse:       h.LastResponse,
	}
}

// PoolerConnection manages a single gRPC connection to a multipooler instance.
// It wraps a QueryService and provides access to pooler metadata.
//
// A PoolerConnection exists if and only if we are actively connected to the pooler.
// The LoadBalancer creates and destroys PoolerConnections based on discovery events.
//
// The connection maintains a health stream to the multipooler and tracks serving state.
// This follows the Vitess tabletHealthCheck pattern.
//
// TODO: Use health state for routing - LoadBalancer.GetConnection should filter by IsServing().
type PoolerConnection struct {
	// mu protects poolerInfo from concurrent access
	mu sync.Mutex

	// poolerInfo contains the pooler metadata from discovery
	poolerInfo *topoclient.MultiPoolerInfo

	// conn is the underlying gRPC connection
	conn *grpc.ClientConn

	// client is the gRPC client for health streaming
	client multipoolerpb.MultiPoolerServiceClient

	// queryService handles query execution over gRPC
	queryService queryservice.QueryService

	// serviceClient is the gRPC client for admin operations (auth, health, etc.)
	serviceClient multipoolerpb.MultiPoolerServiceClient

	// logger for debugging
	logger *slog.Logger

	// ctx and cancel control the health stream goroutine lifecycle.
	// cancel must be called before discarding PoolerConnection to ensure
	// the checkConn goroutine terminates. (Vitess pattern)
	ctx    context.Context
	cancel context.CancelFunc

	// healthMu protects the health field
	healthMu sync.Mutex

	// health contains the current health state from the health stream.
	// Updated atomically as a unit when new health responses arrive.
	health *PoolerHealth

	// timedOut indicates if the health stream has timed out.
	// Accessed atomically because there's a race between the timeout
	// goroutine and the stream processing. (Vitess pattern)
	timedOut atomic.Bool

	// onHealthUpdate is called when health state changes.
	// TODO: Wire up in LoadBalancer.addPooler() to trigger routing updates.
	onHealthUpdate func(*PoolerConnection)
}

// NewPoolerConnection creates a new connection to a multipooler instance and
// starts health streaming automatically.
//
// The onHealthUpdate callback is invoked when health state changes. It may be
// nil if health updates don't need to be observed.
//
// Close must be called when the connection is no longer needed to stop the
// health stream goroutine and release resources.
func NewPoolerConnection(
	pooler *clustermetadatapb.MultiPooler,
	logger *slog.Logger,
	onHealthUpdate func(*PoolerConnection),
) (*PoolerConnection, error) {
	poolerInfo := &topoclient.MultiPoolerInfo{MultiPooler: pooler}
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)
	addr := poolerInfo.Addr()

	logger.Debug("creating pooler connection",
		"pooler_id", poolerID,
		"addr", addr,
		"type", pooler.Type.String())

	// Create gRPC connection with telemetry attributes
	conn, err := grpccommon.NewClient(addr,
		grpccommon.WithAttributes(rpcclient.PoolerSpanAttributes(pooler.Id)...),
		grpccommon.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for pooler %s at %s: %w", poolerID, addr, err)
	}

	// Create internal context for health stream goroutine.
	// TODO: This should inherit from the multigateway command context for proper
	// shutdown propagation, but shouldn't use a short-lived request context since
	// health monitoring is long-running. For now using TODO as a placeholder.
	ctx, cancel := context.WithCancel(context.TODO())

	// Create QueryService wrapper
	queryService := newGRPCQueryService(conn, poolerID, logger)

	// Create service client for admin operations
	serviceClient := multipoolerpb.NewMultiPoolerServiceClient(conn)

	// Initialize health state to NOT_SERVING until health stream provides data.
	// This matches Vitess where tablets start with errUninitialized.
	initialTarget := &query.Target{
		TableGroup: pooler.GetTableGroup(),
		Shard:      pooler.GetShard(),
		PoolerType: pooler.Type,
	}

	pc := &PoolerConnection{
		poolerInfo:     poolerInfo,
		conn:           conn,
		client:         serviceClient,
		queryService:   queryService,
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
		onHealthUpdate: onHealthUpdate,
		health: &PoolerHealth{
			Target:        initialTarget,
			PoolerID:      pooler.Id,
			ServingStatus: clustermetadatapb.PoolerServingStatus_NOT_SERVING,
			LastError:     errPoolerUninitialized,
		},
	}

	// Start health stream goroutine
	go pc.checkConn()

	logger.Debug("pooler connection established",
		"pooler_id", poolerID,
		"addr", addr)

	return pc, nil
}

// ID returns the unique identifier for this pooler connection.
func (pc *PoolerConnection) ID() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return topoclient.MultiPoolerIDString(pc.poolerInfo.Id)
}

// Cell returns the cell where this pooler is located.
func (pc *PoolerConnection) Cell() string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.poolerInfo.Id.GetCell()
}

// Type returns the pooler type (PRIMARY or REPLICA).
func (pc *PoolerConnection) Type() clustermetadatapb.PoolerType {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.poolerInfo.Type
}

// PoolerInfo returns the underlying pooler metadata.
func (pc *PoolerConnection) PoolerInfo() *topoclient.MultiPoolerInfo {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.poolerInfo
}

// ServiceClient returns the MultiPoolerServiceClient for admin operations.
// This can be used for authentication, health checks, and other system-level operations.
func (pc *PoolerConnection) ServiceClient() multipoolerpb.MultiPoolerServiceClient {
	return pc.serviceClient
}

// QueryService returns the QueryService for executing queries on this pooler.
// This is the primary interface for query execution and should be used by callers
// who need to route queries to this specific pooler instance.
func (pc *PoolerConnection) QueryService() queryservice.QueryService {
	return pc.queryService
}

// UpdateMetadata updates the pooler metadata without recreating the gRPC connection.
// This is used when pooler properties change (e.g., type changes from REPLICA to PRIMARY during promotion).
func (pc *PoolerConnection) UpdateMetadata(pooler *clustermetadatapb.MultiPooler) {
	pc.mu.Lock()
	pc.poolerInfo = &topoclient.MultiPoolerInfo{MultiPooler: pooler}
	poolerID := topoclient.MultiPoolerIDString(pooler.Id)
	poolerType := pooler.Type.String()
	pc.mu.Unlock()

	pc.logger.Debug("updated pooler metadata",
		"pooler_id", poolerID,
		"type", poolerType)
}

// Close stops the health stream goroutine and closes the gRPC connection.
func (pc *PoolerConnection) Close() error {
	poolerID := pc.ID()
	pc.logger.Debug("closing pooler connection", "pooler_id", poolerID)

	// Cancel the health stream context to stop the checkConn goroutine
	if pc.cancel != nil {
		pc.cancel()
	}

	if err := pc.queryService.Close(); err != nil {
		return fmt.Errorf("failed to close query service for pooler %s: %w", poolerID, err)
	}
	return nil
}

// Health returns the current health state.
// The returned PoolerHealth is a snapshot that can be safely used without
// synchronization. We don't deep-copy because the PoolerHealth object is
// never modified after creation (same pattern as Vitess SimpleCopy).
func (pc *PoolerConnection) Health() *PoolerHealth {
	pc.healthMu.Lock()
	defer pc.healthMu.Unlock()
	return pc.health
}

// checkConn performs health checking on the pooler connection.
// It continuously attempts to maintain a health stream, retrying with
// exponential backoff on failures. This follows the Vitess tabletHealthCheck pattern.
func (pc *PoolerConnection) checkConn() {
	poolerID := pc.ID()
	pc.logger.Debug("starting health check loop", "pooler_id", poolerID)

	streamRetrier := retry.New(defaultHealthRetryDelay, defaultHealthCheckTimeout)

	for attempt, waitErr := range streamRetrier.Attempts(pc.ctx) {
		if waitErr != nil {
			// Context cancelled - connection is being closed.
			pc.logger.Debug("health check loop exiting",
				"pooler_id", poolerID,
				"attempt", attempt,
				"reason", waitErr)
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
func (pc *PoolerConnection) streamHealth(
	streamCtx context.Context,
	streamCancel context.CancelFunc,
	streamRetrier *retry.Retry,
) error {
	poolerID := pc.ID()

	// Open the health stream.
	stream, err := pc.client.StreamPoolerHealth(streamCtx, &multipoolerpb.StreamPoolerHealthRequest{})
	if err != nil {
		pc.logger.WarnContext(streamCtx, "failed to open health stream",
			"pooler_id", poolerID,
			"error", err)
		return fmt.Errorf("failed to open health stream: %w", err)
	}

	pc.logger.DebugContext(streamCtx, "health stream opened", "pooler_id", poolerID)

	// Set up staleness timer. If no message is received within the timeout,
	// the timer cancels the stream context to unblock stream.Recv().
	stalenessTimeout := defaultHealthCheckTimeout
	stalenessTimer := time.AfterFunc(stalenessTimeout, func() {
		pc.timedOut.Store(true)
		pc.logger.Warn("health stream timed out", "pooler_id", poolerID)
		streamCancel()
	})
	defer stalenessTimer.Stop()

	// Process responses from the stream.
	for {
		response, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				pc.logger.DebugContext(streamCtx, "health stream closed by server", "pooler_id", poolerID)
				return io.EOF
			}
			if streamCtx.Err() != nil {
				// Stream context cancelled (either staleness timeout or shutdown).
				if pc.timedOut.Load() {
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
		pc.timedOut.Store(false)

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
// Creates a new PoolerHealth snapshot (following Vitess's immutable snapshot pattern).
func (pc *PoolerConnection) processHealthResponse(response *multipoolerpb.StreamPoolerHealthResponse) {
	poolerID := pc.ID()

	// Build new health snapshot from the response.
	newHealth := &PoolerHealth{
		Target:             response.Target,
		PoolerID:           response.PoolerId,
		ServingStatus:      response.ServingStatus,
		PrimaryObservation: response.PrimaryObservation,
		LastError:          nil,
		LastResponse:       time.Now(),
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
			"is_serving", newHealth.IsServing())
	}

	// Notify listener of health update.
	if pc.onHealthUpdate != nil {
		pc.onHealthUpdate(pc)
	}
}

// setHealthError updates the health state to reflect an error while preserving
// existing metadata. Uses SimpleCopy to create a new snapshot, then updates
// error-related fields. This ensures forward compatibility: any new fields
// added to PoolerHealth will be automatically preserved.
func (pc *PoolerConnection) setHealthError(err error) {
	pc.healthMu.Lock()
	newHealth := pc.health.SimpleCopy()
	newHealth.ServingStatus = clustermetadatapb.PoolerServingStatus_NOT_SERVING
	newHealth.LastError = err
	pc.health = newHealth
	pc.healthMu.Unlock()

	// Notify listener that health changed (pooler is now unhealthy).
	if pc.onHealthUpdate != nil {
		pc.onHealthUpdate(pc)
	}
}
