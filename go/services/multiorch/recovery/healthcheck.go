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

package recovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdata "github.com/multigres/multigres/go/pb/consensusdata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// pollPooler performs health check on a single pooler instance.
// This is analogous to Vitess's DiscoverInstance.
//
// It includes:
// - Deduplication via cache (prevents redundant checks)
// - Latency tracking
// - Warnings if check exceeds poll interval
// - Detailed metrics for observability
//
// The function calls Status RPC which works for both PRIMARY and REPLICA poolers,
// then updates the in-memory store with the health metrics.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - poolerID: The pooler's ID
//   - pooler: The pooler's health info from the store
//   - forceDiscovery: If true, bypass cache and up-to-date checks (force poll)
func (re *Engine) pollPooler(ctx context.Context, poolerID *clustermetadata.ID, pooler *multiorchdatapb.PoolerHealthState, forceDiscovery bool) {
	poolerIDStr := topoclient.MultiPoolerIDString(poolerID)

	// Skip if this pooler is marked as forgotten (shouldn't happen, but defensive)
	if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
		re.logger.DebugContext(ctx, "skipping poll of nil pooler", "pooler_id", poolerIDStr)
		return
	}

	// Track latency
	totalStart := time.Now()

	defer func() {
		totalLatency := time.Since(totalStart)

		// Determine status for metric recording
		status := PoolerPollStatusSuccess

		// Warn if exceeded poll interval
		pollInterval := re.config.GetPoolerHealthCheckInterval()
		if totalLatency > pollInterval {
			status = PoolerPollStatusExceededInterval
			re.logger.Warn("pollPooler exceeded poll interval",
				"pooler_id", poolerIDStr,
				"poll_interval", pollInterval,
				"actual_duration", totalLatency,
			)
		}

		// Record poll duration with status
		re.metrics.poolerPollDuration.Record(
			re.shutdownCtx,
			totalLatency.Seconds(),
			pooler.MultiPooler.Database,
			pooler.MultiPooler.TableGroup,
			status,
		)
	}()

	// Check cache - prevent redundant checks within poll interval
	// (Similar to recentDiscoveryOperationKeys in Vitess)
	// Skip cache check if forceDiscovery is true
	if !forceDiscovery {
		if re.existsInCache(poolerIDStr) {
			// Recently polled, skip
			re.logger.DebugContext(ctx, "skipping poll - recently checked",
				"pooler_id", poolerIDStr,
				"poll_interval", re.config.GetPoolerHealthCheckInterval(),
			)
			return
		}
	}

	// Add to cache
	re.addToCache(poolerIDStr)

	// Quick check: if up-to-date and valid, skip (unless forceDiscovery)
	if !forceDiscovery && pooler.IsUpToDate && pooler.IsLastCheckValid {
		re.logger.DebugContext(ctx, "skipping poll - already up to date",
			"pooler_id", poolerIDStr,
			"last_check", pooler.LastCheckSuccessful.AsTime(),
		)
		return
	}

	// Log if this is a forced discovery
	if forceDiscovery {
		re.logger.InfoContext(ctx, "force polling pooler",
			"pooler_id", poolerIDStr,
			"type", pooler.MultiPooler.Type,
		)
	}

	// Note: Poll attempts are now tracked via the poll.duration histogram with status attribute

	// Mark attempt timestamp
	// Note: pooler is already a clone from store.Get(), safe to mutate directly
	now := time.Now()
	pooler.LastCheckAttempted = timestamppb.New(now)
	re.poolerStore.Set(poolerIDStr, pooler)

	// Call Status RPC which works for both PRIMARY and REPLICA poolers
	// Use provided context with 5 second timeout to prevent blocking forever
	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	statusResp, err := re.pollPoolerStatus(rpcCtx, poolerID, pooler)
	if err != nil {
		re.logger.WarnContext(ctx, "pooler poll failed",
			"pooler_id", poolerIDStr,
			"type", pooler.MultiPooler.Type,
			"error", err,
			"latency", time.Since(totalStart),
		)

		// Record failure in metrics (the deferred function will record with status)
		re.metrics.poolerPollDuration.Record(
			re.shutdownCtx,
			time.Since(totalStart).Seconds(),
			pooler.MultiPooler.Database,
			pooler.MultiPooler.TableGroup,
			PoolerPollStatusFailure,
		)

		// Mark as failed check
		// Note: pooler is already a clone from store.Get(), safe to mutate directly
		pooler.IsUpToDate = true // We tried, don't retry immediately
		pooler.IsLastCheckValid = false
		re.poolerStore.Set(poolerIDStr, pooler)
		return
	}

	// Success! Extract health metrics from status response and update store
	// Note: pooler is already a clone from store.Get(), safe to mutate directly
	successTime := time.Now()
	pooler.LastCheckSuccessful = timestamppb.New(successTime)
	pooler.LastSeen = timestamppb.New(successTime)
	pooler.IsUpToDate = true
	pooler.IsLastCheckValid = true

	// Status RPC now includes initialization fields and works without db connection
	status := statusResp.Status
	pooler.IsPostgresRunning = status.PostgresRunning
	pooler.PoolerType = status.PoolerType
	pooler.PrimaryStatus = status.PrimaryStatus
	pooler.ReplicationStatus = status.ReplicationStatus
	pooler.IsInitialized = status.IsInitialized
	pooler.HasDataDirectory = status.HasDataDirectory
	pooler.ConsensusTerm = status.ConsensusTerm

	// Fetch consensus status for stale primary detection and timeline divergence
	// This is a separate RPC call that gives us the consensus term and timeline info
	pooler.ConsensusStatus = re.fetchConsensusStatus(ctx, poolerID, pooler)

	re.logger.DebugContext(ctx, "pooler poll successful",
		"pooler_id", poolerIDStr,
		"topology_type", pooler.MultiPooler.Type,
		"reported_type", status.PoolerType,
		"is_initialized", status.IsInitialized,
		"postgres_running", status.PostgresRunning,
		"latency", time.Since(totalStart),
	)

	re.poolerStore.Set(poolerIDStr, pooler)
}

// poolerStatusResult wraps a Status RPC response.
// The Status RPC now includes initialization fields and works without db connection.
type poolerStatusResult struct {
	Status *multipoolermanagerdatapb.Status
}

// pollPoolerStatus calls the Status RPC which works for both PRIMARY and REPLICA poolers.
// The Status RPC returns unified status information that includes both primary and replication
// status, populated based on what type the pooler believes itself to be.
// The Status RPC also includes initialization fields and works even when the database is unavailable.
// Returns the status for the caller to extract and store metrics.
func (re *Engine) pollPoolerStatus(ctx context.Context, poolerID *clustermetadata.ID, pooler *multiorchdatapb.PoolerHealthState) (*poolerStatusResult, error) {
	poolerIDStr := topoclient.MultiPoolerIDString(poolerID)

	re.logger.DebugContext(ctx, "polling pooler status",
		"pooler_id", poolerIDStr,
		"hostname", pooler.MultiPooler.Hostname,
		"grpc_port", pooler.MultiPooler.PortMap["grpc"],
		"type", pooler.MultiPooler.Type,
	)

	// Call Status RPC (now works without db connection)
	resp, err := re.rpcClient.Status(ctx, pooler.MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("Status RPC failed: %w", err)
	}

	// Validate response
	if resp == nil || resp.Status == nil {
		return nil, errors.New("received nil status response")
	}

	// Log status information for observability
	re.logger.DebugContext(ctx, "pooler status received",
		"pooler_id", poolerIDStr,
		"pooler_type", resp.Status.PoolerType,
		"is_initialized", resp.Status.IsInitialized,
		"postgres_running", resp.Status.PostgresRunning,
		"has_primary_status", resp.Status.PrimaryStatus != nil,
		"has_replication_status", resp.Status.ReplicationStatus != nil,
	)

	return &poolerStatusResult{
		Status: resp.Status,
	}, nil
}

// fetchConsensusStatus fetches the consensus status from a pooler.
// This provides consensus term and timeline information needed for stale primary detection
// and timeline divergence detection.
// Returns nil on failure (non-critical for health check).
func (re *Engine) fetchConsensusStatus(ctx context.Context, poolerID *clustermetadata.ID, pooler *multiorchdatapb.PoolerHealthState) *consensusdata.StatusResponse {
	poolerIDStr := topoclient.MultiPoolerIDString(poolerID)

	// Call ConsensusStatus RPC
	resp, err := re.rpcClient.ConsensusStatus(ctx, pooler.MultiPooler, &consensusdata.StatusRequest{})
	if err != nil {
		// Log at debug level - consensus status is optional for health checks
		re.logger.DebugContext(ctx, "consensus status RPC failed",
			"pooler_id", poolerIDStr,
			"error", err,
		)
		return nil
	}

	if resp == nil {
		return nil
	}

	re.logger.DebugContext(ctx, "consensus status received",
		"pooler_id", poolerIDStr,
		"current_term", resp.CurrentTerm,
		"has_timeline_info", resp.TimelineInfo != nil,
	)

	return resp
}

// existsInCache checks if a pooler ID was recently polled.
func (re *Engine) existsInCache(poolerID string) bool {
	re.recentPollCacheMu.Lock()
	defer re.recentPollCacheMu.Unlock()

	if lastPoll, ok := re.recentPollCache[poolerID]; ok {
		// Check if entry is still valid
		if time.Since(lastPoll) < re.config.GetPoolerHealthCheckInterval() {
			return true
		}
		// Entry expired, remove it
		delete(re.recentPollCache, poolerID)
	}
	return false
}

// addToCache adds a pooler ID to the recent poll cache.
func (re *Engine) addToCache(poolerID string) {
	re.recentPollCacheMu.Lock()
	defer re.recentPollCacheMu.Unlock()

	re.recentPollCache[poolerID] = time.Now()

	// Cleanup old entries periodically (simple approach)
	if len(re.recentPollCache) > 10000 {
		// Remove entries older than 2x poll interval
		cutoff := time.Now().Add(-2 * re.config.GetPoolerHealthCheckInterval())
		for id, lastPoll := range re.recentPollCache {
			if lastPoll.Before(cutoff) {
				delete(re.recentPollCache, id)
			}
		}
	}
}

// queuePoolersHealthCheck identifies poolers that need health checking and pushes them to the queue.
// This is called periodically by the health check loop.
func (re *Engine) queuePoolersHealthCheck() {
	pollInterval := re.config.GetPoolerHealthCheckInterval()
	cutoff := time.Now().Add(-pollInterval)

	pushedCount := 0

	// Collect poolers to queue and poolers that need IsUpToDate reset
	var poolersToQueue []string
	var poolersToReset []struct {
		id   string
		info *multiorchdatapb.PoolerHealthState
	}

	// Iterate over poolers using Range() - do NOT call Set inside Range (deadlock!)
	re.poolerStore.Range(func(poolerID string, poolerInfo *multiorchdatapb.PoolerHealthState) bool {
		// Skip if recently attempted (either never attempted or older than interval)
		lastCheckAttempted := time.Time{}
		if poolerInfo.LastCheckAttempted != nil {
			lastCheckAttempted = poolerInfo.LastCheckAttempted.AsTime()
		}
		if !lastCheckAttempted.IsZero() && lastCheckAttempted.After(cutoff) {
			return true // continue iteration
		}

		// Collect pooler for queueing
		poolersToQueue = append(poolersToQueue, poolerID)

		// If IsUpToDate is true, collect for reset (will be done after Range completes)
		// Without this reset, pollPooler skips if IsUpToDate && IsLastCheckValid are both true.
		if poolerInfo.IsUpToDate {
			poolerInfo.IsUpToDate = false
			poolersToReset = append(poolersToReset, struct {
				id   string
				info *multiorchdatapb.PoolerHealthState
			}{poolerID, poolerInfo})
		}

		return true // continue iteration
	})

	// Now safe to call Set (Range lock is released)
	for _, p := range poolersToReset {
		re.poolerStore.Set(p.id, p.info)
	}

	// Push collected poolers to queue
	for _, poolerID := range poolersToQueue {
		re.healthCheckQueue.Push(poolerID)
		pushedCount++
	}

	if pushedCount > 0 {
		re.logger.Debug("pushed poolers to health check queue",
			"count", pushedCount,
			"poll_interval", pollInterval,
		)
	}
}

// handlePoolerHealthChecks runs a worker loop that consumes from the health check queue.
// Multiple instances of this function run concurrently as worker goroutines.
func (re *Engine) handlePoolerHealthChecks() {
	for {
		// Consume blocks until an item is available or context is cancelled
		poolerID, release, ok := re.healthCheckQueue.Consume(re.shutdownCtx)
		if !ok {
			// Context cancelled, exit the worker
			return
		}

		// Perform the health check with context
		func() {
			defer release()

			// Get pooler info from store
			poolerInfo, ok := re.poolerStore.Get(poolerID)
			if !ok {
				re.logger.Debug("pooler not found in store, skipping health check",
					"pooler_id", poolerID,
				)
				return
			}

			// Poll the pooler with engine context (respects shutdown)
			re.pollPooler(re.shutdownCtx, poolerInfo.MultiPooler.Id, poolerInfo, false /* forceDiscovery */)
		}()
	}
}
