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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/pb/clustermetadata"
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
func (re *Engine) pollPooler(ctx context.Context, poolerID *clustermetadata.ID, pooler *store.PoolerHealth, forceDiscovery bool) {
	poolerIDStr := topo.MultiPoolerIDString(poolerID)

	// Skip if this pooler is marked as forgotten (shouldn't happen, but defensive)
	if pooler == nil || pooler.ID == nil {
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
			re.ctx,
			totalLatency.Seconds(),
			pooler.Database,
			pooler.TableGroup,
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
			"last_check", pooler.LastCheckSuccessful,
		)
		return
	}

	// Log if this is a forced discovery
	if forceDiscovery {
		re.logger.InfoContext(ctx, "force polling pooler",
			"pooler_id", poolerIDStr,
			"type", pooler.TopoPoolerType,
		)
	}

	// Note: Poll attempts are now tracked via the poll.duration histogram with status attribute

	// Mark attempt timestamp - create new struct to avoid race condition
	now := time.Now()
	updated := store.NewPoolerHealthFromMultiPooler(pooler.ToMultiPooler())
	updated.LastCheckAttempted = now
	updated.LastCheckSuccessful = pooler.LastCheckSuccessful
	updated.LastSeen = pooler.LastSeen
	updated.IsUpToDate = pooler.IsUpToDate
	updated.IsLastCheckValid = pooler.IsLastCheckValid
	re.poolerStore.Set(poolerIDStr, updated)
	pooler = updated // use updated for rest of function

	// Call Status RPC which works for both PRIMARY and REPLICA poolers
	// Use provided context with 5 second timeout to prevent blocking forever
	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	statusResp, err := re.pollPoolerStatus(rpcCtx, poolerID, pooler)
	if err != nil {
		re.logger.WarnContext(ctx, "pooler poll failed",
			"pooler_id", poolerIDStr,
			"type", pooler.TopoPoolerType,
			"error", err,
			"latency", time.Since(totalStart),
		)

		// Record failure in metrics (the deferred function will record with status)
		re.metrics.poolerPollDuration.Record(
			re.ctx,
			time.Since(totalStart).Seconds(),
			pooler.Database,
			pooler.TableGroup,
			PoolerPollStatusFailure,
		)

		// Mark as failed check - create new struct to avoid race condition
		failed := store.NewPoolerHealthFromMultiPooler(pooler.ToMultiPooler())
		failed.LastCheckAttempted = pooler.LastCheckAttempted
		failed.LastCheckSuccessful = pooler.LastCheckSuccessful
		failed.LastSeen = pooler.LastSeen
		failed.IsUpToDate = true // We tried, don't retry immediately
		failed.IsLastCheckValid = false
		re.poolerStore.Set(poolerIDStr, failed)
		return
	}

	// Success! Extract health metrics from status response and update store
	successTime := time.Now()
	success := store.NewPoolerHealthFromMultiPooler(pooler.ToMultiPooler())
	success.LastCheckAttempted = pooler.LastCheckAttempted
	success.LastCheckSuccessful = successTime
	success.LastSeen = successTime
	success.IsUpToDate = true
	success.IsLastCheckValid = true
	success.PoolerType = statusResp.Status.PoolerType

	// Populate type-specific fields based on what the pooler reports
	if statusResp.Status.PrimaryStatus != nil {
		ps := statusResp.Status.PrimaryStatus
		success.PrimaryLSN = ps.Lsn
		success.PrimaryReady = ps.Ready
		success.PrimaryConnectedFollowers = ps.ConnectedFollowers
		success.PrimarySyncConfig = ps.SyncReplicationConfig
	}

	if statusResp.Status.ReplicationStatus != nil {
		rs := statusResp.Status.ReplicationStatus
		success.ReplicaLastReplayLSN = rs.LastReplayLsn
		success.ReplicaLastReceiveLSN = rs.LastReceiveLsn
		success.ReplicaIsWalReplayPaused = rs.IsWalReplayPaused
		success.ReplicaWalReplayPauseState = rs.WalReplayPauseState
		success.ReplicaLastXactReplayTimestamp = rs.LastXactReplayTimestamp
		success.ReplicaPrimaryConnInfo = rs.PrimaryConnInfo

		// Convert lag duration to milliseconds
		if rs.Lag != nil {
			success.ReplicaLagMillis = rs.Lag.AsDuration().Milliseconds()
		}
	}

	re.poolerStore.Set(poolerIDStr, success)

	re.logger.DebugContext(ctx, "pooler poll successful",
		"pooler_id", poolerIDStr,
		"topology_type", pooler.TopoPoolerType,
		"reported_type", statusResp.Status.PoolerType,
		"latency", time.Since(totalStart),
	)
}

// pollPoolerStatus calls the Status RPC which works for both PRIMARY and REPLICA poolers.
// The Status RPC returns unified status information that includes both primary and replication
// status, populated based on what type the pooler believes itself to be.
// Returns the status response for the caller to extract and store metrics.
func (re *Engine) pollPoolerStatus(ctx context.Context, poolerID *clustermetadata.ID, pooler *store.PoolerHealth) (*multipoolermanagerdatapb.StatusResponse, error) {
	poolerIDStr := topo.MultiPoolerIDString(poolerID)

	re.logger.DebugContext(ctx, "polling pooler status",
		"pooler_id", poolerIDStr,
		"hostname", pooler.Hostname,
		"grpc_port", pooler.PortMap["grpc"],
		"type", pooler.TopoPoolerType,
	)

	// Call Status RPC
	resp, err := re.rpcClient.Status(ctx, pooler.ToMultiPooler(), &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get status from pooler: %w", err)
	}

	// Validate response
	if resp == nil || resp.Status == nil {
		return nil, fmt.Errorf("received nil status response")
	}

	// Log status information for observability
	re.logger.DebugContext(ctx, "pooler status received",
		"pooler_id", poolerIDStr,
		"pooler_type", resp.Status.PoolerType,
		"has_primary_status", resp.Status.PrimaryStatus != nil,
		"has_replication_status", resp.Status.ReplicationStatus != nil,
	)

	return resp, nil
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

	// Iterate over poolers using Range() to hold lock during iteration
	re.poolerStore.Range(func(poolerID string, poolerInfo *store.PoolerHealth) bool {
		// Skip if recently attempted (either never attempted or older than interval)
		if !poolerInfo.LastCheckAttempted.IsZero() && poolerInfo.LastCheckAttempted.After(cutoff) {
			return true // continue iteration
		}

		// Push to queue for health checking
		re.healthCheckQueue.Push(poolerID)
		pushedCount++
		return true // continue iteration
	})

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
		poolerID, release, ok := re.healthCheckQueue.Consume(re.ctx)
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
			re.pollPooler(re.ctx, poolerInfo.ID, poolerInfo, false /* forceDiscovery */)
		}()
	}
}
