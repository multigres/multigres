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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/pb/clustermetadata"
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
// The function calls either GetPrimaryStatus or GetReplicaStatus via gRPC
// depending on the pooler type, then updates the in-memory store.
//
// Parameters:
//   - poolerID: The pooler's ID
//   - pooler: The pooler's health info from the store
//   - forceDiscovery: If true, bypass cache and up-to-date checks (force poll)
func (re *Engine) pollPooler(poolerID *clustermetadata.ID, pooler *store.PoolerHealth, forceDiscovery bool) {
	// Skip if this pooler is marked as forgotten (shouldn't happen, but defensive)
	if pooler == nil || pooler.MultiPooler == nil {
		re.logger.Debug("skipping poll of nil pooler", "pooler_id", topo.MultiPoolerIDString(poolerID))
		return
	}

	poolerIDStr := topo.MultiPoolerIDString(poolerID)

	// Track latency
	totalStart := time.Now()

	defer func() {
		totalLatency := time.Since(totalStart)

		// Warn if exceeded poll interval
		pollInterval := re.config.GetPoolerHealthCheckInterval()
		if totalLatency > pollInterval {
			if re.poolerPollExceededCounter != nil {
				re.poolerPollExceededCounter.Add(re.ctx, 1,
					metric.WithAttributes(
						attribute.String("database", pooler.MultiPooler.Database),
						attribute.String("tablegroup", pooler.MultiPooler.TableGroup),
					))
			}
			re.logger.Warn("pollPooler exceeded poll interval",
				"pooler_id", poolerIDStr,
				"poll_interval", pollInterval,
				"actual_duration", totalLatency,
			)
		}

		// Record latency histogram
		if re.pollLatency != nil {
			re.pollLatency.Record(re.ctx, totalLatency.Seconds(),
				metric.WithAttributes(
					attribute.String("database", pooler.MultiPooler.Database),
					attribute.String("tablegroup", pooler.MultiPooler.TableGroup),
				))
		}
	}()

	// Check cache - prevent redundant checks within poll interval
	// (Similar to recentDiscoveryOperationKeys in Vitess)
	// Skip cache check if forceDiscovery is true
	if !forceDiscovery {
		if re.existsInCache(poolerIDStr) {
			// Recently polled, skip
			re.logger.Debug("skipping poll - recently checked",
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
		re.logger.Debug("skipping poll - already up to date",
			"pooler_id", poolerIDStr,
			"last_check", pooler.LastCheckSuccessful,
		)
		return
	}

	// Log if this is a forced discovery
	if forceDiscovery {
		re.logger.Info("force polling pooler",
			"pooler_id", poolerIDStr,
			"type", pooler.MultiPooler.Type,
		)
	}

	// Increment poll attempts counter
	if re.pollAttemptsCounter != nil {
		re.pollAttemptsCounter.Add(re.ctx, 1,
			metric.WithAttributes(
				attribute.String("database", pooler.MultiPooler.Database),
				attribute.String("tablegroup", pooler.MultiPooler.TableGroup),
			))
	}

	// Mark attempt timestamp - create new struct to avoid race condition
	now := time.Now()
	updated := &store.PoolerHealth{
		MultiPooler:         pooler.MultiPooler,
		LastCheckAttempted:  now,
		LastCheckSuccessful: pooler.LastCheckSuccessful,
		LastSeen:            pooler.LastSeen,
		IsUpToDate:          pooler.IsUpToDate,
		IsLastCheckValid:    pooler.IsLastCheckValid,
	}
	re.poolerStore.Set(poolerIDStr, updated)
	pooler = updated // use updated for rest of function

	// Call appropriate RPC based on pooler type
	ctx, cancel := context.WithTimeout(re.ctx, 5*time.Second)
	defer cancel()

	var err error
	switch pooler.MultiPooler.Type {
	case clustermetadata.PoolerType_PRIMARY:
		err = re.pollPrimaryPooler(ctx, poolerID, pooler)
	case clustermetadata.PoolerType_REPLICA:
		err = re.pollReplicaPooler(ctx, poolerID, pooler)
	default:
		err = fmt.Errorf("unknown pooler type: %v", pooler.MultiPooler.Type)
	}

	if err != nil {
		re.logger.Warn("pooler poll failed",
			"pooler_id", poolerIDStr,
			"type", pooler.MultiPooler.Type,
			"error", err,
			"latency", time.Since(totalStart),
		)
		if re.pollFailuresCounter != nil {
			re.pollFailuresCounter.Add(re.ctx, 1,
				metric.WithAttributes(
					attribute.String("database", pooler.MultiPooler.Database),
					attribute.String("tablegroup", pooler.MultiPooler.TableGroup),
				))
		}

		// Mark as failed check - create new struct to avoid race condition
		failed := &store.PoolerHealth{
			MultiPooler:         pooler.MultiPooler,
			LastCheckAttempted:  pooler.LastCheckAttempted,
			LastCheckSuccessful: pooler.LastCheckSuccessful,
			LastSeen:            pooler.LastSeen,
			IsUpToDate:          true, // We tried, don't retry immediately
			IsLastCheckValid:    false,
		}
		re.poolerStore.Set(poolerIDStr, failed)
		return
	}

	// Success! Create new struct to avoid race condition
	successTime := time.Now()
	success := &store.PoolerHealth{
		MultiPooler:         pooler.MultiPooler,
		LastCheckAttempted:  pooler.LastCheckAttempted,
		LastCheckSuccessful: successTime,
		LastSeen:            successTime,
		IsUpToDate:          true,
		IsLastCheckValid:    true,
	}
	re.poolerStore.Set(poolerIDStr, success)

	re.logger.Debug("pooler poll successful",
		"pooler_id", poolerIDStr,
		"type", pooler.MultiPooler.Type,
		"latency", time.Since(totalStart),
	)
}

// pollPrimaryPooler calls GetPrimaryStatus gRPC and updates pooler state.
func (re *Engine) pollPrimaryPooler(ctx context.Context, poolerID *clustermetadata.ID, pooler *store.PoolerHealth) error {
	// TODO: Implement gRPC call to GetPrimaryStatus
	poolerIDStr := topo.MultiPoolerIDString(poolerID)
	re.logger.DebugContext(ctx, "polling primary pooler",
		"pooler_id", poolerIDStr,
		"hostname", pooler.MultiPooler.Hostname,
		"grpc_port", pooler.MultiPooler.PortMap["grpc"],
	)
	return nil
}

// pollReplicaPooler calls GetReplicaStatus gRPC and updates pooler state.
func (re *Engine) pollReplicaPooler(ctx context.Context, poolerID *clustermetadata.ID, pooler *store.PoolerHealth) error {
	// TODO: Implement gRPC call to GetReplicaStatus
	poolerIDStr := topo.MultiPoolerIDString(poolerID)
	re.logger.DebugContext(ctx, "polling replica pooler",
		"pooler_id", poolerIDStr,
		"hostname", pooler.MultiPooler.Hostname,
		"grpc_port", pooler.MultiPooler.PortMap["grpc"],
	)
	return nil
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
		select {
		case <-re.ctx.Done():
			return
		default:
			// Consume blocks until an item is available
			poolerID := re.healthCheckQueue.Consume()

			// Perform the health check
			func() {
				defer re.healthCheckQueue.Release(poolerID)

				// Get pooler info from store
				poolerInfo, ok := re.poolerStore.Get(poolerID)
				if !ok {
					re.logger.Debug("pooler not found in store, skipping health check",
						"pooler_id", poolerID,
					)
					return
				}

				// Poll the pooler
				re.pollPooler(poolerInfo.MultiPooler.Id, poolerInfo, false /* forceDiscovery */)
			}()
		}
	}
}
