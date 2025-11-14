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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/store"
	"github.com/multigres/multigres/go/pb/clustermetadata"
)

// refreshAllPoolerHealth performs health checks on all poolers.
// This is the top-level function called by the health check loop.
// Similar to Vitess's refreshAllTablets.
func (re *Engine) refreshAllPoolerHealth() {
	re.refreshPoolerHealthUsing(re.pollPooler, false /* forceRefresh */)
}

// refreshPoolerHealthUsing refreshes pooler health using a provided loader function.
// Similar to Vitess's refreshTabletsUsing, this:
// - Iterates over all poolers in the store
// - Skips poolers that are already up-to-date (unless forceRefresh is true)
// - Calls the loader function concurrently for each pooler that needs checking
// - Tracks metrics for the entire cycle
func (re *Engine) refreshPoolerHealthUsing(
	loader func(poolerID *clustermetadata.ID, pooler *store.PoolerHealth),
	forceRefresh bool,
) {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		if re.healthCheckCycleDuration != nil {
			re.healthCheckCycleDuration.Record(re.ctx, duration.Seconds())
		}
		re.logger.Debug("health check cycle completed", "duration", duration)
	}()

	// Get snapshot of all poolers
	poolers := re.poolerStore.GetMap()

	// Track how many we'll check
	toCheck := 0
	for _, pooler := range poolers {
		if forceRefresh || !pooler.IsUpToDate {
			toCheck++
		}
	}

	re.logger.Debug("starting health check cycle",
		"total_poolers", len(poolers),
		"to_check", toCheck,
		"force_refresh", forceRefresh,
	)

	if re.poolersCheckedPerCycle != nil {
		re.poolersCheckedPerCycle.Record(re.ctx, int64(toCheck))
	}

	// Check each pooler concurrently
	var wg sync.WaitGroup
	for _, pooler := range poolers {
		// Skip if recently checked (unless forcing)
		if !forceRefresh && pooler.IsUpToDate {
			continue
		}

		// Parse the pooler ID from the MultiPooler record
		poolerID := pooler.MultiPooler.Id

		wg.Add(1)
		go func(id *clustermetadata.ID, p *store.PoolerHealth) {
			defer wg.Done()
			loader(id, p)
		}(poolerID, pooler)
	}
	wg.Wait()
}

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
func (re *Engine) pollPooler(poolerID *clustermetadata.ID, pooler *store.PoolerHealth) {
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
	if re.existsInCache(poolerIDStr) {
		// Recently polled, skip
		re.logger.Debug("skipping poll - recently checked",
			"pooler_id", poolerIDStr,
			"poll_interval", re.config.GetPoolerHealthCheckInterval(),
		)
		return
	}

	// Add to cache
	re.addToCache(poolerIDStr)

	// Quick check: if up-to-date and valid, skip
	if pooler.IsUpToDate && pooler.IsLastCheckValid {
		re.logger.Debug("skipping poll - already up to date",
			"pooler_id", poolerIDStr,
			"last_check", pooler.LastCheckSuccessful,
		)
		return
	}

	// Increment poll attempts counter
	if re.pollAttemptsCounter != nil {
		re.pollAttemptsCounter.Add(re.ctx, 1,
			metric.WithAttributes(
				attribute.String("database", pooler.MultiPooler.Database),
				attribute.String("tablegroup", pooler.MultiPooler.TableGroup),
			))
	}

	// Mark attempt timestamp
	pooler.LastCheckAttempted = time.Now()
	re.poolerStore.Set(poolerIDStr, pooler)

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

		// Mark as failed check
		pooler.IsLastCheckValid = false
		pooler.IsUpToDate = true // We tried, don't retry immediately
		re.poolerStore.Set(poolerIDStr, pooler)
		return
	}

	// Success!
	pooler.LastCheckSuccessful = time.Now()
	pooler.LastSeen = time.Now()
	pooler.IsUpToDate = true
	pooler.IsLastCheckValid = true
	re.poolerStore.Set(poolerIDStr, pooler)

	re.logger.Debug("pooler poll successful",
		"pooler_id", poolerIDStr,
		"type", pooler.MultiPooler.Type,
		"latency", time.Since(totalStart),
	)
}

// pollPrimaryPooler calls GetPrimaryStatus gRPC and updates pooler state.
func (re *Engine) pollPrimaryPooler(ctx context.Context, poolerID *clustermetadata.ID, pooler *store.PoolerHealth) error {
	// TODO: Implement gRPC call to GetPrimaryStatus
	// This should:
	// 1. Connect to pooler's gRPC endpoint
	// 2. Call GetPrimaryStatus()
	// 3. Update pooler fields with response:
	//    - ReadOnly
	//    - InRecovery
	//    - LSNPosition
	//    - ServerVersion
	//    - Reachable
	//    - Replica count and stats
	//    - etc.

	poolerIDStr := topo.MultiPoolerIDString(poolerID)
	re.logger.DebugContext(ctx, "polling primary pooler",
		"pooler_id", poolerIDStr,
		"hostname", pooler.MultiPooler.Hostname,
		"grpc_port", pooler.MultiPooler.PortMap["grpc"],
	)

	// Example structure (to be implemented):
	// grpcPort, ok := pooler.MultiPooler.PortMap["grpc"]
	// if !ok {
	//     return fmt.Errorf("no grpc port configured")
	// }
	//
	// address := fmt.Sprintf("%s:%d", pooler.MultiPooler.Hostname, grpcPort)
	// conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure())
	// if err != nil {
	//     return fmt.Errorf("failed to connect to pooler: %w", err)
	// }
	// defer conn.Close()
	//
	// client := poolerpb.NewPoolerClient(conn)
	// resp, err := client.GetPrimaryStatus(ctx, &poolerpb.GetPrimaryStatusRequest{})
	// if err != nil {
	//     return fmt.Errorf("GetPrimaryStatus failed: %w", err)
	// }
	//
	// // Update pooler state from response
	// pooler.ReadOnly = resp.ReadOnly
	// pooler.InRecovery = resp.InRecovery
	// pooler.LSNPosition = resp.LSN
	// pooler.ServerVersion = resp.ServerVersion
	// pooler.Reachable = true
	// ... etc

	// For now, just mark as successful (stub implementation)
	_ = ctx
	return nil
}

// pollReplicaPooler calls GetReplicaStatus gRPC and updates pooler state.
func (re *Engine) pollReplicaPooler(ctx context.Context, poolerID *clustermetadata.ID, pooler *store.PoolerHealth) error {
	// TODO: Implement gRPC call to GetReplicaStatus
	// This should:
	// 1. Connect to pooler's gRPC endpoint
	// 2. Call GetReplicaStatus()
	// 3. Update pooler fields with response:
	//    - ReplicationSource (pooler ID of primary)
	//    - ReplicationState (streaming, catchup, etc)
	//    - SecondsBehindPrimary
	//    - ReplayLSN
	//    - InRecovery
	//    - etc.

	poolerIDStr := topo.MultiPoolerIDString(poolerID)
	re.logger.DebugContext(ctx, "polling replica pooler",
		"pooler_id", poolerIDStr,
		"hostname", pooler.MultiPooler.Hostname,
		"grpc_port", pooler.MultiPooler.PortMap["grpc"],
	)

	// Example structure (to be implemented):
	// grpcPort, ok := pooler.MultiPooler.PortMap["grpc"]
	// if !ok {
	//     return fmt.Errorf("no grpc port configured")
	// }
	//
	// address := fmt.Sprintf("%s:%d", pooler.MultiPooler.Hostname, grpcPort)
	// conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure())
	// if err != nil {
	//     return fmt.Errorf("failed to connect to pooler: %w", err)
	// }
	// defer conn.Close()
	//
	// client := poolerpb.NewPoolerClient(conn)
	// resp, err := client.GetReplicaStatus(ctx, &poolerpb.GetReplicaStatusRequest{})
	// if err != nil {
	//     return fmt.Errorf("GetReplicaStatus failed: %w", err)
	// }
	//
	// // Update pooler state from response
	// pooler.ReplicationSource = resp.SourcePoolerID
	// pooler.SecondsBehindPrimary = resp.LagSeconds
	// pooler.ReplayLSN = resp.ReplayLSN
	// pooler.InRecovery = resp.InRecovery
	// pooler.Reachable = true
	// ... etc

	// For now, just mark as successful (stub implementation)
	_ = ctx
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

	// Get all poolers from the store
	poolers := re.poolerStore.GetMap()

	pushedCount := 0
	for poolerID, poolerInfo := range poolers {
		// Skip if recently attempted (either never attempted or older than interval)
		if !poolerInfo.LastCheckAttempted.IsZero() && poolerInfo.LastCheckAttempted.After(cutoff) {
			continue
		}

		// Push to queue for health checking
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
				re.pollPooler(poolerInfo.MultiPooler.Id, poolerInfo)
			}()
		}
	}
}
