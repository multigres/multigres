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
	"fmt"
	"time"

	"github.com/multigres/multigres/go/multiorch/store"
)

// runBookkeeping performs periodic bookkeeping tasks.
func (re *Engine) runBookkeeping() {
	re.logger.Debug("running bookkeeping tasks")

	// Reload configs first
	go re.reloadConfigs()

	// Forget instances that haven't been seen in a long time
	re.forgetLongUnseenInstances()

	// TODO: Add more bookkeeping tasks in future PRs
	// - Expire old recovery history
	// - Clean up stale data
}

// forgetLongUnseenInstances removes pooler instances that haven't been successfully
// health checked in over 4 hours. This handles three cases:
// 1. Broken entries (nil pointers - should never happen)
// 2. Instances discovered in topology but never successfully health checked
// 3. Instances that were previously healthy but haven't been seen in 4+ hours
func (re *Engine) forgetLongUnseenInstances() {
	threshold := 4 * time.Hour
	now := time.Now()
	cutoff := now.Add(-threshold)

	storeSize := re.poolerStore.Len()

	// Warn if store gets too large - operator should consider splitting watchers
	const maxRecommendedPoolers = 1000
	if storeSize > maxRecommendedPoolers {
		re.logger.Warn("pooler store size exceeds recommended threshold",
			"current_size", storeSize,
			"threshold", maxRecommendedPoolers,
			"message", "consider splitting watch targets among multiple multiorch instances to distribute load",
		)
	}

	forgottenBroken := 0
	forgottenNeverSeen := 0
	forgottenLongGone := 0

	// Collect entries to delete (can't delete while iterating due to lock)
	type deleteEntry struct {
		poolerID   string
		auditType  string
		database   string
		tableGroup string
		shard      string
		message    string
	}
	var toDelete []deleteEntry

	// Iterate using Range() to hold lock during iteration
	re.poolerStore.Range(func(poolerID string, poolerInfo *store.PoolerHealth) bool {
		// Case 0: Broken entry (should never happen)
		if poolerInfo == nil || poolerInfo.ID == nil {
			toDelete = append(toDelete, deleteEntry{
				poolerID:  poolerID,
				auditType: "forget-broken-entry",
				message:   "removing broken pooler entry (nil pointers)",
			})
			forgottenBroken++
			return true // continue iteration
		}

		database := poolerInfo.Database
		tableGroup := poolerInfo.TableGroup
		shard := poolerInfo.Shard

		// Case 1: Never successfully health checked (LastSeen is zero)
		if poolerInfo.LastSeen.IsZero() {
			// Check how long since we first saw it (we don't have FirstDiscovered,
			// so we use LastCheckAttempted as a proxy, or skip if both are zero)
			if poolerInfo.LastCheckAttempted.IsZero() {
				// No attempts yet, skip for now
				return true // continue iteration
			}
			if poolerInfo.LastCheckAttempted.Before(cutoff) {
				toDelete = append(toDelete, deleteEntry{
					poolerID:   poolerID,
					auditType:  "forget-never-seen",
					database:   database,
					tableGroup: tableGroup,
					shard:      shard,
					message:    "removing pooler that was never successfully health checked after 4 hours",
				})
				forgottenNeverSeen++
			}
		} else if poolerInfo.LastSeen.Before(cutoff) {
			// Case 2: Was previously healthy but not seen in 4+ hours
			toDelete = append(toDelete, deleteEntry{
				poolerID:   poolerID,
				auditType:  "forget-long-unseen",
				database:   database,
				tableGroup: tableGroup,
				shard:      shard,
				message:    fmt.Sprintf("removing pooler not seen for %s", now.Sub(poolerInfo.LastSeen).Round(time.Second)),
			})
			forgottenLongGone++
		}
		return true // continue iteration
	})

	// Now delete the entries (outside the iteration)
	for _, entry := range toDelete {
		re.audit(entry.auditType, entry.poolerID, entry.database, entry.tableGroup, entry.shard, entry.message)
		re.poolerStore.Delete(entry.poolerID)
	}

	if forgottenBroken > 0 || forgottenNeverSeen > 0 || forgottenLongGone > 0 {
		re.logger.Info("forgot long unseen instances",
			"broken", forgottenBroken,
			"never_seen", forgottenNeverSeen,
			"long_gone", forgottenLongGone,
			"threshold", threshold,
		)
	}
}

// audit logs an audit message with consistent formatting.
// This ensures important operations are logged in a structured way for compliance and debugging.
func (re *Engine) audit(auditType, poolerID, database, tableGroup, shard, message string) {
	re.logger.Info("audit",
		"audit_type", auditType,
		"pooler_id", poolerID,
		"database", database,
		"tablegroup", tableGroup,
		"shard", shard,
		"message", message,
	)
}
