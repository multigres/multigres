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
	"time"

	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
)

// shutdownEtcdCleanupAge is how long a pooler's topology entry must have
// been observed in LIFECYCLE_SHUTDOWN before orch hard-deletes it from etcd.
// The pooler should have stopped publishing updates by the time it entered
// SHUTDOWN, so a stable ShutdownAt timestamp on the tombstone means the entry
// has lingered without anyone reclaiming it.
const shutdownEtcdCleanupAge = 24 * time.Hour

// runBookkeeping performs periodic bookkeeping tasks.
//
// In the cache-driven design, in-memory eviction is owned by the cache:
// SHUTDOWN triggers immediate removal (with a tombstone retained), and NoNode
// triggers missing-from-topo grace removal. Bookkeeping handles the one task the cache
// can't: hard-deleting topology entries for poolers that have been in
// SHUTDOWN for long enough that no operator is reclaiming them.
func (re *Engine) runBookkeeping() {
	re.logger.Debug("running bookkeeping tasks")

	go re.reloadConfigs()

	re.cleanupOldShutdownEntries()
}

// cleanupOldShutdownEntries iterates the cache's tombstone set (poolers observed
// in SHUTDOWN whose topology entries still exist) and hard-deletes from
// etcd any that have been tombstones longer than shutdownEtcdCleanupAge. The
// resulting NoNode event flows back through the cache's watch, which drops
// the tombstone from the set.
func (re *Engine) cleanupOldShutdownEntries() {
	cutoff := time.Now().Add(-shutdownEtcdCleanupAge)
	for _, g := range re.poolerCache.Tombstones() {
		if !g.ShutdownAt.Before(cutoff) {
			continue
		}
		if err := re.ts.UnregisterMultiPooler(re.shutdownCtx, g.ID); err != nil {
			re.logger.Warn("failed to hard-delete shutdown pooler from topology",
				"pooler_id", topoclient.ComponentIDString(g.ID),
				"shutdown_at", g.ShutdownAt,
				"error", err,
			)
			continue
		}
		re.audit("hard-delete-shutdown-tombstone",
			topoclient.ComponentIDString(g.ID),
			nil,
			"removed long-SHUTDOWN pooler entry from topology",
		)
	}
}

// audit logs an audit message with consistent formatting.
func (re *Engine) audit(auditType string, poolerID topoclient.ComponentID, shardKey *clustermetadatapb.ShardKey, message string) {
	re.logger.Info("audit",
		"audit_type", auditType,
		"pooler_id", poolerID,
		"shard_key", commontypes.FormatShardKey(shardKey),
		"message", message,
	)
}
