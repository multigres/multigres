// Copyright 2025 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analysis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/multiorch/store"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// BootstrapRecoveryAction wraps actions.BootstrapShardAction to implement RecoveryAction interface.
type BootstrapRecoveryAction struct {
	bootstrapAction *actions.BootstrapShardAction
	poolerStore     *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState]
	topoStore       topo.Store
	logger          *slog.Logger
}

func (a *BootstrapRecoveryAction) Execute(ctx context.Context, problem Problem) error {
	a.logger.InfoContext(ctx, "executing bootstrap recovery action",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)

	// Step 1: Acquire distributed lock for this shard
	lockPath := fmt.Sprintf("recovery/%s/%s/%s", problem.Database, problem.TableGroup, problem.Shard)
	lockContents := fmt.Sprintf("bootstrap recovery for shard %s/%s/%s", problem.Database, problem.TableGroup, problem.Shard)

	a.logger.InfoContext(ctx, "acquiring recovery lock", "lock_path", lockPath)
	conn, err := a.topoStore.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return fmt.Errorf("failed to get topo connection: %w", err)
	}
	lockDesc, err := conn.LockName(ctx, lockPath, lockContents)
	if err != nil {
		// Another recovery is in progress
		a.logger.InfoContext(ctx, "failed to acquire lock, another recovery in progress",
			"lock_path", lockPath,
			"error", err)
		return fmt.Errorf("failed to acquire recovery lock: %w", err)
	}
	defer func() {
		if unlockErr := lockDesc.Unlock(ctx); unlockErr != nil {
			a.logger.WarnContext(ctx, "failed to release recovery lock",
				"lock_path", lockPath,
				"error", unlockErr)
		}
	}()

	a.logger.InfoContext(ctx, "acquired recovery lock", "lock_path", lockPath)

	// Step 2: Recheck the problem after acquiring lock
	// Fetch fresh cohort data to verify problem still exists
	cohort, err := a.getCohort(problem.Database, problem.TableGroup, problem.Shard)
	if err != nil {
		return fmt.Errorf("failed to get cohort: %w", err)
	}

	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s/%s/%s",
			problem.Database, problem.TableGroup, problem.Shard)
	}

	// Verify all nodes are still uninitialized
	allUninitialized := true
	for _, pooler := range cohort {
		if store.IsInitialized(pooler) {
			allUninitialized = false
			a.logger.InfoContext(ctx, "node is now initialized, skipping bootstrap",
				"node", pooler.MultiPooler.Id.Name,
				"database", problem.Database,
				"shard", problem.Shard)
			break
		}
	}

	if !allUninitialized {
		a.logger.InfoContext(ctx, "shard no longer needs bootstrap, problem resolved by another recovery",
			"database", problem.Database,
			"tablegroup", problem.TableGroup,
			"shard", problem.Shard)
		return nil
	}

	a.logger.InfoContext(ctx, "verified shard still needs bootstrap, proceeding",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard,
		"cohort_size", len(cohort))

	// Step 3: Execute bootstrap action
	if err := a.bootstrapAction.Execute(ctx, problem.Shard, problem.Database, cohort); err != nil {
		return fmt.Errorf("bootstrap action failed: %w", err)
	}

	a.logger.InfoContext(ctx, "bootstrap recovery completed successfully",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)

	return nil
}

// getCohort fetches all poolers in the shard from the pooler store.
func (a *BootstrapRecoveryAction) getCohort(database, tablegroup, shard string) ([]*multiorchdatapb.PoolerHealthState, error) {
	var cohort []*multiorchdatapb.PoolerHealthState

	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Database == database &&
			pooler.MultiPooler.TableGroup == tablegroup &&
			pooler.MultiPooler.Shard == shard {
			cohort = append(cohort, pooler)
		}

		return true // continue
	})

	return cohort, nil
}

func (a *BootstrapRecoveryAction) RequiresHealthyPrimary() bool {
	return false
}

func (a *BootstrapRecoveryAction) RequiresLock() bool {
	return true
}

func (a *BootstrapRecoveryAction) Metadata() RecoveryMetadata {
	return RecoveryMetadata{
		Name:        "BootstrapShard",
		Description: "Initialize empty shard with primary and standbys",
		Timeout:     60 * time.Second,
		Retryable:   false,
	}
}

func (a *BootstrapRecoveryAction) Priority() Priority {
	return PriorityShardBootstrap
}
