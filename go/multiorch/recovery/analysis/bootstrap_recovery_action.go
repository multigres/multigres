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

	"github.com/multigres/multigres/go/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/multiorch/store"
)

// BootstrapRecoveryAction wraps actions.BootstrapShardAction to implement RecoveryAction interface.
type BootstrapRecoveryAction struct {
	bootstrapAction *actions.BootstrapShardAction
	poolerStore     *store.Store[string, *store.PoolerHealth]
	logger          *slog.Logger
}

func (a *BootstrapRecoveryAction) Execute(ctx context.Context, problem Problem) error {
	a.logger.InfoContext(ctx, "executing bootstrap recovery action",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard)

	// Fetch cohort from pooler store
	cohort, err := a.getCohort(problem.Database, problem.TableGroup, problem.Shard)
	if err != nil {
		return fmt.Errorf("failed to get cohort: %w", err)
	}

	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s/%s/%s",
			problem.Database, problem.TableGroup, problem.Shard)
	}

	a.logger.InfoContext(ctx, "fetched cohort for bootstrap",
		"database", problem.Database,
		"tablegroup", problem.TableGroup,
		"shard", problem.Shard,
		"cohort_size", len(cohort))

	// Call the underlying bootstrap action
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
func (a *BootstrapRecoveryAction) getCohort(database, tablegroup, shard string) ([]*store.PoolerHealth, error) {
	var cohort []*store.PoolerHealth

	a.poolerStore.Range(func(key string, pooler *store.PoolerHealth) bool {
		if pooler == nil || pooler.ID == nil {
			return true // continue
		}

		if pooler.Database == database &&
			pooler.TableGroup == tablegroup &&
			pooler.Shard == shard {
			cohort = append(cohort, pooler.DeepCopy())
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
