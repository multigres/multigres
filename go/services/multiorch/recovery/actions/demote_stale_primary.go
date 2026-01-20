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

package actions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that DemoteStalePrimaryAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*DemoteStalePrimaryAction)(nil)

// StalePrimaryDrainTimeout is a shorter drain timeout for stale primaries.
// Stale primaries that just came back online typically have no active connections,
// so we use a shorter timeout to speed up demotion.
const StalePrimaryDrainTimeout = 5 * time.Second

// DemoteStalePrimaryAction demotes a stale primary that was detected after failover.
// It uses the Demote RPC with the correct primary's term to force the stale primary
// to accept the term and demote, preventing further writes.
type DemoteStalePrimaryAction struct {
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerHealthStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewDemoteStalePrimaryAction creates a new action to demote a stale primary.
func NewDemoteStalePrimaryAction(
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerHealthStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *DemoteStalePrimaryAction {
	return &DemoteStalePrimaryAction{
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

func (a *DemoteStalePrimaryAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "DemoteStalePrimary",
		Description: "Demote a stale primary that came back online after failover",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *DemoteStalePrimaryAction) Priority() types.Priority {
	return types.PriorityHigh
}

func (a *DemoteStalePrimaryAction) RequiresHealthyPrimary() bool {
	// We're demoting a primary, so we can't require a healthy primary
	return false
}

// Execute demotes the stale primary using the Demote RPC with the correct primary's term.
// This is safer than BeginTerm because:
// 1. We use the correct primary's term (not a new term), avoiding term inconsistency
// 2. The stale primary accepts term >= its current term and demotes
// 3. Both primaries end up with the same term (no term inconsistency)
func (a *DemoteStalePrimaryAction) Execute(ctx context.Context, problem types.Problem) error {
	poolerIDStr := topoclient.MultiPoolerIDString(problem.PoolerID)

	a.logger.InfoContext(ctx, "executing demote stale primary action",
		"shard_key", problem.ShardKey.String(),
		"stale_primary", poolerIDStr)

	// Get the stale primary from the store
	stalePrimary, ok := a.poolerStore.Get(poolerIDStr)
	if !ok {
		return fmt.Errorf("stale primary %s not found in store", poolerIDStr)
	}

	// Check if postgres is running on the stale primary before attempting demote.
	// Demote requires postgres to be healthy. If postgres is not running yet,
	// we should skip this attempt and let the next recovery cycle retry once
	// postgres is ready. This avoids wasting time on RPCs that will fail.
	// if !stalePrimary.IsPostgresRunning {
	// 	return mterrors.New(mtrpcpb.Code_UNAVAILABLE,
	// 		fmt.Sprintf("postgres not running on stale primary %s, skipping demote attempt", poolerIDStr))
	// }

	// Find the correct primary to use as rewind source
	correctPrimary, correctPrimaryTerm, err := a.findCorrectPrimary(problem.ShardKey, poolerIDStr)
	if err != nil {
		return mterrors.Wrap(err, "failed to find correct primary")
	}

	a.logger.InfoContext(ctx, "demoting stale primary using DemoteStalePrimary RPC",
		"stale_primary", poolerIDStr,
		"correct_primary", correctPrimary.MultiPooler.Id.Name,
		"correct_primary_term", correctPrimaryTerm)

	// Call DemoteStalePrimary RPC - this will:
	// 1. Stop postgres
	// 2. Run pg_rewind to sync with correct primary
	// 3. Restart as standby
	// 4. Clear sync replication config
	// 5. Update topology to REPLICA
	demoteResp, err := a.rpcClient.DemoteStalePrimary(ctx, stalePrimary.MultiPooler, &multipoolermanagerdatapb.DemoteStalePrimaryRequest{
		Source:        correctPrimary.MultiPooler,
		ConsensusTerm: correctPrimaryTerm,
		Force:         false,
	})
	if err != nil {
		return mterrors.Wrap(err, "DemoteStalePrimary RPC failed")
	}

	a.logger.InfoContext(ctx, "stale primary demoted successfully",
		"stale_primary", poolerIDStr,
		"rewind_performed", demoteResp.RewindPerformed,
		"lsn_position", demoteResp.LsnPosition)

	a.logger.InfoContext(ctx, "demote stale primary action completed",
		"shard_key", problem.ShardKey.String(),
		"demoted_primary", poolerIDStr)

	return nil
}

// findCorrectPrimary finds the correct primary in the shard and returns it along with its term.
// The correct primary is the one with the higher consensus term.
func (a *DemoteStalePrimaryAction) findCorrectPrimary(shardKey commontypes.ShardKey, stalePrimaryIDStr string) (*multiorchdatapb.PoolerHealthState, int64, error) {
	var correctPrimary *multiorchdatapb.PoolerHealthState
	var maxTerm int64

	// Iterate through all poolers to find the correct primary
	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		// Only consider poolers in the same shard
		if pooler.MultiPooler.Database != shardKey.Database ||
			pooler.MultiPooler.TableGroup != shardKey.TableGroup ||
			pooler.MultiPooler.Shard != shardKey.Shard {
			return true // continue
		}

		poolerIDStr := topoclient.MultiPoolerIDString(pooler.MultiPooler.Id)

		// Skip the stale primary
		if poolerIDStr == stalePrimaryIDStr {
			return true // continue
		}

		// Check if this pooler is a PRIMARY
		poolerType := pooler.PoolerType
		if poolerType == clustermetadatapb.PoolerType_UNKNOWN && pooler.MultiPooler != nil {
			poolerType = pooler.MultiPooler.Type
		}

		if poolerType == clustermetadatapb.PoolerType_PRIMARY {
			// Get its term
			if pooler.ConsensusStatus != nil && pooler.ConsensusStatus.CurrentTerm > maxTerm {
				maxTerm = pooler.ConsensusStatus.CurrentTerm
				correctPrimary = pooler
			}
		}

		return true // continue
	})

	if correctPrimary == nil {
		return nil, 0, fmt.Errorf("no correct primary found in shard %s", shardKey.String())
	}

	return correctPrimary, maxTerm, nil
}
