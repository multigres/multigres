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

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
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
	return types.PriorityEmergency
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
	if !stalePrimary.IsPostgresRunning {
		return mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			fmt.Sprintf("postgres not running on stale primary %s, skipping demote attempt", poolerIDStr))
	}

	// Find the correct primary (the one with higher term) to get its term
	correctPrimaryTerm, err := a.findCorrectPrimaryTerm(problem.ShardKey, poolerIDStr)
	if err != nil {
		return mterrors.Wrap(err, "failed to find correct primary term")
	}

	a.logger.InfoContext(ctx, "demoting stale primary with correct primary's term",
		"stale_primary", poolerIDStr,
		"correct_primary_term", correctPrimaryTerm)

	// Call Demote RPC with the correct primary's term
	// The Demote RPC accepts term >= currentTerm and performs the demotion
	// Use a shorter drain timeout for stale primaries since they typically have no active connections
	demoteResp, err := a.rpcClient.Demote(ctx, stalePrimary.MultiPooler, &multipoolermanagerdatapb.DemoteRequest{
		ConsensusTerm: correctPrimaryTerm,
		DrainTimeout:  durationpb.New(StalePrimaryDrainTimeout),
		Force:         false, // Respect term validation
	})
	if err != nil {
		return mterrors.Wrap(err, "Demote RPC failed")
	}

	a.logger.InfoContext(ctx, "stale primary demoted successfully",
		"stale_primary", poolerIDStr,
		"lsn_position", demoteResp.LsnPosition)

	// Verify the demotion by checking status
	statusResp, err := a.rpcClient.Status(ctx, stalePrimary.MultiPooler, &multipoolermanagerdatapb.StatusRequest{})
	if err != nil {
		a.logger.WarnContext(ctx, "failed to verify demotion status",
			"stale_primary", poolerIDStr,
			"error", err)
	} else if statusResp.Status != nil && statusResp.Status.PostgresRole == "standby" {
		a.logger.InfoContext(ctx, "verified: stale primary is now in recovery mode (standby)",
			"stale_primary", poolerIDStr)
	}

	a.logger.InfoContext(ctx, "demote stale primary action completed",
		"shard_key", problem.ShardKey.String(),
		"demoted_primary", poolerIDStr)

	return nil
}

// findCorrectPrimaryTerm finds the correct primary in the shard and returns its term.
// The correct primary is the one with the higher consensus term.
func (a *DemoteStalePrimaryAction) findCorrectPrimaryTerm(shardKey commontypes.ShardKey, stalePrimaryIDStr string) (int64, error) {
	var maxTerm int64
	var foundCorrectPrimary bool

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
				foundCorrectPrimary = true
			}
		}

		return true // continue
	})

	if !foundCorrectPrimary {
		return 0, fmt.Errorf("no correct primary found in shard %s", shardKey.String())
	}

	return maxTerm, nil
}
