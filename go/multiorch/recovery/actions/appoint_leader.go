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
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	"github.com/multigres/multigres/go/multiorch/recovery/types"
	"github.com/multigres/multigres/go/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Compile-time assertion that AppointLeaderAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*AppointLeaderAction)(nil)

// AppointLeaderAction handles leader appointment using the coordinator's consensus protocol.
// This action is used for both repair (mixed initialized/empty nodes) and reelect
// (all nodes initialized) scenarios. The coordinator.AppointLeader method handles
// both cases by selecting the most advanced node based on WAL position and running
// the full consensus protocol to establish a new primary.
type AppointLeaderAction struct {
	coordinator *coordinator.Coordinator
	poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState]
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewAppointLeaderAction creates a new leader appointment action
func NewAppointLeaderAction(
	coordinator *coordinator.Coordinator,
	poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState],
	topoStore topoclient.Store,
	logger *slog.Logger,
) *AppointLeaderAction {
	return &AppointLeaderAction{
		coordinator: coordinator,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs leader appointment by running the coordinator's consensus protocol
func (a *AppointLeaderAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing appoint leader action",
		"shard_key", problem.ShardKey.String())

	// Fetch cohort and recheck the problem after acquiring lock
	cohort := a.getCohort(problem.ShardKey)
	if len(cohort) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	// Check if a primary already exists (problem resolved)
	for _, pooler := range cohort {
		if pooler.MultiPooler != nil &&
			pooler.MultiPooler.Type == clustermetadatapb.PoolerType_PRIMARY &&
			pooler.IsLastCheckValid {
			a.logger.InfoContext(ctx, "primary already exists, skipping leader appointment",
				"primary", pooler.MultiPooler.Id.Name,
				"shard_key", problem.ShardKey.String())
			return nil
		}
	}

	a.logger.InfoContext(ctx, "verified shard still needs leader appointment, proceeding",
		"shard_key", problem.ShardKey.String(),
		"cohort_size", len(cohort))

	// Use the coordinator's AppointLeader to handle the election
	// It will select the most advanced node based on WAL position
	// and run the full consensus protocol (term discovery, candidate selection,
	// node recruitment, quorum validation, promotion, and replication setup)
	if err := a.coordinator.AppointLeader(ctx, problem.ShardKey.Shard, cohort, problem.ShardKey.Database); err != nil {
		return mterrors.Wrap(err, "failed to appoint leader")
	}

	a.logger.InfoContext(ctx, "appoint leader action completed successfully",
		"shard_key", problem.ShardKey.String())

	return nil
}

// getCohort fetches all poolers in the shard from the pooler store.
func (a *AppointLeaderAction) getCohort(shardKey commontypes.ShardKey) []*multiorchdatapb.PoolerHealthState {
	var cohort []*multiorchdatapb.PoolerHealthState

	a.poolerStore.Range(func(key string, pooler *multiorchdatapb.PoolerHealthState) bool {
		if pooler == nil || pooler.MultiPooler == nil || pooler.MultiPooler.Id == nil {
			return true // continue
		}

		if pooler.MultiPooler.Database == shardKey.Database &&
			pooler.MultiPooler.TableGroup == shardKey.TableGroup &&
			pooler.MultiPooler.Shard == shardKey.Shard {
			cohort = append(cohort, pooler)
		}

		return true // continue
	})

	return cohort
}

// RecoveryAction interface implementation

func (a *AppointLeaderAction) RequiresHealthyPrimary() bool {
	return false // leader appointment doesn't need existing primary
}

func (a *AppointLeaderAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "AppointLeader",
		Description: "Elect a new primary for the shard using consensus",
		Timeout:     60 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true, // can retry if it fails
	}
}

func (a *AppointLeaderAction) Priority() types.Priority {
	return types.PriorityShardBootstrap
}
