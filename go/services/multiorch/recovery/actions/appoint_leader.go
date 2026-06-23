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
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	commontypes "github.com/multigres/multigres/go/common/types"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// Compile-time assertion that AppointLeaderAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*AppointLeaderAction)(nil)

// AppointLeaderAction handles leader appointment using the coordinator's consensus protocol.
// This action is used for both repair (mixed initialized/empty nodes) and reelect
// (all nodes initialized) scenarios. The consensus.AppointLeader method handles
// both cases by selecting the most advanced node based on WAL position and running
// the full consensus protocol to establish a new primary.
type AppointLeaderAction struct {
	config      *config.Config
	consensus   *consensus.Coordinator
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerCache
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewAppointLeaderAction creates a new leader appointment action
func NewAppointLeaderAction(
	cfg *config.Config,
	consensus *consensus.Coordinator,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerCache,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *AppointLeaderAction {
	return &AppointLeaderAction{
		config:      cfg,
		consensus:   consensus,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute performs leader appointment by running the coordinator's consensus protocol
func (a *AppointLeaderAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing appoint leader action",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey))

	// Gather every pooler known for the shard, then recheck the problem.
	shard := store.FindShardMembers(a.poolerStore, problem.ShardKey)
	if len(shard.Poolers) == 0 {
		return fmt.Errorf("no poolers found for shard %s", commontypes.FormatShardKey(problem.ShardKey))
	}

	// Check if a healthy consensus leader already exists (problem resolved). The
	// leader is named by the highest known rule across all poolers — the global
	// consensus view, never a node's local self-claim — and is verified still
	// leading via a live status check. An error means no healthy leader exists, so
	// appointment proceeds.
	//
	// TODO: Reconsider if trying to contact a dead leader here makes sense. If it's unreachable,
	// this may just be wasting time.
	shortCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if leader, err := pollLeaderHealth(shortCtx, a.rpcClient, shard); err == nil {
		if types.LeaderNeedsReplacement(leader.Health()) {
			a.logger.InfoContext(ctx, "primary has requested replacement, proceeding with election",
				"primary", leader.Health().MultiPooler.Id.Name,
				"shard_key", commontypes.FormatShardKey(problem.ShardKey))
		} else {
			a.logger.InfoContext(ctx, "primary already exists, skipping leader appointment",
				"primary", leader.Health().MultiPooler.Id.Name,
				"shard_key", commontypes.FormatShardKey(problem.ShardKey))
			return nil
		}
	}

	a.logger.InfoContext(ctx, "verified shard still needs leader appointment, proceeding",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey),
		"pooler_count", len(shard.Poolers))

	// Use the coordinator's AppointLeader to handle the election.
	// Use the problem code as the reason for the election.
	reason := string(problem.Code)
	cohort := make([]*multiorchdatapb.PoolerHealthState, len(shard.Poolers))
	for i, p := range shard.Poolers {
		cohort[i] = p.Health()
	}
	if err := a.consensus.AppointLeader(ctx, problem.ShardKey, cohort, reason); err != nil {
		return mterrors.Wrap(err, "failed to appoint leader")
	}

	a.logger.InfoContext(ctx, "appoint leader action completed successfully",
		"shard_key", commontypes.FormatShardKey(problem.ShardKey))

	return nil
}

// RecoveryAction interface implementation

func (a *AppointLeaderAction) RequiresHealthyLeader() bool {
	return false // leader appointment doesn't need existing primary
}

func (a *AppointLeaderAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "AppointLeader",
		Description: "Elect a new primary for the shard using consensus",
		// Two sequential phases each bounded by RuleWriteTimeout
		// (Recruit, then concurrent Promote/SetPrimary), plus margin so the
		// action context does not race its own phases to the deadline.
		Timeout:     2*timeouts.RuleWriteTimeout + 5*time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true, // can retry if it fails
	}
}

func (a *AppointLeaderAction) Priority() types.Priority {
	return types.PriorityShardBootstrap
}

func (a *AppointLeaderAction) GracePeriod() *types.GracePeriodConfig {
	return &types.GracePeriodConfig{
		BaseDelay: a.config.GetLeaderFailoverGracePeriodBase(),
		MaxJitter: a.config.GetLeaderFailoverGracePeriodMaxJitter(),
	}
}
