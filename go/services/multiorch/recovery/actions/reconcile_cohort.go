// Copyright 2026 Supabase, Inc.
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
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// Compile-time assertion that ReconcileCohortAction implements types.RecoveryAction.
var _ types.RecoveryAction = (*ReconcileCohortAction)(nil)

// ReconcileCohortAction applies a single cohort-membership change on the
// shard's leader.
//
// It handles two problem codes:
//   - ProblemPoolerNotInCohort: add the pooler via UpdateConsensusRule(ADD).
//   - ProblemCohortMemberIneligible: remove the pooler via UpdateConsensusRule(REMOVE).
//
// The action mutates exactly one cohort member per execution; multiple
// drifting members produce multiple problems and run separately.
//
// TODO: future work will likely cap the cohort size based on the durability
// policy and require a fitness heuristic to choose the best-qualified
// candidates among many eligible poolers. Today the action adds every
// eligible non-cohort pooler unconditionally.
type ReconcileCohortAction struct {
	config      *config.Config
	rpcClient   rpcclient.MultiPoolerClient
	poolerStore *store.PoolerStore
	topoStore   topoclient.Store
	logger      *slog.Logger
}

// NewReconcileCohortAction creates a new cohort reconciliation action.
func NewReconcileCohortAction(
	cfg *config.Config,
	rpcClient rpcclient.MultiPoolerClient,
	poolerStore *store.PoolerStore,
	topoStore topoclient.Store,
	logger *slog.Logger,
) *ReconcileCohortAction {
	return &ReconcileCohortAction{
		config:      cfg,
		rpcClient:   rpcClient,
		poolerStore: poolerStore,
		topoStore:   topoStore,
		logger:      logger,
	}
}

// Execute applies the cohort change on the shard leader.
func (a *ReconcileCohortAction) Execute(ctx context.Context, problem types.Problem) error {
	a.logger.InfoContext(ctx, "executing reconcile cohort action",
		"shard_key", problem.ShardKey.String(),
		"pooler", problem.PoolerID.Name,
		"problem_code", string(problem.Code))

	target, err := a.poolerStore.FindPoolerByID(problem.PoolerID)
	if err != nil {
		return mterrors.Wrap(err, "failed to find target pooler")
	}

	poolers := a.poolerStore.FindPoolersInShard(problem.ShardKey)
	if len(poolers) == 0 {
		return fmt.Errorf("no poolers found for shard %s", problem.ShardKey)
	}

	primary, err := a.poolerStore.FindHealthyPrimary(ctx, poolers)
	if err != nil {
		return mterrors.Wrap(err, "failed to find primary")
	}

	var op multipoolermanagerdatapb.CohortUpdateOperation
	switch problem.Code {
	case types.ProblemPoolerNotInCohort:
		op = multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_ADD
	case types.ProblemCohortMemberIneligible:
		op = multipoolermanagerdatapb.CohortUpdateOperation_COHORT_UPDATE_OPERATION_REMOVE
	default:
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"unsupported problem code for reconcile cohort: %s", problem.Code)
	}

	// Capture the leader's current rule for CAS. If our view is stale by the
	// time the multipooler handles the request, the CAS check fails and we
	// retry on the next analyzer cycle with a fresh view — preventing two
	// coordinators from racing on overlapping cohort changes.
	expectedRule := primary.GetConsensusStatus().GetCurrentPosition().GetRule().GetRuleNumber()
	if expectedRule == nil {
		return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"primary %s has no recorded rule; cannot reconcile cohort", primary.MultiPooler.Id.Name)
	}

	req := &multipoolermanagerdatapb.UpdateConsensusRuleRequest{
		Operation:            op,
		StandbyIds:           []*clustermetadatapb.ID{target.MultiPooler.Id},
		ReloadConfig:         true,
		ExpectedOutgoingRule: expectedRule,
		Force:                false,
	}

	if _, err := a.rpcClient.UpdateConsensusRule(ctx, primary.MultiPooler, req); err != nil {
		return mterrors.Wrap(err, "UpdateConsensusRule failed")
	}

	a.logger.InfoContext(ctx, "reconcile cohort action completed",
		"target", target.MultiPooler.Id.Name,
		"primary", primary.MultiPooler.Id.Name,
		"operation", op.String())
	return nil
}

// RecoveryAction interface implementation

func (a *ReconcileCohortAction) RequiresHealthyLeader() bool {
	return true // UpdateConsensusRule must run on a healthy primary.
}

func (a *ReconcileCohortAction) Metadata() types.RecoveryMetadata {
	return types.RecoveryMetadata{
		Name:        "ReconcileCohort",
		Description: "Add or remove a single cohort member on the shard leader",
		Timeout:     30 * time.Second,
		LockTimeout: 15 * time.Second,
		Retryable:   true,
	}
}

func (a *ReconcileCohortAction) Priority() types.Priority {
	// Cohort drift is not service-impacting until durability is at risk;
	// run after replication repair (PriorityHigh) so a new pooler is fully
	// streaming before we propose adding it.
	return types.PriorityNormal
}

func (a *ReconcileCohortAction) GracePeriod() *types.GracePeriodConfig {
	return nil
}
