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

package recovery

import (
	"log/slog"

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/consensus"
	"github.com/multigres/multigres/go/services/multiorch/internal/snapshot"
	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/services/multiorch/recovery/analysis"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// actionFactory creates recovery actions with all necessary dependencies.
type actionFactory struct {
	config      *config.Config
	poolerStore *store.PoolerCache
	rpcClient   rpcclient.MultipoolerClient
	topoStore   topoclient.Store
	coordinator *consensus.Coordinator
	logger      *slog.Logger
}

// newActionFactory creates a factory for recovery actions.
func newActionFactory(
	cfg *config.Config,
	poolerStore *store.PoolerCache,
	rpcClient rpcclient.MultipoolerClient,
	topoStore topoclient.Store,
	coordinator *consensus.Coordinator,
	logger *slog.Logger,
) *actionFactory {
	if coordinator == nil {
		panic("coordinator cannot be nil")
	}
	return &actionFactory{
		config:      cfg,
		poolerStore: poolerStore,
		rpcClient:   rpcClient,
		topoStore:   topoStore,
		coordinator: coordinator,
		logger:      logger,
	}
}

// NewShardInitAction creates a shard initialization action.
func (f *actionFactory) NewShardInitAction() types.RecoveryAction {
	return actions.NewShardInitAction(f.config, f.coordinator, f.poolerStore, f.topoStore, f.logger)
}

// NewAppointLeaderAction creates an appoint leader action.
func (f *actionFactory) NewAppointLeaderAction() types.RecoveryAction {
	return actions.NewAppointLeaderAction(f.config, f.coordinator, f.poolerStore, f.logger, f.AvailabilityPolicy().PreRecheckRefreshTimeout)
}

// NewFixReplicationAction creates a fix replication action.
func (f *actionFactory) NewFixReplicationAction() types.RecoveryAction {
	return actions.NewFixReplicationAction(f.config, f.rpcClient, f.poolerStore, f.logger)
}

// NewDemoteStaleLeaderAction creates an action to demote a stale primary.
func (f *actionFactory) NewDemoteStaleLeaderAction() types.RecoveryAction {
	return actions.NewDemoteStaleLeaderAction(f.rpcClient, f.poolerStore, f.logger)
}

// NewReconcileCohortAction creates an action to add or remove a cohort member.
func (f *actionFactory) NewReconcileCohortAction() types.RecoveryAction {
	return actions.NewReconcileCohortAction(f.rpcClient, f.poolerStore, f.logger)
}

// NewAlertOnlyAction creates a no-op action for non-actionable problems (e.g.
// ShardStuck, ShardAtRisk) — detection is the point; there is nothing to fix.
func (f *actionFactory) NewAlertOnlyAction() types.RecoveryAction {
	return actions.NewAlertOnlyAction(f.logger)
}

// NewReconnectRecruitAbandonedAction creates an action to reconnect a follower
// stranded by an abandoned recruit via a leader-led no-op rule advance.
func (f *actionFactory) NewReconnectRecruitAbandonedAction() types.RecoveryAction {
	return actions.NewReconnectRecruitAbandonedAction(f.rpcClient, f.poolerStore, f.logger)
}

// AvailabilityPolicy returns the policy built from the factory's current
// configuration. The engine freezes this same policy into every snapshot, so the
// analyzers and the action timings read one source.
func (f *actionFactory) AvailabilityPolicy() snapshot.AvailabilityPolicy {
	return analysis.NewAvailabilityPolicy(f.config)
}
