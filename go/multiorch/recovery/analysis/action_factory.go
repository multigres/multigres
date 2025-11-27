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
	"log/slog"

	"github.com/multigres/multigres/go/clustermetadata/topo"
	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	"github.com/multigres/multigres/go/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/multiorch/store"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// RecoveryActionFactory creates recovery actions with all necessary dependencies.
type RecoveryActionFactory struct {
	poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState]
	rpcClient   rpcclient.MultiPoolerClient
	topoStore   topo.Store
	coordinator *coordinator.Coordinator
	logger      *slog.Logger
}

// NewRecoveryActionFactory creates a factory for recovery actions.
func NewRecoveryActionFactory(
	poolerStore *store.ProtoStore[string, *multiorchdatapb.PoolerHealthState],
	rpcClient rpcclient.MultiPoolerClient,
	topoStore topo.Store,
	coordinator *coordinator.Coordinator,
	logger *slog.Logger,
) *RecoveryActionFactory {
	return &RecoveryActionFactory{
		poolerStore: poolerStore,
		rpcClient:   rpcClient,
		topoStore:   topoStore,
		coordinator: coordinator,
		logger:      logger,
	}
}

// NewBootstrapRecoveryAction creates a bootstrap recovery action.
func (f *RecoveryActionFactory) NewBootstrapRecoveryAction() RecoveryAction {
	bootstrapAction := actions.NewBootstrapShardAction(f.rpcClient, f.topoStore, f.logger)
	return &BootstrapRecoveryAction{
		bootstrapAction: bootstrapAction,
		poolerStore:     f.poolerStore,
		topoStore:       f.topoStore,
		logger:          f.logger,
	}
}

// NewAppointLeaderRecoveryAction creates an appoint leader recovery action.
func (f *RecoveryActionFactory) NewAppointLeaderRecoveryAction() RecoveryAction {
	appointAction := actions.NewAppointLeaderAction(f.coordinator, f.logger)
	return &AppointLeaderRecoveryAction{
		appointAction: appointAction,
		poolerStore:   f.poolerStore,
		topoStore:     f.topoStore,
		logger:        f.logger,
	}
}
