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

	"github.com/multigres/multigres/go/common/rpcclient"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/config"
	"github.com/multigres/multigres/go/services/multiorch/coordinator"
	"github.com/multigres/multigres/go/services/multiorch/recovery/actions"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// RecoveryActionFactory creates recovery actions with all necessary dependencies.
type RecoveryActionFactory struct {
	config      *config.Config
	poolerStore *store.PoolerHealthStore
	rpcClient   rpcclient.MultiPoolerClient
	topoStore   topoclient.Store
	coordinator *coordinator.Coordinator
	logger      *slog.Logger
}

// NewRecoveryActionFactory creates a factory for recovery actions.
func NewRecoveryActionFactory(
	cfg *config.Config,
	poolerStore *store.PoolerHealthStore,
	rpcClient rpcclient.MultiPoolerClient,
	topoStore topoclient.Store,
	coordinator *coordinator.Coordinator,
	logger *slog.Logger,
) *RecoveryActionFactory {
	if coordinator == nil {
		panic("coordinator cannot be nil")
	}
	return &RecoveryActionFactory{
		config:      cfg,
		poolerStore: poolerStore,
		rpcClient:   rpcClient,
		topoStore:   topoStore,
		coordinator: coordinator,
		logger:      logger,
	}
}

// NewBootstrapShardAction creates a bootstrap shard action.
func (f *RecoveryActionFactory) NewBootstrapShardAction() types.RecoveryAction {
	return actions.NewBootstrapShardAction(f.config, f.rpcClient, f.poolerStore, f.topoStore, f.coordinator, f.logger)
}

// NewAppointLeaderAction creates an appoint leader action.
func (f *RecoveryActionFactory) NewAppointLeaderAction() types.RecoveryAction {
	return actions.NewAppointLeaderAction(f.config, f.coordinator, f.poolerStore, f.topoStore, f.logger)
}

// NewFixReplicationAction creates a fix replication action.
func (f *RecoveryActionFactory) NewFixReplicationAction() types.RecoveryAction {
	poolerStore := store.NewPoolerStore(f.poolerStore, f.rpcClient, f.logger)
	return actions.NewFixReplicationAction(f.config, f.rpcClient, poolerStore, f.topoStore, f.logger)
}

// NewDemoteStalePrimaryAction creates an action to demote a stale primary.
func (f *RecoveryActionFactory) NewDemoteStalePrimaryAction() types.RecoveryAction {
	return actions.NewDemoteStalePrimaryAction(f.config, f.rpcClient, f.poolerStore, f.topoStore, f.logger)
}

// Logger returns the factory's logger for use by analyzers.
func (f *RecoveryActionFactory) Logger() *slog.Logger {
	return f.logger
}
