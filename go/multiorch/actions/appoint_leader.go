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
	"log/slog"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/multiorch/coordinator"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// AppointLeaderAction handles leader appointment using the coordinator's consensus protocol.
// This action is used for both repair (mixed initialized/empty nodes) and reelect
// (all nodes initialized) scenarios. The coordinator.AppointLeader method handles
// both cases by selecting the most advanced node based on WAL position and running
// the full consensus protocol to establish a new primary.
type AppointLeaderAction struct {
	coordinator *coordinator.Coordinator
	logger      *slog.Logger
}

// NewAppointLeaderAction creates a new leader appointment action
func NewAppointLeaderAction(coordinator *coordinator.Coordinator, logger *slog.Logger) *AppointLeaderAction {
	return &AppointLeaderAction{
		coordinator: coordinator,
		logger:      logger,
	}
}

// Execute performs leader appointment by running the coordinator's consensus protocol
func (a *AppointLeaderAction) Execute(ctx context.Context, shardID string, database string, cohort []*multiorchdatapb.PoolerHealthState) error {
	a.logger.InfoContext(ctx, "Executing leader appointment",
		"shard", shardID,
		"database", database,
		"cohort_size", len(cohort))

	// Use the coordinator's AppointLeader to handle the election
	// It will select the most advanced node based on WAL position
	// and run the full consensus protocol (term discovery, candidate selection,
	// node recruitment, quorum validation, promotion, and replication setup)
	if err := a.coordinator.AppointLeader(ctx, shardID, cohort, database); err != nil {
		return mterrors.Wrap(err, "failed to appoint leader")
	}

	a.logger.InfoContext(ctx, "Leader appointment complete",
		"shard", shardID,
		"database", database)
	return nil
}
