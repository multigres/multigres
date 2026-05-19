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

package analysis

import (
	"errors"
	"fmt"
	"time"

	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
)

// LeaderResignedAnalyzer detects when the topology leader has explicitly
// requested its own replacement via LEADERSHIP_SIGNAL_REQUESTING_DEMOTION
// (emitted by EmergencyDemote and graceful shutdown of a leader). Distinct
// from LeaderIsDeadAnalyzer, which infers leader loss from reachability and
// health. Resignation is an unambiguous, intentional signal so failover can
// fire immediately with no follower-connection grace period.
type LeaderResignedAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *LeaderResignedAnalyzer) Name() types.CheckName {
	return "LeaderResigned"
}

func (a *LeaderResignedAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemLeaderResigned
}

func (a *LeaderResignedAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewAppointLeaderAction()
}

func (a *LeaderResignedAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}
	if sa.HighestTermDiscoveredLeaderID == nil {
		return nil, nil
	}
	if !sa.LeaderHasResigned {
		return nil, nil
	}
	return []types.Problem{{
		Code:           types.ProblemLeaderResigned,
		CheckName:      a.Name(),
		PoolerID:       sa.HighestTermDiscoveredLeaderID,
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Leader for shard %s has requested demotion", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewAppointLeaderAction(),
	}}, nil
}
