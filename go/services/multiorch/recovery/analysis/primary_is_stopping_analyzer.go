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

// PrimaryIsStoppingAnalyzer detects when the highest-term primary has signalled
// PoolerType_STOPPING, indicating a graceful shutdown is in progress. It triggers
// ShutdownPrimaryAction to orchestrate the controlled failover (EmergencyDemote +
// AppointLeader) instead of letting PrimaryIsDeadAnalyzer run an uncontrolled election.
type PrimaryIsStoppingAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *PrimaryIsStoppingAnalyzer) Name() types.CheckName {
	return "PrimaryIsStopping"
}

func (a *PrimaryIsStoppingAnalyzer) ProblemCode() types.ProblemCode {
	return types.ProblemPrimaryIsStopping
}

func (a *PrimaryIsStoppingAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewShutdownPrimaryAction()
}

func (a *PrimaryIsStoppingAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	if !sa.PrimaryIsStopping {
		return nil, nil
	}

	if sa.HighestTermDiscoveredPrimaryID == nil {
		return nil, nil
	}

	// Only trigger if the pooler is reachable — we need to be able to call
	// EmergencyDemote on it. If unreachable, fall through to PrimaryIsDeadAnalyzer.
	if !sa.PrimaryPoolerReachable {
		return nil, nil
	}

	return []types.Problem{{
		Code:           types.ProblemPrimaryIsStopping,
		CheckName:      "PrimaryIsStopping",
		PoolerID:       sa.HighestTermDiscoveredPrimaryID,
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Primary for shard %s is gracefully stopping", sa.ShardKey),
		Priority:       types.PriorityEmergency,
		Scope:          types.ScopeShard,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewShutdownPrimaryAction(),
	}}, nil
}
