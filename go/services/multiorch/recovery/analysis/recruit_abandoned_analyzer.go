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

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/services/multiorch/store"
)

// RecruitAbandonedAnalyzer detects a follower stranded by an abandoned recruit:
// its TermRevocation revokes the shard's committed rule, so it rejects the
// leader's SetPrimary and cannot rejoin. The remedy is a leader-led no-op rule
// advance (see ReconnectRecruitAbandonedAction), not FixReplication — a
// SetPrimary at the current rule would just be ignored.
type RecruitAbandonedAnalyzer struct {
	factory *RecoveryActionFactory
}

func (a *RecruitAbandonedAnalyzer) Name() types.CheckName {
	return "ReplicaRecruitAbandoned"
}

func (a *RecruitAbandonedAnalyzer) RecoveryAction() types.RecoveryAction {
	return a.factory.NewReconnectRecruitAbandonedAction()
}

func (a *RecruitAbandonedAnalyzer) Analyze(sa *ShardAnalysis) ([]types.Problem, error) {
	return analyzeAllPoolers(sa, a.analyzePooler)
}

// revocationStrandsFollower reports whether the rule orch would relay via
// SetPrimary is rejected by the follower's recorded TermRevocation — the
// signature of an abandoned recruit. It mirrors IsRuleRevoked against
// HighestPosition exactly as SetPrimary's own gate does (rather than the
// stricter ValidateRevocation) so detection can't drift from enforcement.
// Also consulted by ReplicaNotReplicatingAnalyzer to defer to this analyzer
// instead of sending a SetPrimary the follower would just ignore.
func revocationStrandsFollower(sa *ShardAnalysis, p *store.Pooler) bool {
	if sa.HighestPosition == nil {
		return false
	}
	revocation := p.Health().GetConsensusStatus().GetTermRevocation()
	return commonconsensus.IsRuleRevoked(sa.HighestPosition, revocation)
}

// revocationAgedPastFailoverGrace reports whether the follower's revocation was
// issued longer ago than the failover grace window (base + max jitter), the
// bound within which a live failover completes. Only then do we treat the
// recruit as abandoned. A revocation with no coordinator_initiated_at (which a
// valid one always carries) is treated as not-yet-aged, erring toward waiting.
func revocationAgedPastFailoverGrace(factory *RecoveryActionFactory, sa *ShardAnalysis, p *store.Pooler) bool {
	initiatedAt := p.Health().GetConsensusStatus().GetTermRevocation().GetCoordinatorInitiatedAt()
	if initiatedAt == nil {
		return false
	}
	var grace time.Duration
	if cfg := factory.Config(); cfg != nil {
		grace = cfg.GetLeaderFailoverGracePeriodBase() + cfg.GetLeaderFailoverGracePeriodMaxJitter()
	}
	return sa.Now.Sub(initiatedAt.AsTime()) >= grace
}

func (a *RecruitAbandonedAnalyzer) analyzePooler(sa *ShardAnalysis, pa *store.Pooler) (*types.Problem, error) {
	if a.factory == nil {
		return nil, errors.New("recovery action factory not initialized")
	}

	// Only followers can be stranded; the leader defines the rule.
	if commonconsensus.SelfConsensusRole(pa.Health().GetConsensusStatus()) == commonconsensus.ConsensusRoleLeader {
		return nil, nil
	}
	hs, ok := pa.HealthWithin(sa.Now, sa.Policy.ObservationFreshness)
	if !ok || !hs.GetStatus().GetIsInitialized() {
		return nil, nil
	}

	// We need a known leader with an address to dial: the fix drives an
	// UpdateConsensusRule on the leader and then a SetPrimary toward the follower.
	if sa.Leader == nil || sa.Leader.Health().GetMultipooler().GetHostname() == "" {
		return nil, nil
	}

	// Only proceed if it looks safe to attempt a write on the leader right now —
	// otherwise we can't distinguish an abandoned recruit from a live election
	// still in flight (see store.LeaderWritesProgressing).
	if !store.LeaderWritesProgressing(sa.Leader, sa.HighestPosition, sa.Now, sa.Policy.LeaderChangeFreshness) {
		return nil, nil
	}

	// Only a follower that is actually recruited can be stranded: its accepted
	// revocation must still revoke its own committed position (self-revoked). The
	// revocation is a monotonic promise floor that ConsensusStatus keeps reporting
	// even after the follower's rule catches up, so its mere presence is not
	// enough.
	if !commonconsensus.IsSelfRevoked(pa.Health().GetConsensusStatus()) {
		return nil, nil
	}

	// The stranding signature: the follower's revocation revokes the committed rule.
	if !revocationStrandsFollower(sa, pa) {
		return nil, nil
	}

	// Give an in-flight recruit time to finish before concluding it was abandoned —
	// another orchestrator may still be completing a legitimate failover at this
	// term, and advancing the rule out from under it would fight it.
	if !revocationAgedPastFailoverGrace(a.factory, sa, pa) {
		return nil, nil
	}

	return &types.Problem{
		Code:           types.ProblemReplicaRecruitAbandoned,
		CheckName:      a.Name(),
		PoolerID:       poolerID(pa),
		ShardKey:       sa.ShardKey,
		Description:    fmt.Sprintf("Follower %s is stranded by an abandoned recruit (revocation outranks the committed rule)", poolerID(pa).Name),
		Priority:       types.PriorityHigh,
		Scope:          types.ScopePooler,
		DetectedAt:     time.Now(),
		RecoveryAction: a.factory.NewReconnectRecruitAbandonedAction(),
	}, nil
}
