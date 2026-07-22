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

// revocationStrandsFollower reports whether the leader's rule — the very rule
// orch would relay to this follower via SetPrimary — would be rejected by the
// follower's recorded TermRevocation, leaving it stranded. This is the signature
// of an abandoned recruit: a failover started at a higher term reached this
// follower but never committed a rule at that term.
//
// It is a faithful pre-check of SetPrimary's own gate, which ignores an incoming
// rule when IsRuleRevoked(rule, revocation) holds (see the pooler's
// SetPrimary). We mirror that exact predicate against the position orch would
// send (HighestPosition) so detection cannot drift from enforcement — rather
// than ValidateRevocation, which layers on checks SetPrimary does not apply (LSN
// parse, recruit floor, stored-revocation consistency). A legitimate failover
// that has since committed a rule at the revocation's term makes HighestPosition
// outrank the revocation, so this returns false and the follower is no longer
// considered stranded. Returns false when no rule is known.
//
// ReplicaNotReplicatingAnalyzer also consults this to hand such a follower off
// to this analyzer rather than firing a SetPrimary the follower would ignore.
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
	if !pa.IsInitialized() {
		return nil, nil
	}

	// We need a serving, known leader with an address: the fix drives an
	// UpdateConsensusRule on the leader and then a SetPrimary toward the follower.
	if !leaderServing(sa) || sa.Leader.Health().GetMultipooler().GetHostname() == "" {
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

	// Give an in-flight recruit time to finish before concluding it was
	// abandoned. Another orchestrator with fresher information may still be
	// completing a legitimate failover at this term (e.g. the current leader is
	// resigning but still writable). Advancing the rule out from under such a
	// failover would fight it, so we wait until the revocation has aged past the
	// failover grace window — the bound within which a live failover acts. Once
	// that failover commits a rule at the higher term the follower is no longer
	// stranded (its revocation no longer outranks the committed decision) and this
	// analyzer stops firing on its own.
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
