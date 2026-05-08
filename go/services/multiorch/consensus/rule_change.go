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

package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// coordinatorLedRuleChange orchestrates the recruit → check → propose workflow
// for a coordinator-initiated rule change. It is parameterized by action-specific
// callbacks so the same workflow serves both normal failover and bootstrap.
type coordinatorLedRuleChange struct {
	coordinator           *Coordinator
	reason                string
	tryBuildProposal      func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error)
	checkProposalPossible func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) error
}

func (c *Coordinator) newRuleChange(
	reason string,
	tryBuildProposal func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error),
	checkProposalPossible func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) error,
) *coordinatorLedRuleChange {
	return &coordinatorLedRuleChange{
		coordinator:           c,
		reason:                reason,
		tryBuildProposal:      tryBuildProposal,
		checkProposalPossible: checkProposalPossible,
	}
}

// Run executes the rule change: derive term, pre-validate, recruit all nodes
// concurrently, and propose as soon as a viable proposal can be assembled.
// Each node receives its Propose immediately once it has been recruited and
// the proposal is ready — there is no unnecessary waiting between the two.
func (r *coordinatorLedRuleChange) Run(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState) error {
	// Extract cached consensus statuses to derive the revocation term.
	var initialStatuses []*clustermetadatapb.ConsensusStatus
	for _, p := range cohort {
		if cs := p.GetConsensusStatus(); cs != nil {
			initialStatuses = append(initialStatuses, cs)
		}
	}

	revocation := commonconsensus.NewTermRevocation(initialStatuses, r.coordinator.coordinatorID)

	r.coordinator.logger.InfoContext(ctx, "Starting rule change",
		"proposed_term", revocation.GetRevokedBelowTerm(),
		"cohort_size", len(cohort))

	// Back off if any node recently accepted a revocation — another coordinator
	// may be running an election.
	if err := checkRecentAcceptance(ctx, r.coordinator.logger, cohort); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "%v", err)
	}

	// Pre-validate that a proposal would be feasible with current statuses before
	// committing to a recruitment round.
	if err := r.checkProposalPossible(revocation, initialStatuses); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pre-vote failed: %v", err)
	}

	// Recruit all nodes concurrently.
	type recruitResult struct {
		pooler *multiorchdatapb.PoolerHealthState
		cs     *clustermetadatapb.ConsensusStatus // nil if recruit failed
	}
	recruited := make(chan recruitResult, len(cohort))
	for _, p := range cohort {
		go func() {
			recruited <- recruitResult{pooler: p, cs: r.recruit(ctx, p, revocation)}
		}()
	}

	var (
		statuses  []*clustermetadatapb.ConsensusStatus
		recruits  []*multiorchdatapb.PoolerHealthState
		propReq   *consensusdatapb.ProposeRequest
		leaderKey string
	)

	type proposeResult struct {
		poolerName string
		isLeader   bool
		err        error
	}
	proposeResults := make(chan proposeResult, len(cohort))

	sendPropose := func(p *multiorchdatapb.PoolerHealthState) {
		isLeader := topoclient.ClusterIDString(p.MultiPooler.Id) == leaderKey
		go func() {
			err := r.propose(ctx, p, propReq)
			proposeResults <- proposeResult{poolerName: p.MultiPooler.Id.Name, isLeader: isLeader, err: err}
		}()
	}

	// Phase 1: collect recruits until tryBuildProposal succeeds (or we run out).
	//
	// We commit to the leader at the FIRST successful tryBuildProposal and stream
	// Proposes from there — recruits arriving after that point don't change
	// the choice. This is consensus-correct: only committed writes are
	// guaranteed durable across the quorum, so the chosen leader carries
	// every committed write by definition. The tradeoff is that *uncommitted*
	// WAL on a late-arriving more-advanced node is discarded when the node
	// joins as a follower (pg_rewind on rejoin) instead of being preserved
	// by electing that node leader.
	//
	// TODO: add a configurable grace period after quorum is reached but
	// before the leader is locked in, to let slow-but-not-dead nodes
	// participate in leader selection. With grace=0 we keep the current
	// streaming behavior; with grace>0 we wait up to that long for further
	// recruits and re-run tryBuildProposal on the augmented set. The right default
	// should come from observed failover latency in production.
	remaining := len(cohort)
	for ; remaining > 0 && propReq == nil; remaining-- {
		rr := <-recruited
		if rr.cs == nil {
			continue
		}
		statuses = append(statuses, rr.cs)
		recruits = append(recruits, rr.pooler)
		p, err := r.tryBuildProposal(revocation, statuses)
		if err != nil {
			continue
		}
		ids := make([]*clustermetadatapb.ID, 0, len(statuses))
		for _, s := range statuses {
			ids = append(ids, s.GetId())
		}
		leaderKey = topoclient.ClusterIDString(p.GetProposalLeader().GetId())
		propReq = &consensusdatapb.ProposeRequest{
			Proposal:        p,
			Reason:          r.reason,
			AcceptedNodeIds: ids,
		}
		eventlog.Emit(ctx, r.coordinator.logger, eventlog.Started, eventlog.PrimaryPromotion{
			NewPrimary: p.GetProposalLeader().GetId().GetName(),
		})
	}

	if propReq == nil {
		_, err := r.tryBuildProposal(revocation, statuses)
		return mterrors.Wrap(err, "recruitment failed")
	}
	for _, p := range recruits {
		sendPropose(p)
	}

	// Phase 2: drain remaining recruits, proposing to each as it arrives.
	for range remaining {
		if rr := <-recruited; rr.cs != nil {
			sendPropose(rr.pooler)
			recruits = append(recruits, rr.pooler)
		}
	}

	// Wait for every Propose to return, not just the leader's. The rule
	// change is committed when the leader's Propose succeeds — which can
	// only happen because enough followers endorsed the proposal first to
	// satisfy the cohort's durability quorum. Returning early here would
	// let Run's context cancel the in-flight non-leader Proposes. The shard
	// is already safe at that point, but any pooler whose Propose was
	// cancelled wouldn't learn the new primary on this round and would
	// need to be re-wired before it could route correctly. Waiting avoids
	// that follow-up.
	//
	// Exception: if the leader's Propose fails, no new primary exists for
	// non-leaders to learn about, so there is nothing to gain by waiting
	// — return as soon as we see the leader's failure.
	var leaderErr error
	for range len(recruits) {
		pr := <-proposeResults
		if pr.err != nil {
			if pr.isLeader {
				leaderErr = pr.err
				break
			}
			r.coordinator.logger.WarnContext(ctx, "Propose failed for non-leader",
				"pooler", pr.poolerName, "error", pr.err)
		} else {
			r.coordinator.logger.InfoContext(ctx, "Propose succeeded",
				"pooler", pr.poolerName, "is_leader", pr.isLeader)
		}
	}

	newPrimary := propReq.GetProposal().GetProposalLeader().GetId().GetName()
	if leaderErr != nil {
		eventlog.Emit(ctx, r.coordinator.logger, eventlog.Failed, eventlog.PrimaryPromotion{
			NewPrimary: newPrimary,
		}, "error", leaderErr)
		return mterrors.Wrapf(leaderErr, "leader %s failed to accept proposal", newPrimary)
	}
	eventlog.Emit(ctx, r.coordinator.logger, eventlog.Success, eventlog.PrimaryPromotion{
		NewPrimary: newPrimary,
	})
	return nil
}

// recruit issues a Recruit RPC to a single pooler and returns the resulting
// ConsensusStatus, or nil if the call failed or returned an empty status.
func (r *coordinatorLedRuleChange) recruit(
	ctx context.Context,
	p *multiorchdatapb.PoolerHealthState,
	revocation *clustermetadatapb.TermRevocation,
) *clustermetadatapb.ConsensusStatus {
	rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()
	resp, err := r.coordinator.rpcClient.Recruit(rpcCtx, p.MultiPooler, &consensusdatapb.RecruitRequest{
		TermRevocation: revocation,
	})
	switch {
	case err != nil:
		r.coordinator.logger.WarnContext(ctx, "Recruit failed",
			"pooler", p.MultiPooler.Id.Name, "error", err)
		return nil
	case resp.GetConsensusStatus() == nil:
		r.coordinator.logger.WarnContext(ctx, "Recruit returned nil ConsensusStatus",
			"pooler", p.MultiPooler.Id.Name)
		return nil
	default:
		cs := resp.GetConsensusStatus()
		r.coordinator.logger.InfoContext(ctx, "Recruited pooler",
			"pooler", p.MultiPooler.Id.Name,
			"lsn", cs.GetCurrentPosition().GetLsn())
		return cs
	}
}

// propose issues a Propose RPC to a single pooler.
func (r *coordinatorLedRuleChange) propose(
	ctx context.Context,
	p *multiorchdatapb.PoolerHealthState,
	req *consensusdatapb.ProposeRequest,
) error {
	rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer cancel()
	_, err := r.coordinator.rpcClient.Propose(rpcCtx, p.MultiPooler, req)
	return err
}

// buildFailoverProposal constructs a CoordinatorProposal for normal failover.
// It picks the first non-resigned eligible leader from result.EligibleLeaders
// and derives the cohort and durability policy from result.OutgoingRule.
func buildFailoverProposal(
	ctx context.Context,
	logger *slog.Logger,
	result commonconsensus.RecruitmentResult,
	poolerByID map[string]*clustermetadatapb.MultiPooler,
	healthByID map[string]*multiorchdatapb.PoolerHealthState,
) (*consensusdatapb.CoordinatorProposal, error) {
	if result.OutgoingRule == nil {
		return nil, errors.New("no committed rule found; use bootstrap path for fresh clusters")
	}

	var leader *clustermetadatapb.ConsensusStatus
	for _, cs := range result.EligibleLeaders {
		key := topoclient.ClusterIDString(cs.GetId())
		if health, ok := healthByID[key]; ok && types.LeaderNeedsReplacement(health) {
			logger.InfoContext(ctx, "Skipping resigned primary during leader selection",
				"pooler", cs.GetId().GetName())
			continue
		}
		leader = cs
		break
	}
	if leader == nil {
		return nil, errors.New("all eligible leaders have resigned")
	}

	mp, ok := poolerByID[topoclient.ClusterIDString(leader.GetId())]
	if !ok {
		return nil, fmt.Errorf("leader %s not found in cohort", leader.GetId().GetName())
	}

	return &consensusdatapb.CoordinatorProposal{
		TermRevocation: result.TermRevocation,
		ProposalLeader: &consensusdatapb.ProposalLeader{
			Id:           leader.GetId(),
			Host:         mp.GetHostname(),
			PostgresPort: mp.GetPortMap()["postgres"],
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: result.TermRevocation.GetRevokedBelowTerm()},
			CohortMembers:    result.OutgoingRule.GetCohortMembers(),
			DurabilityPolicy: result.OutgoingRule.GetDurabilityPolicy(),
			LeaderId:         leader.GetId(),
		},
	}, nil
}

// checkRecentAcceptance returns an error if any node in the cohort recently
// accepted a term revocation, which may indicate another coordinator is making
// a rule change.
func checkRecentAcceptance(ctx context.Context, logger *slog.Logger, cohort []*multiorchdatapb.PoolerHealthState) error {
	const backoffWindow = 4 * time.Second
	now := time.Now()
	for _, pooler := range cohort {
		rev := pooler.GetConsensusStatus().GetTermRevocation()
		if rev == nil || rev.CoordinatorInitiatedAt == nil {
			continue
		}
		timeSince := now.Sub(rev.CoordinatorInitiatedAt.AsTime())
		if timeSince >= 0 && timeSince < backoffWindow {
			logger.InfoContext(ctx, "Recent term acceptance detected, backing off",
				"pooler", pooler.MultiPooler.Id.Name,
				"accepted_term", rev.RevokedBelowTerm,
				"time_since_acceptance", timeSince)
			return fmt.Errorf("another coordinator started recruiting recently (%v ago), backing off",
				timeSince.Round(time.Millisecond))
		}
	}
	return nil
}

// buildCohortMaps returns pooler-by-ID and health-by-ID lookup maps built from
// a cohort slice. Keys are ClusterIDString values.
func buildCohortMaps(cohort []*multiorchdatapb.PoolerHealthState) (map[string]*clustermetadatapb.MultiPooler, map[string]*multiorchdatapb.PoolerHealthState) {
	poolerByID := make(map[string]*clustermetadatapb.MultiPooler, len(cohort))
	healthByID := make(map[string]*multiorchdatapb.PoolerHealthState, len(cohort))
	for _, p := range cohort {
		key := topoclient.ClusterIDString(p.MultiPooler.Id)
		poolerByID[key] = p.MultiPooler
		healthByID[key] = p
	}
	return poolerByID, healthByID
}
