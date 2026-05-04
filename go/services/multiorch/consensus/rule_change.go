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
	coordinator   *Coordinator
	reason        string
	tryBuild      func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error)
	checkPossible func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) error
}

func (c *Coordinator) newRuleChange(
	reason string,
	tryBuild func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error),
	checkPossible func(*clustermetadatapb.TermRevocation, []*clustermetadatapb.ConsensusStatus) error,
) *coordinatorLedRuleChange {
	return &coordinatorLedRuleChange{
		coordinator:   c,
		reason:        reason,
		tryBuild:      tryBuild,
		checkPossible: checkPossible,
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
	if err := r.checkPossible(revocation, initialStatuses); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pre-vote failed: %v", err)
	}

	// Recruit all nodes concurrently. Each goroutine sends its result to a
	// buffered channel so it never blocks.
	type recruitResult struct {
		pooler *multiorchdatapb.PoolerHealthState
		cs     *clustermetadatapb.ConsensusStatus // nil if recruit failed
	}
	recruited := make(chan recruitResult, len(cohort))
	for _, p := range cohort {
		go func(p *multiorchdatapb.PoolerHealthState) {
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			resp, err := r.coordinator.rpcClient.Recruit(rpcCtx, p.MultiPooler, &consensusdatapb.RecruitRequest{
				TermRevocation: revocation,
			})
			cancel()
			var cs *clustermetadatapb.ConsensusStatus
			switch {
			case err != nil:
				r.coordinator.logger.WarnContext(ctx, "Recruit failed",
					"pooler", p.MultiPooler.Id.Name, "error", err)
			case resp.GetConsensusStatus() == nil:
				r.coordinator.logger.WarnContext(ctx, "Recruit returned nil ConsensusStatus",
					"pooler", p.MultiPooler.Id.Name)
			default:
				cs = resp.GetConsensusStatus()
				r.coordinator.logger.InfoContext(ctx, "Recruited pooler",
					"pooler", p.MultiPooler.Id.Name,
					"lsn", cs.GetCurrentPosition().GetLsn())
			}
			recruited <- recruitResult{pooler: p, cs: cs}
		}(p)
	}

	// Process recruit results as they arrive. As soon as a viable proposal can
	// be built, propose to all already-recruited nodes immediately. Any node
	// that recruits after the proposal is ready is proposed to right away.
	var (
		statuses  []*clustermetadatapb.ConsensusStatus
		propReq   *consensusdatapb.ProposeRequest
		leaderKey string
		waiting   []*multiorchdatapb.PoolerHealthState // recruited before proposal was ready
	)

	type proposeResult struct {
		poolerName string
		isLeader   bool
		err        error
	}
	proposeResults := make(chan proposeResult, len(cohort))
	proposeSent := 0

	sendPropose := func(p *multiorchdatapb.PoolerHealthState) {
		isLeader := topoclient.ClusterIDString(p.MultiPooler.Id) == leaderKey
		go func() {
			propCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			_, err := r.coordinator.rpcClient.Propose(propCtx, p.MultiPooler, propReq)
			cancel()
			proposeResults <- proposeResult{poolerName: p.MultiPooler.Id.Name, isLeader: isLeader, err: err}
		}()
		proposeSent++
	}

	for range len(cohort) {
		rr := <-recruited
		if rr.cs == nil {
			continue
		}
		statuses = append(statuses, rr.cs)
		if propReq != nil {
			// Proposal already built; propose to this node immediately.
			sendPropose(rr.pooler)
		} else if p, err := r.tryBuild(revocation, statuses); err == nil {
			// This recruit tipped us over quorum. Build the proposal and propose
			// to all waiting nodes and this one.
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
			for _, pp := range waiting {
				sendPropose(pp)
			}
			sendPropose(rr.pooler)
		} else {
			waiting = append(waiting, rr.pooler)
		}
	}

	// Collect all propose results.
	var leaderErr error
	for range proposeSent {
		pr := <-proposeResults
		if pr.err != nil {
			if pr.isLeader {
				leaderErr = pr.err
			} else {
				r.coordinator.logger.WarnContext(ctx, "Propose failed for non-leader",
					"pooler", pr.poolerName, "error", pr.err)
			}
		} else {
			r.coordinator.logger.InfoContext(ctx, "Propose succeeded",
				"pooler", pr.poolerName, "is_leader", pr.isLeader)
		}
	}

	if propReq == nil {
		_, err := r.tryBuild(revocation, statuses)
		return mterrors.Wrap(err, "recruitment failed")
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

// buildFailoverProposal constructs a CoordinatorProposal for normal failover.
// It picks the first non-resigned eligible leader from result.EligibleLeaders
// and derives the cohort and durability policy from result.BestRule.
func buildFailoverProposal(
	ctx context.Context,
	logger *slog.Logger,
	result commonconsensus.RecruitmentResult,
	poolerByID map[string]*clustermetadatapb.MultiPooler,
	healthByID map[string]*multiorchdatapb.PoolerHealthState,
) (*consensusdatapb.CoordinatorProposal, error) {
	if result.BestRule == nil {
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
			CohortMembers:    result.BestRule.GetCohortMembers(),
			DurabilityPolicy: result.BestRule.GetDurabilityPolicy(),
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
