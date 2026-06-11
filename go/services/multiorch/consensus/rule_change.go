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

	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/eventlog"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// coordinatorLedRuleChange orchestrates the recruit → check → promote workflow
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

// Run executes the rule change: pre-validate, recruit all nodes concurrently,
// and promote as soon as a viable proposal can be assembled. Each node
// receives its Promote immediately once it has been recruited and the
// proposal is ready — there is no unnecessary waiting between the two.
//
// revocation is the authoritative term revocation the coordinator is
// proposing. Callers own its construction:
//   - For safe coordinator-led transitions (failover), use
//     commonconsensus.NewTermRevocation to derive it from cohort statuses.
//   - For externally-certified transitions (bootstrap, operator override),
//     construct the cert and pass cert.GetTermRevocation() — the agent
//     defines revoked_below_term and outgoing_rule, not local discovery.
func (r *coordinatorLedRuleChange) Run(
	ctx context.Context,
	cohort []*multiorchdatapb.PoolerHealthState,
	revocation *clustermetadatapb.TermRevocation,
) error {
	if revocation == nil {
		return mterrors.New(mtrpcpb.Code_INVALID_ARGUMENT, "Run: revocation is required")
	}
	proposedTerm := revocation.GetRevokedBelowTerm()

	// Extract cached consensus statuses for the pre-vote feasibility check.
	var initialStatuses []*clustermetadatapb.ConsensusStatus
	for _, p := range cohort {
		if cs := p.GetConsensusStatus(); cs != nil {
			initialStatuses = append(initialStatuses, cs)
		}
	}

	r.coordinator.logger.InfoContext(ctx, "Starting rule change",
		"proposed_term", proposedTerm,
		"outgoing_rule", commonconsensus.FormatRuleNumber(revocation.GetOutgoingRule()),
		"cohort_size", len(cohort))

	// Back off if any node recently accepted a revocation — another coordinator
	// may be running an election. A backoff is a decision not to start this
	// appointment, so it precedes the Started event below rather than failing one.
	if err := checkRecentAcceptance(ctx, r.coordinator.logger, cohort); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "%v", err)
	}

	// Pre-validate that a proposal would be feasible with current statuses before
	// committing to a recruitment round.
	if err := r.checkProposalPossible(revocation, initialStatuses); err != nil {
		return mterrors.Errorf(mtrpcpb.Code_UNAVAILABLE, "pre-vote failed: %v", err)
	}

	// Mark the start of the appointment before any RPCs go out. The leader is not
	// chosen until recruitment completes, so new_primary is omitted here; the
	// terminal Success/Failed event below carries it. proposed_term correlates
	// this Started event with its terminal event for tracing.
	//
	// start and selectedAt bracket the two latency phases reported on the
	// terminal event: recruit (start → leader selected) and promote (leader
	// selected → commit). Monotonic, so unaffected by wall-clock adjustments.
	start := time.Now()
	primaryPromotion := eventlog.PrimaryPromotion{
		ProposedTerm: proposedTerm,
		Reason:       r.reason,
	}
	eventlog.Emit(ctx, r.coordinator.logger, eventlog.Started, primaryPromotion)

	// Phase 1: Recruit all nodes concurrently.
	//
	// Each recruit returns a ConsensusStatus that reflects the node's current WAL
	// position and durability commitments. The coordinator accumulates these and
	// tries to build a proposal after each arrival, until one succeeds. This
	// lets the coordinator select the most advanced node that can still satisfy
	// the durability policy, without waiting for the slowest nodes in the
	// cohort to respond.

	recruited := r.dispatchRecruit(ctx, cohort, revocation)

	// Phase 2: collect recruits until tryBuildProposal succeeds, or all nodes
	// have responded without reaching quorum.
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

	propReq, leaderKey, err := r.collectRecruitsAndBuildProposal(cohort, revocation, recruited)
	selectedAt := time.Now()
	recruitMs := selectedAt.Sub(start).Milliseconds()
	if err != nil {
		promoteEvent := eventlog.PrimaryPromotion{
			ProposedTerm: proposedTerm,
			Reason:       r.reason,
			RecruitMs:    &recruitMs,
		}
		eventlog.Emit(ctx, r.coordinator.logger, eventlog.Failed, promoteEvent, "error", err)
		return err
	}

	// Phase 3: Propose to all nodes concurrently.
	//
	// Note that we send proposals to all nodes, including those that didn't
	// respond to recruit or whose recruits arrived after the proposal was
	// selected. This is important for safety: if a node didn't respond to
	// recruit, it may have been partitioned or slow, and thus may not have
	// received the proposal's term revocation. Sending it SetPrimary
	// ensures it learns about the new term. The same applies to late recruits
	// that arrived after the proposal was selected — they may have been too
	// slow to factor into leader selection, but they still need to learn about
	// the new term.

	promoteResults := r.dispatchPromote(ctx, cohort, propReq, leaderKey)

	// Phase 4: Wait for every Promote to return, not just the leader's. The rule
	// change is committed when the leader's Promote succeeds — which can
	// only happen because enough followers endorsed the proposal first to
	// satisfy the cohort's durability quorum. Returning early here would
	// let Run's context cancel the in-flight non-leader Promotes. The shard
	// is already safe at that point, but any pooler whose Promote was
	// cancelled wouldn't learn the new primary on this round and would
	// need to be re-wired before it could route correctly. Waiting avoids
	// that follow-up.
	//
	// Exception: if the leader's Promote fails, no new primary exists for
	// non-leaders to learn about, so there is nothing to gain by waiting
	// — return as soon as we see the leader's failure.
	leaderErr := r.waitForPromotes(ctx, cohort, promoteResults)
	newPrimary := propReq.GetProposal().GetProposalLeader().GetId().GetName()
	promoteMs := time.Since(selectedAt).Milliseconds()
	promoteEvent := eventlog.PrimaryPromotion{
		NewPrimary:   newPrimary,
		ProposedTerm: proposedTerm,
		Reason:       r.reason,
		RecruitMs:    &recruitMs,
		PromoteMs:    &promoteMs,
	}
	if leaderErr != nil {
		eventlog.Emit(ctx, r.coordinator.logger, eventlog.Failed, promoteEvent, "error", leaderErr)
		return mterrors.Wrapf(leaderErr, "leader %s failed to accept proposal", newPrimary)
	}

	eventlog.Emit(ctx, r.coordinator.logger, eventlog.Success, promoteEvent)
	return nil
}

type recruitResult struct {
	pooler *multiorchdatapb.PoolerHealthState
	cs     *clustermetadatapb.ConsensusStatus // nil if recruit failed
}

// dispatchRecruit issues Recruit RPCs to all cohort members concurrently and
// returns a channel that receives their results as they arrive. Each result
// includes the pooler's health state and the ConsensusStatus returned by its
// Recruit, or nil if the RPC failed or returned an empty status.
func (r *coordinatorLedRuleChange) dispatchRecruit(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, revocation *clustermetadatapb.TermRevocation) chan recruitResult {
	recruited := make(chan recruitResult, len(cohort))
	for _, p := range cohort {
		go func() {
			recruited <- recruitResult{pooler: p, cs: r.recruit(ctx, p, revocation)}
		}()
	}
	return recruited
}

// collectRecruitsAndBuildProposal reads recruit results from the channel and
// accumulates ConsensusStatuses until tryBuildProposal returns a non-error
// proposal, or all recruits have been processed. It returns the first
// successful proposal and the corresponding leader's key. If no proposal can
// be built after processing all recruits, it returns an error.
func (r *coordinatorLedRuleChange) collectRecruitsAndBuildProposal(cohort []*multiorchdatapb.PoolerHealthState, revocation *clustermetadatapb.TermRevocation, recruited chan recruitResult) (*consensusdatapb.PromoteRequest, string, error) {
	var err error
	var p *consensusdatapb.CoordinatorProposal
	statuses := make([]*clustermetadatapb.ConsensusStatus, 0, len(cohort))
	for range len(cohort) {
		rr := <-recruited
		if rr.cs == nil {
			continue
		}
		statuses = append(statuses, rr.cs)
		p, err = r.tryBuildProposal(revocation, statuses)
		if err == nil {
			ids := make([]*clustermetadatapb.ID, 0, len(statuses))
			for _, s := range statuses {
				ids = append(ids, s.GetId())
			}
			leaderKey := topoclient.ClusterIDString(p.GetProposalLeader().GetId())
			propReq := &consensusdatapb.PromoteRequest{
				Proposal:        p,
				Reason:          r.reason,
				AcceptedNodeIds: ids,
			}
			return propReq, leaderKey, nil
		}
	}
	if err == nil {
		err = errors.New("no nodes responded to recruit")
	}
	return nil, "", mterrors.Wrap(err, "recruitment failed")
}

type promoteResult struct {
	poolerName string
	isLeader   bool
	err        error
}

// dispatchPromote issues Promote or SetPrimary RPCs to all cohort members
// concurrently, depending on whether each is the selected leader, and returns a
// channel that receives their results as they arrive. Each result includes the
// pooler's name, whether it was the leader, and any error returned by the RPC
// (nil if it succeeded).
func (r *coordinatorLedRuleChange) dispatchPromote(
	ctx context.Context,
	cohort []*multiorchdatapb.PoolerHealthState,
	propReq *consensusdatapb.PromoteRequest,
	leaderKey string,
) chan promoteResult {
	promoteResults := make(chan promoteResult, len(cohort))
	for _, p := range cohort {
		isLeader := topoclient.ClusterIDString(p.MultiPooler.Id) == leaderKey
		go func() {
			err := r.promote(ctx, p, propReq, isLeader)
			promoteResults <- promoteResult{poolerName: p.MultiPooler.Id.Name, isLeader: isLeader, err: err}
		}()
	}

	return promoteResults
}

// waitForPromotes collects results from all cohort Promote/SetPrimary goroutines.
// It returns the leader's error as soon as one is seen (there is nothing to gain
// by waiting further if the leader failed), or nil if the leader succeeded.
// Non-leader failures are logged as warnings and do not affect the return value.
func (r *coordinatorLedRuleChange) waitForPromotes(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, promoteResults chan promoteResult) error {
	for range len(cohort) {
		pr := <-promoteResults
		if pr.err != nil {
			if pr.isLeader {
				return pr.err
			}
			r.coordinator.logger.WarnContext(ctx, "Promote failed for non-leader",
				"pooler", pr.poolerName, "error", pr.err)
		} else {
			r.coordinator.logger.InfoContext(ctx, "Promote succeeded",
				"pooler", pr.poolerName, "is_leader", pr.isLeader)
		}
	}
	return nil
}

// recruit issues a Recruit RPC to a single pooler and returns the resulting
// ConsensusStatus, or nil if the call failed or returned an empty status.
func (r *coordinatorLedRuleChange) recruit(
	ctx context.Context,
	p *multiorchdatapb.PoolerHealthState,
	revocation *clustermetadatapb.TermRevocation,
) *clustermetadatapb.ConsensusStatus {
	rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RuleWriteTimeout)
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

// promote dispatches the rule change to a single pooler. The designated leader
// receives a Promote RPC (it promotes postgres and writes the new rule); every
// other cohort member receives SetPrimary (it points replication at the
// new leader). Both RPCs read directly from the CoordinatorProposal —
// proposal_leader is a PoolerAddress, exactly what SetPrimary's leader
// field wants.
func (r *coordinatorLedRuleChange) promote(
	ctx context.Context,
	p *multiorchdatapb.PoolerHealthState,
	req *consensusdatapb.PromoteRequest,
	isLeader bool,
) error {
	rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RuleWriteTimeout)
	defer cancel()
	if isLeader {
		_, err := r.coordinator.rpcClient.Promote(rpcCtx, p.MultiPooler, req)
		return err
	}
	proposal := req.GetProposal()
	_, err := r.coordinator.rpcClient.SetPrimary(rpcCtx, p.MultiPooler, &consensusdatapb.SetPrimaryRequest{
		Leader: proposal.GetProposalLeader(),
		Rule:   proposal.GetProposedRule(),
	})
	return err
}

// buildFailoverProposal constructs a CoordinatorProposal for normal failover.
// It picks the first node from result.EligibleLeaders and resolves its contact
// address from addressByID. EligibleLeaders is produced by the Discoverer
// supplied to BuildSafeProposal — leadershipAwareDiscoverer ensures the set
// contains the most-advanced node that reports ELIGIBLE leadership availability,
// falling back to the most-advanced node overall if all are INELIGIBLE. Resigned
// poolers are filtered out upstream in Coordinator.runFailover.
func buildFailoverProposal(
	result commonconsensus.RecruitmentResult,
	addressByID map[string]*clustermetadatapb.PoolerAddress,
) (*consensusdatapb.CoordinatorProposal, error) {
	if result.OutgoingRule == nil {
		return nil, errors.New("no committed rule found; use bootstrap path for fresh clusters")
	}
	if len(result.EligibleLeaders) == 0 {
		return nil, errors.New("no eligible leaders for failover proposal")
	}
	leader := result.EligibleLeaders[0]

	addr, ok := addressByID[topoclient.ClusterIDString(leader.GetId())]
	if !ok {
		return nil, fmt.Errorf("leader %s not found in cohort", leader.GetId().GetName())
	}

	return &consensusdatapb.CoordinatorProposal{
		TermRevocation: result.TermRevocation,
		ProposalLeader: addr,
		ProposedRule: &clustermetadatapb.ShardRule{
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: result.TermRevocation.GetRevokedBelowTerm()},
			CohortMembers:    result.OutgoingRule.GetCohortMembers(),
			DurabilityPolicy: result.OutgoingRule.GetDurabilityPolicy(),
			LeaderId:         leader.GetId(),
			// The coordinator that ran the recruit round (carried in the
			// revocation's accepted_coordinator_id) is also the
			// coordinator-of-record for the rule it produces.
			CoordinatorId: result.TermRevocation.GetAcceptedCoordinatorId(),
			CreationTime:  timestamppb.Now(),
		},
	}, nil
}

// buildBootstrapProposal constructs a CoordinatorProposal for initial leader
// appointment on a fresh shard. Unlike buildFailoverProposal, the cohort and
// durability policy come from the caller — there is no recorded rule to
// derive them from — and any eligible leader works since none have prior
// commitments.
func buildBootstrapProposal(
	result commonconsensus.RecruitmentResult,
	cohortIDs []*clustermetadatapb.ID,
	policy *clustermetadatapb.DurabilityPolicy,
	addressByID map[string]*clustermetadatapb.PoolerAddress,
) (*consensusdatapb.CoordinatorProposal, error) {
	if len(result.EligibleLeaders) == 0 {
		return nil, errors.New("no eligible leaders for bootstrap proposal")
	}
	leader := result.EligibleLeaders[0]
	addr, ok := addressByID[topoclient.ClusterIDString(leader.GetId())]
	if !ok {
		return nil, fmt.Errorf("leader %s not found in cohort", leader.GetId().GetName())
	}
	return &consensusdatapb.CoordinatorProposal{
		TermRevocation: result.TermRevocation,
		ProposalLeader: addr,
		ProposedRule: &clustermetadatapb.ShardRule{
			RuleNumber:       &clustermetadatapb.RuleNumber{CoordinatorTerm: result.TermRevocation.GetRevokedBelowTerm()},
			CohortMembers:    cohortIDs,
			DurabilityPolicy: policy,
			LeaderId:         leader.GetId(),
			CoordinatorId:    result.TermRevocation.GetAcceptedCoordinatorId(),
			CreationTime:     timestamppb.Now(),
		},
		SkipOutgoingQuorum: true,
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

// buildCohortMaps returns address-by-ID and health-by-ID lookup maps built
// from a cohort slice. Keys are ClusterIDString values. Consensus paths only
// need the leader's contact info to build proposals, so the cohort map is
// flattened to PoolerAddress here.
func buildCohortMaps(cohort []*multiorchdatapb.PoolerHealthState) (map[string]*clustermetadatapb.PoolerAddress, map[string]*multiorchdatapb.PoolerHealthState) {
	addressByID := make(map[string]*clustermetadatapb.PoolerAddress, len(cohort))
	healthByID := make(map[string]*multiorchdatapb.PoolerHealthState, len(cohort))
	for _, p := range cohort {
		key := topoclient.ClusterIDString(p.MultiPooler.Id)
		addressByID[key] = topoclient.PoolerAddressFor(p.MultiPooler)
		healthByID[key] = p
	}
	return addressByID, healthByID
}
