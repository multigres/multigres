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
	"sync"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
)

// ApplyCertifiedRuleChange installs a new shard rule using a fully-populated
// externally certified revocation. Used for both initial leader appointment
// (term 0 → term 1) and stuck-quorum recovery (term N → term N+1).
//
// The coordinator is a pure executor here: every identity and timing field
// (proposed_rule.coordinator_id, .creation_time, .rule_number;
// cert.term_revocation.*) must be populated by the caller. The coordinator
// validates the inputs, then runs the standard Recruit + Propose fan-out
// against the proposed cohort.
func (c *Coordinator) ApplyCertifiedRuleChange(
	ctx context.Context,
	shardKey *clustermetadatapb.ShardKey,
	proposedRule *clustermetadatapb.ShardRule,
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
	reason string,
) error {
	if err := validateCertifiedRuleChange(shardKey, proposedRule, cert); err != nil {
		return err
	}

	// Refresh ConsensusStatus from every pooler registered for the shard
	// (not just the proposed cohort). The same snapshot drives both the
	// shard-wide safety check below and the pre-vote check in
	// coordinatorLedRuleChange.Run; without primed statuses the
	// externally-certified pre-vote sees an empty set and fails before
	// recruitment can prove the cert is acceptable.
	statusesByPoolerID, err := c.refreshShardConsensusStatuses(ctx, shardKey)
	if err != nil {
		return err
	}
	if err := checkNoShardPoolerAheadOfOutgoingRule(statusesByPoolerID, cert.GetTermRevocation().GetOutgoingRule()); err != nil {
		return err
	}

	poolerByID, cohort, err := c.resolveCohort(ctx, proposedRule.GetCohortMembers())
	if err != nil {
		return err
	}
	for _, p := range cohort {
		if cs, ok := statusesByPoolerID[topoclient.ClusterIDString(p.MultiPooler.Id)]; ok {
			p.ConsensusStatus = cs
		}
	}

	// Defense in depth: validateCertifiedRuleChange already verified the
	// leader appears in cohort_members by ID, and resolveCohort fetched a
	// MultiPooler for every cohort member, so this lookup cannot fail in
	// practice. The explicit check guards against future refactors.
	leaderKey := topoclient.ClusterIDString(proposedRule.GetLeaderId())
	leaderPooler, ok := poolerByID[leaderKey]
	if !ok {
		return mterrors.Errorf(mtrpcpb.Code_INTERNAL,
			"leader %s is not a member of the proposed cohort (should be unreachable)", leaderKey)
	}

	// The cert's term_revocation is the revocation each pooler will record
	// via Recruit. Pre-build the proposal struct that BuildExternallyCertifiedProposal
	// will return — it does not change as recruits arrive, since the caller has
	// already committed to a specific leader/cohort/durability.
	revocation := cert.GetTermRevocation()
	proposal := &consensusdatapb.CoordinatorProposal{
		TermRevocation: revocation,
		ProposalLeader: &clustermetadatapb.PoolerAddress{
			Id:           proposedRule.GetLeaderId(),
			Host:         leaderPooler.GetHostname(),
			PostgresPort: leaderPooler.GetPortMap()["postgres"],
		},
		ProposedRule: proposedRule,
	}

	buildProposal := func(_ commonconsensus.RecruitmentResult) (*consensusdatapb.CoordinatorProposal, error) {
		return proposal, nil
	}
	tryBuildProposal := func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) (*consensusdatapb.CoordinatorProposal, error) {
		return commonconsensus.BuildExternallyCertifiedProposal(cert, statuses, buildProposal)
	}
	checkProposalPossible := func(_ *clustermetadatapb.TermRevocation, statuses []*clustermetadatapb.ConsensusStatus) error {
		return commonconsensus.CheckExternallyCertifiedProposalPossible(cert, statuses, buildProposal)
	}

	c.logger.InfoContext(ctx, "Applying certified rule change",
		"shard", shardKey,
		"leader", proposedRule.GetLeaderId().GetName(),
		"cohort_size", len(cohort),
		"outgoing_term", revocation.GetOutgoingRule().GetCoordinatorTerm(),
		"new_term", revocation.GetRevokedBelowTerm(),
		"frozen_lsn", cert.GetFrozenLsn(),
		"reason", reason)

	return c.newRuleChange(reason, tryBuildProposal, checkProposalPossible).Run(ctx, cohort, revocation)
}

// refreshShardConsensusStatuses calls ConsensusStatus on every pooler
// registered for the shard (not just the proposed cohort) in parallel and
// returns the responses keyed by ClusterIDString.
//
// Unreachable poolers are absent from the map: by submitting the cert, the
// operator has already attested that absent nodes will not commit further
// writes, and we can only verify what we can observe.
//
// Topology enumeration errors are fatal — silently skipping a cell we could
// not list might miss a pooler that is ahead of the cert.
func (c *Coordinator) refreshShardConsensusStatuses(
	ctx context.Context,
	shardKey *clustermetadatapb.ShardKey,
) (map[string]*clustermetadatapb.ConsensusStatus, error) {
	cellNames, err := c.topoStore.GetCellNames(ctx)
	if err != nil {
		return nil, mterrors.Wrap(err, "failed to list cells for shard status refresh")
	}
	var poolers []*clustermetadatapb.MultiPooler
	for _, cell := range cellNames {
		infos, err := c.topoStore.GetMultiPoolersByCell(ctx, cell, &topoclient.GetMultiPoolersByCellOptions{
			DatabaseShard: &topoclient.DatabaseShard{
				Database:   shardKey.GetDatabase(),
				TableGroup: shardKey.GetTableGroup(),
				Shard:      shardKey.GetShard(),
			},
		})
		if err != nil {
			return nil, mterrors.Wrapf(err, "failed to list poolers in cell %s during shard status refresh", cell)
		}
		for _, info := range infos {
			if info.MultiPooler != nil {
				poolers = append(poolers, info.MultiPooler)
			}
		}
	}
	statuses := make(map[string]*clustermetadatapb.ConsensusStatus)
	if len(poolers) == 0 {
		return statuses, nil
	}

	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, p := range poolers {
		wg.Go(func() {
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			resp, err := c.rpcClient.ConsensusStatus(rpcCtx, p, &consensusdatapb.StatusRequest{})
			if err != nil {
				c.logger.WarnContext(ctx, "shard ConsensusStatus probe failed",
					"pooler", p.GetId().GetName(), "error", err)
				return
			}
			mu.Lock()
			statuses[topoclient.ClusterIDString(p.GetId())] = resp.GetConsensusStatus()
			mu.Unlock()
		})
	}
	wg.Wait()
	return statuses, nil
}

// checkNoShardPoolerAheadOfOutgoingRule rejects the rule change if any
// reachable pooler has a recorded rule with a coordinator_term greater than
// the cert's outgoing_rule_number.coordinator_term. This catches operator
// certs that claim a stale outgoing rule when a non-cohort node has actually
// progressed further. Operates on the snapshot returned by
// refreshShardConsensusStatuses.
func checkNoShardPoolerAheadOfOutgoingRule(
	statuses map[string]*clustermetadatapb.ConsensusStatus,
	outgoingRule *clustermetadatapb.RuleNumber,
) error {
	for poolerKey, cs := range statuses {
		observed := cs.GetCurrentPosition().GetRule().GetRuleNumber()
		if observed == nil {
			continue
		}
		if commonconsensus.CompareRuleNumbers(observed, outgoingRule) > 0 {
			return mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
				"pooler %s has rule term %d which is ahead of cert.outgoing_rule_number.coordinator_term %d; cert is stale",
				poolerKey, observed.GetCoordinatorTerm(), outgoingRule.GetCoordinatorTerm())
		}
	}
	return nil
}

// resolveCohort looks up each cohort member ID in topology and returns a
// lookup map by ClusterIDString plus a PoolerHealthState slice for the
// coordinatorLedRuleChange runner. PoolerHealthState entries carry only
// MultiPooler — consensus statuses are gathered fresh during recruit.
func (c *Coordinator) resolveCohort(
	ctx context.Context,
	cohortMembers []*clustermetadatapb.ID,
) (map[string]*clustermetadatapb.MultiPooler, []*multiorchdatapb.PoolerHealthState, error) {
	poolerByID := make(map[string]*clustermetadatapb.MultiPooler, len(cohortMembers))
	cohort := make([]*multiorchdatapb.PoolerHealthState, 0, len(cohortMembers))
	for _, id := range cohortMembers {
		info, err := c.topoStore.GetMultiPooler(ctx, id)
		if err != nil {
			return nil, nil, mterrors.Wrapf(err,
				"failed to look up cohort member %s", topoclient.ClusterIDString(id))
		}
		key := topoclient.ClusterIDString(id)
		poolerByID[key] = info.MultiPooler
		cohort = append(cohort, &multiorchdatapb.PoolerHealthState{
			MultiPooler: info.MultiPooler,
		})
	}
	return poolerByID, cohort, nil
}

// validateCertifiedRuleChange enforces the shape contract documented on the
// proto request: every identity and timing field must be set, the proposed
// rule_number must agree with the cert's revoked_below_term, and the leader
// must be in the proposed cohort.
func validateCertifiedRuleChange(
	shardKey *clustermetadatapb.ShardKey,
	proposedRule *clustermetadatapb.ShardRule,
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
) error {
	if shardKey == nil || shardKey.GetDatabase() == "" || shardKey.GetTableGroup() == "" || shardKey.GetShard() == "" {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "shard_key is required (database, table_group, shard)")
	}
	if proposedRule == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule is required")
	}
	if proposedRule.GetLeaderId() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule.leader_id is required")
	}
	if len(proposedRule.GetCohortMembers()) == 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule.cohort_members must be non-empty")
	}
	if proposedRule.GetDurabilityPolicy() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule.durability_policy is required")
	}
	if proposedRule.GetRuleNumber() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule.rule_number is required")
	}
	if proposedRule.GetCoordinatorId() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule.coordinator_id is required")
	}
	if proposedRule.GetCreationTime() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "proposed_rule.creation_time is required")
	}

	if cert == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert is required")
	}
	if cert.GetFrozenLsn() == "" {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert.frozen_lsn is required (use \"0/0\" for initial appointment)")
	}
	rev := cert.GetTermRevocation()
	if rev == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert.term_revocation is required")
	}
	if rev.GetOutgoingRule() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert.term_revocation.outgoing_rule is required (use a zero RuleNumber for initial appointment)")
	}
	if rev.GetRevokedBelowTerm() <= 0 {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert.term_revocation.revoked_below_term must be positive")
	}
	if rev.GetAcceptedCoordinatorId() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert.term_revocation.accepted_coordinator_id is required")
	}
	if rev.GetCoordinatorInitiatedAt() == nil {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT, "cert.term_revocation.coordinator_initiated_at is required")
	}

	// Cross-field consistency: the proposed rule's coordinator_term must match
	// the cert's revoked_below_term (enforced by validateProposal in
	// proposals.go, but we check eagerly here for a clearer error).
	if proposedRule.GetRuleNumber().GetCoordinatorTerm() != rev.GetRevokedBelowTerm() {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"proposed_rule.rule_number.coordinator_term (%d) must equal cert.term_revocation.revoked_below_term (%d)",
			proposedRule.GetRuleNumber().GetCoordinatorTerm(), rev.GetRevokedBelowTerm())
	}
	if rev.GetRevokedBelowTerm() <= rev.GetOutgoingRule().GetCoordinatorTerm() {
		return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
			"cert.term_revocation.revoked_below_term (%d) must be greater than cert.term_revocation.outgoing_rule.coordinator_term (%d)",
			rev.GetRevokedBelowTerm(), rev.GetOutgoingRule().GetCoordinatorTerm())
	}

	// Leader must be in the cohort.
	leaderKey := topoclient.ClusterIDString(proposedRule.GetLeaderId())
	for _, id := range proposedRule.GetCohortMembers() {
		if topoclient.ClusterIDString(id) == leaderKey {
			return nil
		}
	}
	return mterrors.Errorf(mtrpcpb.Code_INVALID_ARGUMENT,
		"proposed_rule.leader_id (%s) must be a member of proposed_rule.cohort_members", leaderKey)
}
