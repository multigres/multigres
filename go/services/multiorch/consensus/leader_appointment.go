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

package consensus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/timeouts"
	"github.com/multigres/multigres/go/services/multiorch/recovery/types"
	"github.com/multigres/multigres/go/tools/pgutil"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multiorchdatapb "github.com/multigres/multigres/go/pb/multiorchdata"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// BeginTerm achieves Revocation, Candidacy, and Discovery by recruiting poolers
// under the proposed term:
//
//   - Revocation: Recruited poolers accept the new term, preventing any old leader
//     from completing requests under a previous term
//   - Discovery: Identifies the most progressed pooler based on WAL position to serve
//     as the candidate. From Raft: the log with highest term is most progressed;
//     for identical terms, highest LSN is most progressed
//   - Candidacy: Validates that recruited poolers satisfy the quorum rules, ensuring
//     the candidate has sufficient support to proceed
//
// The proposedTerm parameter is the term number to use (computed as maxTerm + 1).
//
// Returns the candidate pooler, standbys that accepted the term, the term, and any error.
func (c *Coordinator) BeginTerm(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, policy commonconsensus.DurabilityPolicy, proposedTerm int64) (*multiorchdatapb.PoolerHealthState, []*multiorchdatapb.PoolerHealthState, int64, error) {
	c.logger.InfoContext(ctx, "Beginning term", "shard", shardID, "term", proposedTerm)

	// Recruit Nodes - Send BeginTerm RPC to all poolers in parallel
	// This is now FIRST to ensure we only select from nodes that accept the term
	recruited, err := c.recruitNodes(ctx, cohort, proposedTerm, consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_REVOKE)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to recruit poolers")
	}

	c.logger.InfoContext(ctx, "Recruited poolers", "shard", shardID, "count", len(recruited))

	if len(recruited) == 0 {
		return nil, nil, 0, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			"no poolers accepted the term")
	}

	candidate, err := c.selectCandidate(ctx, recruited)
	if err != nil {
		return nil, nil, 0, mterrors.Wrap(err, "failed to select candidate from recruited nodes")
	}

	c.logger.InfoContext(ctx, "Selected candidate from recruited nodes",
		"shard", shardID,
		"candidate", candidate.MultiPooler.Id.Name)

	// Extract PoolerHealthState list for quorum validation
	recruitedPoolers := make([]*multiorchdatapb.PoolerHealthState, 0, len(recruited))
	for _, r := range recruited {
		recruitedPoolers = append(recruitedPoolers, r.pooler)
	}

	// Validate recruitment: proposal-agnostic invariants (revocation + majority)
	// plus the candidacy check for this failover (does the recruited set satisfy
	// the durability policy's quorum?). Candidacy is proposal-specific and
	// lives at this layer rather than inside the policy method.
	c.logger.InfoContext(ctx, "Validating recruitment",
		"shard", shardID,
		"policy", policy.Description())

	recruitedIDs := poolerIDs(recruitedPoolers)
	if err := policy.CheckSufficientRecruitment(poolerIDs(cohort), recruitedIDs); err != nil {
		return nil, nil, 0, mterrors.Wrapf(err, "recruitment validation failed for shard %s", shardID)
	}
	if err := policy.CheckAchievable(recruitedIDs); err != nil {
		return nil, nil, 0, mterrors.Wrapf(err, "candidacy validation failed for shard %s", shardID)
	}

	// Resigned poolers are excluded: they may still carry a stale primary rule
	// that needs pg_rewind, which the stale-primary analyzer owns.
	// TODO: once poolers self-rewind after emergency demotion, this exclusion
	// becomes unnecessary — the resigned pooler can rejoin as a standby directly.
	var standbys []*multiorchdatapb.PoolerHealthState
	for _, pooler := range recruitedPoolers {
		if pooler.MultiPooler.Id.Name == candidate.MultiPooler.Id.Name {
			continue
		}
		if types.PrimaryNeedsReplacement(pooler) {
			c.logger.InfoContext(ctx, "Skipping resigned pooler from standbys",
				"pooler", pooler.MultiPooler.Id.Name)
			continue
		}
		standbys = append(standbys, pooler)
	}

	return candidate, standbys, proposedTerm, nil
}

// discoverMaxTerm finds the maximum consensus term from cached health state.
// This uses the ConsensusTerm data already populated by health checks, avoiding extra RPCs.
func (c *Coordinator) discoverMaxTerm(cohort []*multiorchdatapb.PoolerHealthState) (int64, error) {
	var maxTerm int64

	for _, pooler := range cohort {
		// Invariant: poolers in the cohort with successful health checks must have TermRevocation populated
		if pooler.IsLastCheckValid && pooler.GetStatus().GetTermRevocation() == nil {
			return 0, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"healthy pooler %s in cohort missing consensus term data - health check invariant violated",
				pooler.MultiPooler.Id.Name)
		}

		if ct := pooler.GetStatus().GetTermRevocation(); ct != nil && ct.RevokedBelowTerm > maxTerm {
			maxTerm = ct.RevokedBelowTerm
		}
	}

	// Invariant: at least one pooler in the cohort must have a term > 0
	if maxTerm == 0 {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no poolers in cohort have initialized consensus term - cannot discover max term")
	}

	return maxTerm, nil
}

// walPositionLSN extracts and parses the most relevant LSN from a WALPosition.
// For a primary node CurrentLsn is used; for a standby, LastReceiveLsn.
// Returns (0, false) when pos is nil, both LSN fields are empty, or the string
// cannot be parsed.
func walPositionLSN(pos *consensusdatapb.WALPosition) (pgutil.LSN, bool) {
	if pos == nil {
		return 0, false
	}

	// Get either the CurrentLsn (for primaries) or LastReceiveLsn (for
	// standbys). Both field may be empty if the server is not a primary or
	// standby, or if the pooler hasn't fully initialized and fetched WAL
	// position yet.
	// Use the most advanced LSN available: current (primary), receive (streaming standby),
	// or replay (standby that has replayed from backup but not yet streaming).
	lsnStr := pos.CurrentLsn
	if lsnStr == "" {
		lsnStr = pos.LastReceiveLsn
	}
	if lsnStr == "" {
		lsnStr = pos.LastReplayLsn
	}

	// If all LSN fields are empty, we consider the WAL position invalid for
	// selection purposes.
	if lsnStr == "" {
		return 0, false
	}

	lsn, err := pgutil.ParseLSN(lsnStr)
	if err != nil {
		return 0, false
	}

	return lsn, true
}

// selectCandidate chooses the best candidate from recruited poolers.
//
// WAL positions are captured after REVOKE (demote/pause), so they reflect the
// final state and won't advance further.
//
// Selection Strategy (per generalized consensus):
//
// Two-level ordering: leadership_term → LSN.
//
// 1. Highest leadership_term (primary criterion).
//
//	Each promotion and replication-config change writes a record to
//	multigres.rule_history with the current consensus term, using
//	RemoteOperationTimeout so synchronous standbys acknowledge the write
//	before the primary returns. The most recent coordinator_term in a standby's
//	local rule_history therefore reflects how far through the agreed
//	consensus history that standby has replicated. A standby with a higher
//	leadership_term has definitively applied more of the cluster's
//	committed WAL history than one with a lower term.
//
//	A stranded standby that diverged before a term-N promotion write was
//	replicated may accumulate a numerically larger LSN (e.g. a large
//	transaction written just before its primary crashed), but its
//	leadership_term will be lower, correctly excluding it.
//
//	Falls back to 0 when rule_history is empty (pre-bootstrap), in
//	which case the secondary criteria (LSN) determine the winner.
//
// 2. Highest LSN (secondary tiebreaker).
//
//	When terms are identical, LSN measures genuine progress
//	within the same history, so the node with the highest LSN is selected.
//
// Example (why LSN-only is wrong):
//
//	mp1: term=1, LSN=0/5000000  ← stranded, high LSN, old term
//	mp2: term=2, LSN=0/3000000  ← current term
//
// LSN-only would incorrectly elect mp1. Term-first correctly elects mp2.
func (c *Coordinator) selectCandidate(ctx context.Context, recruited []recruitmentResult) (*multiorchdatapb.PoolerHealthState, error) {
	if len(recruited) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			"no recruited poolers available for candidate selection")
	}

	var bestRecruit *recruitmentResult
	var bestLSN pgutil.LSN

	for i := range recruited {
		r := &recruited[i]

		// Skip nodes that have voluntarily requested demotion. The resignation
		// signal is a deliberate request not to be re-elected; honoring it
		// unconditionally avoids confusing re-elections of a node that just
		// stepped down. If all candidates are resigned the election is deferred
		// until a non-resigned candidate is available.
		if types.PrimaryNeedsReplacement(r.pooler) {
			c.logger.InfoContext(ctx, "Skipping resigned candidate during selection",
				"pooler", r.pooler.MultiPooler.Id.Name)
			continue
		}

		lsn, ok := walPositionLSN(r.walPosition)
		if !ok {
			c.logger.WarnContext(ctx, "Skipping recruited pooler with missing or invalid WAL position",
				"pooler", r.pooler.MultiPooler.Id.Name)
			continue
		}

		var leadershipTerm int64
		if r.walPosition != nil {
			leadershipTerm = r.walPosition.LeadershipTerm
		}

		var bestLeadershipTerm int64
		if bestRecruit != nil && bestRecruit.walPosition != nil {
			bestLeadershipTerm = bestRecruit.walPosition.LeadershipTerm
		}

		// Select by: highest leadership_term, then highest LSN.
		if bestRecruit == nil ||
			leadershipTerm > bestLeadershipTerm ||
			(leadershipTerm == bestLeadershipTerm && lsn > bestLSN) {
			bestRecruit = r
			bestLSN = lsn
		}
	}

	if bestRecruit == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			"no valid candidate found among recruited poolers - all have empty or invalid WAL positions")
	}

	// Log candidate selection details for observability. timeline_id is printed
	// for debugging purposes but does not factor into selection criteria.
	var timelineID int64
	if bestRecruit.walPosition != nil {
		timelineID = bestRecruit.walPosition.TimelineId
	}
	c.logger.InfoContext(ctx, "Selected candidate from recruited nodes",
		"pooler", bestRecruit.pooler.MultiPooler.Id.Name,
		"leadership_term", bestRecruit.walPosition.GetLeadershipTerm(),
		"timeline_id", timelineID,
		"lsn", bestLSN)

	return bestRecruit.pooler, nil
}

// selectCandidateFromRecruited chooses the best candidate from recruited nodes based on WAL position.
// Selection criteria: prefer the node with the highest LSN.
// recruitmentResult captures recruitment outcome and WAL position from BeginTerm response
type recruitmentResult struct {
	pooler      *multiorchdatapb.PoolerHealthState
	walPosition *consensusdatapb.WALPosition
}

// recruitNodes sends BeginTerm RPC to all poolers in parallel and returns those that accepted.
func (c *Coordinator) recruitNodes(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, term int64, action consensusdatapb.BeginTermAction) ([]recruitmentResult, error) {
	type result struct {
		pooler      *multiorchdatapb.PoolerHealthState
		accepted    bool
		walPosition *consensusdatapb.WALPosition
		err         error
	}

	results := make(chan result, len(cohort))
	var wg sync.WaitGroup

	for _, pooler := range cohort {
		wg.Add(1)
		go func(n *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			req := &consensusdatapb.BeginTermRequest{
				Term:        term,
				CandidateId: c.coordinatorID,
				ShardId:     n.MultiPooler.Shard,
				Action:      action,
			}
			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()
			resp, err := c.rpcClient.BeginTerm(rpcCtx, n.MultiPooler, req)
			if err != nil {
				results <- result{pooler: n, err: err}
				return
			}
			results <- result{
				pooler:      n,
				accepted:    resp.Accepted,
				walPosition: resp.WalPosition,
			}
		}(pooler)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect accepted poolers with WAL position data
	var recruited []recruitmentResult
	for r := range results {
		if r.err != nil {
			c.logger.WarnContext(ctx, "BeginTerm failed for pooler",
				"pooler", r.pooler.MultiPooler.Id.Name,
				"error", r.err)
			continue
		}
		if r.accepted {
			recruited = append(recruited, recruitmentResult{
				pooler:      r.pooler,
				walPosition: r.walPosition,
			})

			// Log LSN from WAL position
			lsn := ""
			if r.walPosition != nil {
				if r.walPosition.CurrentLsn != "" {
					lsn = r.walPosition.CurrentLsn
				} else if r.walPosition.LastReceiveLsn != "" {
					lsn = r.walPosition.LastReceiveLsn
				}
			}

			c.logger.InfoContext(ctx, "Pooler accepted term",
				"pooler", r.pooler.MultiPooler.Id.Name,
				"term", term,
				"lsn", lsn)
		} else {
			c.logger.WarnContext(ctx, "Pooler rejected term",
				"pooler", r.pooler.MultiPooler.Id.Name,
				"term", term)
		}
	}

	return recruited, nil
}

// EstablishLeadership achieves the Propagation and Establishment goals from the consensus model:
// It makes the candidate's timeline durable under the new term and establishes leadership.
//
// In consensus terminology:
// - Propagation: Ensuring the recruited quorum has the candidate's complete timeline
// - Establishment: Delegating the term to the candidate so it can begin accepting requests
//
// This is accomplished by:
//  1. Configuring standbys to replicate from the candidate (before promotion)
//  2. Promoting the candidate to primary with synchronous replication configured.
//  3. Writing rule history under the new timeline. This write blocks until
//     acknowledged by the quorum, which proves:
//     a) The quorum has replicated the candidate's entire timeline (up to promotion point).
//     b) The quorum has replicated the rule history write itself.
//     c) The timeline is now durable under the new term.
//
// Once the rule history write succeeds, the new leader has successfully
// propagated its timeline and established leadership. The rule_history table serves
// as the canonical source of truth for when the new term began.
//
// Critical ordering: Standbys MUST be configured BEFORE promotion (step 1) to avoid
// deadlock. Promotion configures sync replication and writes rule history, which
// blocks waiting for acknowledgments. If standbys aren't replicating yet, the write
// blocks forever.
//
// If we fail to write rule history, leadership couldn't be established.
// A future coordinator will need to re-discover the most advanced timeline and re-propagate.
func (c *Coordinator) EstablishLeadership(
	ctx context.Context,
	candidate *multiorchdatapb.PoolerHealthState,
	standbys []*multiorchdatapb.PoolerHealthState,
	term int64,
	policy *clustermetadatapb.DurabilityPolicy,
	reason string,
	cohort []*multiorchdatapb.PoolerHealthState,
	recruited []*multiorchdatapb.PoolerHealthState,
) error {
	// Get current WAL position before promotion (for validation)
	statusReq := &consensusdatapb.StatusRequest{}
	statusCtx, statusCancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer statusCancel()
	status, err := c.rpcClient.ConsensusStatus(statusCtx, candidate.MultiPooler, statusReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to get candidate status before promotion")
	}

	expectedLSN := ""
	if status.WalPosition != nil {
		if status.Role == consensusdatapb.PostgresRole_POSTGRES_ROLE_PRIMARY {
			expectedLSN = status.WalPosition.CurrentLsn
		} else {
			// For standbys, use receive position (includes unreplayed WAL)
			expectedLSN = status.WalPosition.LastReceiveLsn

			// Wait for standby to replay all received WAL before promotion
			// This ensures validateExpectedLSN in Promote will pass
			if expectedLSN != "" {
				c.logger.InfoContext(ctx, "Waiting for candidate to replay all received WAL",
					"pooler", candidate.MultiPooler.Id.Name,
					"target_lsn", expectedLSN)

				waitReq := &multipoolermanagerdatapb.WaitForLSNRequest{
					TargetLsn: expectedLSN,
				}
				waitCtx, waitCancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
				defer waitCancel()
				if _, err := c.rpcClient.WaitForLSN(waitCtx, candidate.MultiPooler, waitReq); err != nil {
					return mterrors.Wrapf(err, "candidate failed to replay WAL to %s", expectedLSN)
				}
			}
		}
	}

	// Build lists of cohort member IDs and accepted member IDs
	cohortMembers := make([]*clustermetadatapb.ID, 0, len(cohort))
	for _, pooler := range cohort {
		if pooler.MultiPooler != nil && pooler.MultiPooler.Id != nil {
			cohortMembers = append(cohortMembers, pooler.MultiPooler.Id)
		}
	}

	acceptedMembers := make([]*clustermetadatapb.ID, 0, len(recruited))
	for _, pooler := range recruited {
		if pooler.MultiPooler != nil && pooler.MultiPooler.Id != nil {
			acceptedMembers = append(acceptedMembers, pooler.MultiPooler.Id)
		}
	}

	// Configure standbys to replicate from the candidate BEFORE promoting.
	// This ensures standbys are ready to connect when sync replication is configured.
	// Without this, the Promote call can deadlock: it configures sync replication and
	// tries to write rule history, but blocks waiting for standby acknowledgment.
	// The standbys can't acknowledge because they haven't been told to replicate yet.
	var wg sync.WaitGroup
	errChan := make(chan error, len(standbys))

	for _, standby := range standbys {
		wg.Add(1)
		go func(s *multiorchdatapb.PoolerHealthState) {
			defer wg.Done()
			c.logger.InfoContext(ctx, "Configuring standby replication before promotion",
				"standby", s.MultiPooler.Id.Name,
				"primary", candidate.MultiPooler.Id.Name)

			rpcCtx, cancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
			defer cancel()

			setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
				Primary:               candidate.MultiPooler,
				CurrentTerm:           term,
				StopReplicationBefore: false,
				StartReplicationAfter: true, // Start streaming immediately so sync replication can proceed
				Force:                 false,
			}
			if _, err := c.rpcClient.SetPrimaryConnInfo(rpcCtx, s.MultiPooler, setPrimaryReq); err != nil {
				errChan <- mterrors.Wrapf(err, "failed to configure standby %s", s.MultiPooler.Id.Name)
				return
			}

			c.logger.InfoContext(ctx, "Standby configured successfully", "standby", s.MultiPooler.Id.Name)
		}(standby)
	}

	wg.Wait()
	close(errChan)

	// Check for errors - log but don't fail, standbys can be fixed later
	var standbyErrs []error
	for err := range errChan {
		standbyErrs = append(standbyErrs, err)
	}
	for _, err := range standbyErrs {
		c.logger.WarnContext(ctx, "Standby configuration failed", "error", err)
	}

	// Build synchronous replication configuration based on quorum policy
	syncConfig, err := BuildSyncReplicationConfig(c.logger, policy, cohort, candidate)
	if err != nil {
		return mterrors.Wrap(err, "failed to build synchronous replication config")
	}

	promoteReq := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
		Reason:                reason,
		CoordinatorId:         c.GetCoordinatorID(),
		CohortMembers:         cohortMembers,
		AcceptedMembers:       acceptedMembers,
	}
	promoteCtx, promoteCancel := context.WithTimeout(ctx, timeouts.RemoteOperationTimeout)
	defer promoteCancel()
	_, err = c.rpcClient.Promote(promoteCtx, candidate.MultiPooler, promoteReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to promote candidate")
	}

	c.logger.InfoContext(ctx, "Candidate promoted successfully", "pooler", candidate.MultiPooler.Id.Name)

	return nil
}

// preVote performs a pre-election check to decide whether an election is
// likely to succeed. It prevents disruptive elections that would fail due to:
//  1. Not enough currently reachable poolers to achieve a valid recruitment
//     (candidacy + revocation) under the durability policy.
//  2. Another coordinator recently started an election (within last 10 seconds).
//
// Returns (canProceed, reason) where canProceed indicates if election should proceed.
func (c *Coordinator) preVote(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, policy commonconsensus.DurabilityPolicy, proposedTerm int64) (bool, string) {
	now := time.Now()
	const recentAcceptanceWindow = 4 * time.Second

	// Filter cohort to poolers eligible to participate in recruitment right now.
	// A pooler is eligible only if we can reach it, it's initialized with
	// consensus-term data, and its postgres is running. Without consensus-term
	// info we can't reason about election safety; without postgres the pooler
	// can't participate.
	var eligiblePoolers []*multiorchdatapb.PoolerHealthState
	for _, pooler := range cohort {
		status := pooler.GetStatus()
		if pooler.IsLastCheckValid && status.GetIsInitialized() && status.GetTermRevocation() != nil && status.GetPostgresReady() {
			eligiblePoolers = append(eligiblePoolers, pooler)
		}
	}

	c.logger.InfoContext(ctx, "pre-vote health check",
		"eligible_poolers", len(eligiblePoolers),
		"total_poolers", len(cohort),
		"policy", policy.Description(),
		"proposed_term", proposedTerm)

	// If we attempted recruitment right now with the eligible poolers, would
	// the result be sufficient (candidacy + revocation) under the policy?
	// If not, abort early rather than disrupt the cluster with a doomed election.
	eligibleIDs := poolerIDs(eligiblePoolers)
	if err := policy.CheckSufficientRecruitment(poolerIDs(cohort), eligibleIDs); err != nil {
		return false, fmt.Sprintf("not enough eligible poolers to achieve valid recruitment: %v", err)
	}
	if err := policy.CheckAchievable(eligibleIDs); err != nil {
		return false, fmt.Sprintf("not enough eligible poolers to achieve valid recruitment: %v", err)
	}

	// Check 2: Has another coordinator recently started an election?
	// If we detect a recent term acceptance (within the last 10 seconds), back off
	// to give the other coordinator a chance to complete their election.
	for _, pooler := range eligiblePoolers {
		// Check if this pooler recently accepted a term from another coordinator
		if ct := pooler.GetStatus().GetTermRevocation(); ct != nil && ct.CoordinatorInitiatedAt != nil {
			lastAcceptanceTime := ct.CoordinatorInitiatedAt.AsTime()
			timeSinceAcceptance := now.Sub(lastAcceptanceTime)

			// If the acceptance was recent (within our window), back off
			if timeSinceAcceptance < recentAcceptanceWindow && timeSinceAcceptance >= 0 {
				c.logger.InfoContext(ctx, "detected recent term acceptance, backing off to avoid disruption",
					"pooler", pooler.MultiPooler.Id.Name,
					"accepted_term", ct.RevokedBelowTerm,
					"accepted_from", ct.AcceptedCoordinatorId,
					"time_since_acceptance", timeSinceAcceptance,
					"backoff_window", recentAcceptanceWindow)

				return false, fmt.Sprintf("another coordinator started election recently (%v ago), backing off to avoid disruption",
					timeSinceAcceptance.Round(time.Millisecond))
			}
		}
	}

	c.logger.InfoContext(ctx, "pre-vote check passed",
		"proposed_term", proposedTerm,
		"eligible_poolers", len(eligiblePoolers))

	return true, ""
}

// poolerIDs extracts the clustermetadata IDs from a slice of PoolerHealthState.
// Used at the boundary where poolers cross into the durability-policy layer,
// which operates on bare *clustermetadatapb.ID values.
func poolerIDs(poolers []*multiorchdatapb.PoolerHealthState) []*clustermetadatapb.ID {
	out := make([]*clustermetadatapb.ID, len(poolers))
	for i, p := range poolers {
		out[i] = p.MultiPooler.Id
	}
	return out
}
