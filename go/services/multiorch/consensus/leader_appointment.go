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

	"github.com/jackc/pglogrepl"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/topoclient"
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
func (c *Coordinator) BeginTerm(ctx context.Context, shardID string, cohort []*multiorchdatapb.PoolerHealthState, quorumRule *clustermetadatapb.QuorumRule, proposedTerm int64) (*multiorchdatapb.PoolerHealthState, []*multiorchdatapb.PoolerHealthState, int64, error) {
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

	// Validate Quorum
	c.logger.InfoContext(ctx, "Validating quorum",
		"shard", shardID,
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount)

	if err := c.ValidateQuorum(quorumRule, cohort, recruitedPoolers); err != nil {
		return nil, nil, 0, mterrors.Wrapf(err, "quorum validation failed for shard %s", shardID)
	}

	// Separate candidate from standbys
	var standbys []*multiorchdatapb.PoolerHealthState
	for _, pooler := range recruitedPoolers {
		if pooler.MultiPooler.Id.Name != candidate.MultiPooler.Id.Name {
			standbys = append(standbys, pooler)
		}
	}

	return candidate, standbys, proposedTerm, nil
}

// discoverMaxTerm finds the maximum consensus term from cached health state.
// This uses the ConsensusTerm data already populated by health checks, avoiding extra RPCs.
func (c *Coordinator) discoverMaxTerm(cohort []*multiorchdatapb.PoolerHealthState) (int64, error) {
	var maxTerm int64

	for _, pooler := range cohort {
		// Invariant: poolers in the cohort with successful health checks must have ConsensusTerm populated
		if pooler.IsLastCheckValid && pooler.ConsensusTerm == nil {
			return 0, mterrors.Errorf(mtrpcpb.Code_INTERNAL,
				"healthy pooler %s in cohort missing consensus term data - health check invariant violated",
				pooler.MultiPooler.Id.Name)
		}

		if pooler.ConsensusTerm != nil && pooler.ConsensusTerm.TermNumber > maxTerm {
			maxTerm = pooler.ConsensusTerm.TermNumber
		}
	}

	// Invariant: at least one pooler in the cohort must have a term > 0
	if maxTerm == 0 {
		return 0, mterrors.Errorf(mtrpcpb.Code_FAILED_PRECONDITION,
			"no poolers in cohort have initialized consensus term - cannot discover max term")
	}

	return maxTerm, nil
}

// selectCandidate chooses the best candidate from recruited poolers.
//
// WAL positions are captured after REVOKE (demote/pause), so they reflect
// the final state and won't advance further.
//
// Selection Strategy (per generalized consensus):
//
// The framework (https://multigres.com/blog/generalized-consensus-part7) requires
// choosing the timeline with the "latest decision" - meaning the most recent
// term/coordinator that wrote to it.
//
// Current Implementation (INCORRECT for multiple failures):
//   - We currently select based on highest LSN. This works for simple failover
//     scenarios with a single timeline.
//   - However, when there are CONFLICTING TIMELINES from multiple failures,
//     LSN alone cannot determine which timeline is most recent.
//   - Problem: LSN measures bytes written, not logical progression.
//     Example: Term 5 writes a large transaction (LSN 1000), Term 6 writes
//     a small transaction (LSN 500). LSN would incorrectly choose Term 5's
//     abandoned timeline over Term 6's. It will incorrectly propagate LSN from Term 5.
//
// Why use Timestamps? (Next Steps)
//   - We follow the "timestamp way" proposal from the blogpost (Part 7).
//   - We use the timestamp of the most recent transaction commit to
//     disambiguate conflicting timelines.
//   - This provides logical ordering across conflicting timelines.
//   - Limitation: Relies on clock synchronization (we accept theoretical
//     clock skew for now).
//   - TODO: Update selectCandidate to use timestamps
//
// Why *NOT* use PrimaryTerm?
//   - We track PrimaryTerm at the pooler/coordinator level (which term the
//     coordinator is operating in).
//   - But we DON'T track which term each individual WAL transaction belongs to.
//   - Without per-transaction term numbers embedded in WAL, we can't determine
//     which transactions came from which term when comparing conflicting logs.
//
// Future Enhancement:
//   - Once we implement two-phase sync, our API will be able to infer
//     which term was associated with a WAL transaction.
//   - Then we can use: highest term first, then highest LSN within that term
//   - This eliminates reliance on timestamps and clock assumptions
func (c *Coordinator) selectCandidate(ctx context.Context, recruited []recruitmentResult) (*multiorchdatapb.PoolerHealthState, error) {
	if len(recruited) == 0 {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			"no recruited poolers available for candidate selection")
	}

	var bestCandidate *multiorchdatapb.PoolerHealthState
	var bestLSN pglogrepl.LSN

	for _, r := range recruited {
		// Extract LSN from WAL position
		// For primary: use current_lsn, for standby: use last_receive_lsn
		lsnStr := ""
		if r.walPosition != nil {
			if r.walPosition.CurrentLsn != "" {
				lsnStr = r.walPosition.CurrentLsn
			} else if r.walPosition.LastReceiveLsn != "" {
				lsnStr = r.walPosition.LastReceiveLsn
			}
		}

		// Skip poolers with no WAL position data
		if lsnStr == "" {
			c.logger.InfoContext(ctx, "Skipping recruited pooler with empty WAL position",
				"pooler", r.pooler.MultiPooler.Id.Name)
			continue
		}

		// Parse and validate LSN
		lsn, err := pglogrepl.ParseLSN(lsnStr)
		if err != nil {
			c.logger.WarnContext(ctx, "Invalid LSN format for recruited pooler, skipping",
				"pooler", r.pooler.MultiPooler.Id.Name,
				"lsn", lsnStr,
				"error", err)
			continue
		}

		// Select node with highest LSN
		if bestCandidate == nil || lsn > bestLSN {
			bestCandidate = r.pooler
			bestLSN = lsn
		}
	}

	if bestCandidate == nil {
		return nil, mterrors.New(mtrpcpb.Code_UNAVAILABLE,
			"no valid candidate found among recruited poolers - all have empty or invalid WAL positions")
	}

	c.logger.InfoContext(ctx, "Selected candidate from recruited nodes",
		"pooler", bestCandidate.MultiPooler.Id.Name,
		"lsn", bestLSN)

	return bestCandidate, nil
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
			resp, err := c.rpcClient.BeginTerm(ctx, n.MultiPooler, req)
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
//  3. Writing leadership history under the new timeline. This write blocks until
//     acknowledged by the quorum, which proves:
//     a) The quorum has replicated the candidate's entire timeline (up to promotion point).
//     b) The quorum has replicated the leadership history write itself.
//     c) The timeline is now durable under the new term.
//
// Once the leadership history write succeeds, the new leader has successfully
// propagated its timeline and established leadership. The leadership table serves
// as the canonical source of truth for when the new term began.
//
// Critical ordering: Standbys MUST be configured BEFORE promotion (step 1) to avoid
// deadlock. Promotion configures sync replication and writes leadership history, which
// blocks waiting for acknowledgments. If standbys aren't replicating yet, the write
// blocks forever.
//
// If we fail to write leadership history, leadership couldn't be established.
// A future coordinator will need to re-discover the most advanced timeline and re-propagate.
func (c *Coordinator) EstablishLeadership(
	ctx context.Context,
	candidate *multiorchdatapb.PoolerHealthState,
	standbys []*multiorchdatapb.PoolerHealthState,
	term int64,
	quorumRule *clustermetadatapb.QuorumRule,
	reason string,
	cohort []*multiorchdatapb.PoolerHealthState,
	recruited []*multiorchdatapb.PoolerHealthState,
) error {
	// Get current WAL position before promotion (for validation)
	statusReq := &consensusdatapb.StatusRequest{}
	status, err := c.rpcClient.ConsensusStatus(ctx, candidate.MultiPooler, statusReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to get candidate status before promotion")
	}

	expectedLSN := ""
	if status.WalPosition != nil {
		if status.Role == "primary" {
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
				if _, err := c.rpcClient.WaitForLSN(ctx, candidate.MultiPooler, waitReq); err != nil {
					return mterrors.Wrapf(err, "candidate failed to replay WAL to %s", expectedLSN)
				}
			}
		}
	}

	// Build lists of cohort member names and accepted member names
	cohortMembers := make([]string, 0, len(cohort))
	for _, pooler := range cohort {
		if pooler.MultiPooler != nil && pooler.MultiPooler.Id != nil {
			cohortMembers = append(cohortMembers, pooler.MultiPooler.Id.Name)
		}
	}

	acceptedMembers := make([]string, 0, len(recruited))
	for _, pooler := range recruited {
		if pooler.MultiPooler != nil && pooler.MultiPooler.Id != nil {
			acceptedMembers = append(acceptedMembers, pooler.MultiPooler.Id.Name)
		}
	}

	// Configure standbys to replicate from the candidate BEFORE promoting.
	// This ensures standbys are ready to connect when sync replication is configured.
	// Without this, the Promote call can deadlock: it configures sync replication and
	// tries to write leadership history, but blocks waiting for standby acknowledgment.
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

			setPrimaryReq := &multipoolermanagerdatapb.SetPrimaryConnInfoRequest{
				Primary:               candidate.MultiPooler,
				CurrentTerm:           term,
				StopReplicationBefore: false,
				StartReplicationAfter: true, // Start streaming immediately so sync replication can proceed
				Force:                 false,
			}
			if _, err := c.rpcClient.SetPrimaryConnInfo(ctx, s.MultiPooler, setPrimaryReq); err != nil {
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

	// Get coordinator ID as a string
	coordinatorIDStr := topoclient.ClusterIDString(c.GetCoordinatorID())

	// Build synchronous replication configuration based on quorum policy
	syncConfig, err := BuildSyncReplicationConfig(c.logger, quorumRule, cohort, candidate)
	if err != nil {
		return mterrors.Wrap(err, "failed to build synchronous replication config")
	}

	promoteReq := &multipoolermanagerdatapb.PromoteRequest{
		ConsensusTerm:         term,
		ExpectedLsn:           expectedLSN,
		SyncReplicationConfig: syncConfig,
		Force:                 false,
		Reason:                reason,
		CoordinatorId:         coordinatorIDStr,
		CohortMembers:         cohortMembers,
		AcceptedMembers:       acceptedMembers,
	}
	_, err = c.rpcClient.Promote(ctx, candidate.MultiPooler, promoteReq)
	if err != nil {
		return mterrors.Wrap(err, "failed to promote candidate")
	}

	c.logger.InfoContext(ctx, "Candidate promoted successfully", "pooler", candidate.MultiPooler.Id.Name)

	return nil
}

// preVote performs a pre-election check to determine if an election is likely to succeed.
// This prevents disruptive elections that would fail due to:
// 1. Insufficient healthy poolers to form a quorum (based on durability policy)
// 2. Another coordinator recently started an election (within last 10 seconds)
//
// Returns (canProceed, reason) where canProceed indicates if election should proceed.
func (c *Coordinator) preVote(ctx context.Context, cohort []*multiorchdatapb.PoolerHealthState, quorumRule *clustermetadatapb.QuorumRule, proposedTerm int64) (bool, string) {
	now := time.Now()
	const recentAcceptanceWindow = 4 * time.Second

	// Check 1: Verify we have enough healthy initialized poolers with consensus term data
	// PreVote is conservative and doesn't handle bootstrap - if poolers lack consensus
	// term information, we can't make an informed decision about election safety.
	// We also skip poolers where postgres is not running, as they cannot participate
	// in an election.
	var healthyInitializedPoolers []*multiorchdatapb.PoolerHealthState
	for _, pooler := range cohort {
		if pooler.IsLastCheckValid && pooler.IsInitialized && pooler.ConsensusTerm != nil && pooler.IsPostgresRunning {
			healthyInitializedPoolers = append(healthyInitializedPoolers, pooler)
		}
	}

	c.logger.InfoContext(ctx, "pre-vote health check",
		"healthy_initialized_poolers", len(healthyInitializedPoolers),
		"total_poolers", len(cohort),
		"quorum_type", quorumRule.QuorumType,
		"required_count", quorumRule.RequiredCount,
		"proposed_term", proposedTerm)

	// Validate we have enough healthy initialized poolers to satisfy quorum
	if err := c.ValidateQuorum(quorumRule, cohort, healthyInitializedPoolers); err != nil {
		return false, fmt.Sprintf("insufficient healthy initialized poolers for quorum: %v", err)
	}

	// Check 2: Has another coordinator recently started an election?
	// If we detect a recent term acceptance (within the last 10 seconds), back off
	// to give the other coordinator a chance to complete their election.
	for _, pooler := range healthyInitializedPoolers {
		// Check if this pooler recently accepted a term from another coordinator
		if pooler.ConsensusTerm.LastAcceptanceTime != nil {
			lastAcceptanceTime := pooler.ConsensusTerm.LastAcceptanceTime.AsTime()
			timeSinceAcceptance := now.Sub(lastAcceptanceTime)

			// If the acceptance was recent (within our window), back off
			if timeSinceAcceptance < recentAcceptanceWindow && timeSinceAcceptance >= 0 {
				c.logger.InfoContext(ctx, "detected recent term acceptance, backing off to avoid disruption",
					"pooler", pooler.MultiPooler.Id.Name,
					"accepted_term", pooler.ConsensusTerm.TermNumber,
					"accepted_from", pooler.ConsensusTerm.AcceptedTermFromCoordinatorId,
					"time_since_acceptance", timeSinceAcceptance,
					"backoff_window", recentAcceptanceWindow)

				return false, fmt.Sprintf("another coordinator started election recently (%v ago), backing off to avoid disruption",
					timeSinceAcceptance.Round(time.Millisecond))
			}
		}
	}

	c.logger.InfoContext(ctx, "pre-vote check passed",
		"proposed_term", proposedTerm,
		"healthy_initialized_poolers", len(healthyInitializedPoolers))

	return true, ""
}
