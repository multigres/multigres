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

package multiadmin

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonconsensus "github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/topoclient"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	multiorchpb "github.com/multigres/multigres/go/pb/multiorch"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
)

// ApplyCertifiedRuleChange installs a new shard rule using an externally
// certified revocation. Multiadmin is a convenience layer over the multiorch
// RPC: it (1) optionally derives outgoing_rule_number and frozen_lsn by
// probing the proposed cohort, (2) picks an orch and fills in any identity
// or timing fields the caller omitted, and (3) forwards the fully-populated
// request to the chosen multiorch.
func (s *MultiadminServer) ApplyCertifiedRuleChange(ctx context.Context, req *multiadminpb.ApplyCertifiedRuleChangeRequest) (*multiadminpb.ApplyCertifiedRuleChangeResponse, error) {
	if req.GetShardKey() == nil {
		return nil, status.Error(codes.InvalidArgument, "shard_key is required")
	}
	if req.GetProposedTransition().GetProposal() == nil {
		return nil, status.Error(codes.InvalidArgument, "proposed_transition.proposal is required")
	}
	if req.GetCertSource() == nil {
		return nil, status.Error(codes.InvalidArgument, "exactly one of cert or unsafe_derive_cert must be set")
	}

	orch, err := s.pickOrch(ctx, req.GetProposedTransition().GetProposal().GetLeaderId())
	if err != nil {
		return nil, err
	}

	proposedRule := proto.Clone(req.GetProposedTransition().GetProposal()).(*clustermetadatapb.ShardRule)
	decision, cert, err := s.buildCert(ctx, req, proposedRule)
	if err != nil {
		return nil, err
	}

	now := timestamppb.Now()
	if err := fillIdentityFields(proposedRule, cert, orch.Id, now); err != nil {
		return nil, err
	}

	conn, err := s.dialOrch(ctx, orch)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if _, err := multiorchpb.NewMultiorchServiceClient(conn).ApplyCertifiedRuleChange(ctx, &multiorchpb.ApplyCertifiedRuleChangeRequest{
		ShardKey:           req.GetShardKey(),
		ProposedTransition: &clustermetadatapb.RulePosition{Decision: decision, Proposal: proposedRule},
		Cert:               cert,
		Reason:             req.GetReason(),
	}); err != nil {
		s.logger.ErrorContext(ctx, "multiorch ApplyCertifiedRuleChange failed",
			"orch", topoclient.ClusterIDString(orch.Id), "error", err)
		return nil, err
	}

	return &multiadminpb.ApplyCertifiedRuleChangeResponse{
		InstalledRule: proposedRule,
		CertUsed:      cert,
	}, nil
}

// pickOrch returns a multiorch to forward the rule change to. It prefers an
// orch in the same cell as the proposed leader (cross-cell traffic during
// Recruit/Promote is wasteful) and falls back to any orch in any other cell.
//
// Per-cell listing errors are tolerated as long as we find at least one
// orch somewhere — they only surface if no orch is found in any cell, in
// which case they are included in the error message.
//
// Until orchs publish their watch_targets in topology, multiadmin accepts
// whatever multiorch it finds and lets the orch reject with NotFound if it
// does not actually watch the shard.
//
// TODO(before launch): the chosen orch might be registered but not
// responding, leaving the caller with a non-recoverable error. Options to
// address: (a) gRPC client-side load balancing that tries each orch in turn,
// or (b) expose an optional orch_id parameter on the request so a caller
// who already knows a healthy orch can pin it. Out of scope for this PR.
func (s *MultiadminServer) pickOrch(ctx context.Context, leaderID *clustermetadatapb.ID) (*clustermetadatapb.Multiorch, error) {
	cellNames, err := s.ts.GetCellNames(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list cells: %v", err)
	}

	// Try the leader's cell first, then the rest. Per-cell errors are
	// logged but not fatal; we only fail if no cell yields an orch.
	preferredCell := leaderID.GetCell()
	cells := make([]string, 0, len(cellNames))
	if preferredCell != "" {
		cells = append(cells, preferredCell)
	}
	for _, c := range cellNames {
		if c != preferredCell {
			cells = append(cells, c)
		}
	}

	var lastErr error
	for _, cell := range cells {
		orchs, err := s.ts.GetMultiorchsByCell(ctx, cell)
		if err != nil {
			s.logger.WarnContext(ctx, "failed to list orchs in cell while picking one",
				"cell", cell, "error", err)
			lastErr = err
			continue
		}
		for _, o := range orchs {
			if o.Multiorch != nil {
				return o.Multiorch, nil
			}
		}
	}
	if lastErr != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no multiorch found in any reachable cell; last error: %v", lastErr)
	}
	return nil, status.Error(codes.FailedPrecondition, "no multiorch registered in any cell")
}

// dialOrch opens a gRPC connection to a multiorch using its grpc port.
func (s *MultiadminServer) dialOrch(ctx context.Context, orch *clustermetadatapb.Multiorch) (*grpc.ClientConn, error) {
	port, ok := orch.PortMap["grpc"]
	if !ok || port <= 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"multiorch %s has no grpc port registered", topoclient.ClusterIDString(orch.Id))
	}
	target := fmt.Sprintf("%s:%d", orch.Hostname, port)
	conn, err := s.gatewayDialer(ctx, target)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable,
			"failed to dial multiorch %s: %v", topoclient.ClusterIDString(orch.Id), err)
	}
	return conn, nil
}

// buildCert returns the outgoing decision and ExternallyCertifiedRevocation
// to forward. For an explicit cert, the caller already states the outgoing
// rule number on cert.term_revocation.outgoing_rule — multiadmin derives
// decision from that directly rather than requiring the caller to separately
// populate proposed_transition.decision with a matching value (multiadmin
// isn't bound by multiorch's API's need to stay a pure, theoretically-sound
// executor here, so it can smooth over this redundancy for callers). For
// unsafe_derive_cert, multiadmin discovers both itself by probing the
// proposed cohort and computing term_revocation.outgoing_rule and frozen_lsn
// from the most-advanced response.
func (s *MultiadminServer) buildCert(
	ctx context.Context,
	req *multiadminpb.ApplyCertifiedRuleChangeRequest,
	proposedRule *clustermetadatapb.ShardRule,
) (*clustermetadatapb.ShardRule, *clustermetadatapb.ExternallyCertifiedRevocation, error) {
	switch cs := req.GetCertSource().(type) {
	case *multiadminpb.ApplyCertifiedRuleChangeRequest_Cert:
		if cs.Cert == nil {
			return nil, nil, status.Error(codes.InvalidArgument, "cert source is empty")
		}
		cert := proto.Clone(cs.Cert).(*clustermetadatapb.ExternallyCertifiedRevocation)
		outgoingRule := cert.GetTermRevocation().GetOutgoingRule()
		if outgoingRule == nil {
			return nil, nil, status.Error(codes.InvalidArgument, "cert.term_revocation.outgoing_rule is required")
		}
		decision, err := s.findRuleByNumber(ctx, proposedRule.GetCohortMembers(), outgoingRule)
		if err != nil {
			return nil, nil, err
		}
		return decision, cert, nil

	case *multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert:
		pos, err := s.probeMostAdvanced(ctx, proposedRule.GetCohortMembers(), proposedRule.GetDurabilityPolicy())
		if err != nil {
			return nil, nil, err
		}
		// Normally the outgoing rule is a decision, but for externally-certified
		// changes it can be an undecided proposal that we'll be able to instantly
		// "propagate" since outgoing cohorts aren't required to reach quorum for
		// externally-certified rule changes.
		decision := commonconsensus.PossiblyUndecidedRule(pos.GetPosition())
		return decision, &clustermetadatapb.ExternallyCertifiedRevocation{
			TermRevocation: &clustermetadatapb.TermRevocation{
				OutgoingRule: decision.GetRuleNumber(),
			},
			FrozenLsn: pos.GetFlushedLsn(),
		}, nil

	default:
		return nil, nil, status.Error(codes.InvalidArgument, "unknown cert_source variant")
	}
}

// probedPosition pairs a cohort member's ID with the PoolerPosition observed
// from its Status RPC.
type probedPosition struct {
	id  *clustermetadatapb.ID
	pos *clustermetadatapb.PoolerPosition
}

// probeCohort calls MultipoolerManager.Status on every cohort member and
// returns the positions observed from the reachable subset, alongside the
// total cohort size (for quorum checks by callers that need one).
//
// Hard failure: a cohort member ID that does not resolve in topology — we
// refuse to silently exclude it.
//
// Soft failures: individual RPC errors, or a pooler reporting no current
// position, are logged and skipped; callers decide whether the reachable
// subset is sufficient for their purpose.
func (s *MultiadminServer) probeCohort(
	ctx context.Context,
	cohortMembers []*clustermetadatapb.ID,
) ([]probedPosition, int, error) {
	if len(cohortMembers) == 0 {
		return nil, 0, status.Error(codes.InvalidArgument, "cohort_members is required")
	}

	// Resolve every cohort member up front. A missing pooler is a hard
	// failure — proceeding would mean deriving from a strict subset of the
	// cohort that the caller did not opt into.
	poolers := make([]*clustermetadatapb.Multipooler, 0, len(cohortMembers))
	for _, id := range cohortMembers {
		info, err := s.ts.GetMultipooler(ctx, id)
		if err != nil {
			return nil, 0, status.Errorf(codes.NotFound, "pooler %s not found in topology: %v",
				topoclient.ClusterIDString(id), err)
		}
		poolers = append(poolers, info.Multipooler)
	}

	type probeResult struct {
		pooler *clustermetadatapb.Multipooler
		pos    *clustermetadatapb.PoolerPosition
		err    error
	}
	results := make(chan probeResult, len(poolers))
	for _, p := range poolers {
		go func() {
			resp, err := s.rpcClient.Status(ctx, p, &multipoolermanagerdatapb.StatusRequest{})
			if err != nil {
				results <- probeResult{pooler: p, err: err}
				return
			}
			results <- probeResult{pooler: p, pos: resp.GetConsensusStatus().GetCurrentPosition()}
		}()
	}

	var reachable []probedPosition
	for range poolers {
		r := <-results
		if r.err != nil {
			s.logger.WarnContext(ctx, "Status probe failed",
				"pooler", topoclient.ClusterIDString(r.pooler.GetId()), "error", r.err)
			continue
		}
		if r.pos == nil {
			s.logger.WarnContext(ctx, "pooler returned no current position; skipping",
				"pooler", topoclient.ClusterIDString(r.pooler.GetId()))
			continue
		}
		reachable = append(reachable, probedPosition{id: r.pooler.GetId(), pos: r.pos})
	}
	return reachable, len(poolers), nil
}

// probeMostAdvanced calls MultipoolerManager.Status on every proposed cohort
// member and returns the highest position observed across the reachable
// subset (by ComparePoolerPosition — decision, then proposal, then LSN).
//
// Insufficient responses is a hard failure: per
// commonconsensus.CheckSufficientRecruitment, the reachable subset must be
// enough to satisfy the new rule's quorum. If not, the cert we'd derive
// could be missing the most-advanced node in the proposed cohort.
func (s *MultiadminServer) probeMostAdvanced(
	ctx context.Context,
	cohortMembers []*clustermetadatapb.ID,
	durabilityPolicy *clustermetadatapb.DurabilityPolicy,
) (*clustermetadatapb.PoolerPosition, error) {
	policy, err := commonconsensus.NewPolicyFromProto(durabilityPolicy)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid durability_policy: %v", err)
	}
	probed, total, err := s.probeCohort(ctx, cohortMembers)
	if err != nil {
		return nil, err
	}

	var best *clustermetadatapb.PoolerPosition
	reachable := make([]*clustermetadatapb.ID, 0, len(probed))
	for _, p := range probed {
		reachable = append(reachable, p.id)
		if best == nil || commonconsensus.ComparePoolerPosition(p.pos, best) > 0 {
			best = p.pos
		}
	}

	// Require a quorum of the proposed cohort to respond. Without that,
	// the derived cert could understate the cohort's most-advanced position.
	if err := commonconsensus.CheckSufficientRecruitment(policy, cohortMembers, reachable); err != nil {
		return nil, status.Errorf(codes.Unavailable, "insufficient cohort responses to derive cert: %v", err)
	}

	s.logger.InfoContext(ctx, "derived cert from reachable cohort",
		"reachable", len(reachable), "total", total)

	frozenLSN := best.GetFlushedLsn()
	if frozenLSN == "" {
		return nil, status.Error(codes.Unavailable, "most-advanced cohort member reported an empty LSN")
	}

	return best, nil
}

// findRuleByNumber probes every cohort member and returns the full ShardRule
// (decision or, if undecided, proposal — via PossiblyUndecidedRule) whose
// rule number matches target. Used to look up the real content behind a
// caller-supplied cert.term_revocation.outgoing_rule, rather than fabricating
// a ShardRule with only the rule number populated: a single reachable cohort
// member reporting it is sufficient evidence (unlike probeMostAdvanced, no
// quorum is required here — outgoing_rule is already externally certified by
// the caller, this just fills in its cohort/policy/leader details).
func (s *MultiadminServer) findRuleByNumber(
	ctx context.Context,
	cohortMembers []*clustermetadatapb.ID,
	target *clustermetadatapb.RuleNumber,
) (*clustermetadatapb.ShardRule, error) {
	probed, _, err := s.probeCohort(ctx, cohortMembers)
	if err != nil {
		return nil, err
	}
	for _, p := range probed {
		rule := commonconsensus.PossiblyUndecidedRule(p.pos.GetPosition())
		if commonconsensus.CompareRuleNumbers(rule.GetRuleNumber(), target) == 0 {
			return rule, nil
		}
	}
	return nil, status.Errorf(codes.NotFound,
		"no reachable cohort member reports rule %s (matching cert.term_revocation.outgoing_rule)",
		commonconsensus.FormatRuleNumber(target))
}

// fillIdentityFields populates any identity / timing fields the caller left
// unset on proposedRule or cert. Fields the caller provided are validated
// for consistency with the chosen orch but otherwise preserved.
func fillIdentityFields(
	proposedRule *clustermetadatapb.ShardRule,
	cert *clustermetadatapb.ExternallyCertifiedRevocation,
	orchID *clustermetadatapb.ID,
	now *timestamppb.Timestamp,
) error {
	rev := cert.GetTermRevocation()
	if rev == nil {
		rev = &clustermetadatapb.TermRevocation{}
		cert.TermRevocation = rev
	}
	if rev.GetAcceptedCoordinatorId() == nil {
		rev.AcceptedCoordinatorId = orchID
	} else if !proto.Equal(rev.GetAcceptedCoordinatorId(), orchID) {
		return status.Errorf(codes.InvalidArgument,
			"cert.term_revocation.accepted_coordinator_id (%s) does not match chosen multiorch (%s)",
			topoclient.ClusterIDString(rev.GetAcceptedCoordinatorId()), topoclient.ClusterIDString(orchID))
	}
	if rev.GetCoordinatorInitiatedAt() == nil {
		rev.CoordinatorInitiatedAt = now
	}
	if rev.GetRevokedBelowTerm() == 0 {
		rev.RevokedBelowTerm = rev.GetOutgoingRule().GetCoordinatorTerm() + 1
	}

	if proposedRule.GetRuleNumber() == nil {
		proposedRule.RuleNumber = &clustermetadatapb.RuleNumber{CoordinatorTerm: rev.GetRevokedBelowTerm()}
	} else if proposedRule.GetRuleNumber().GetCoordinatorTerm() == 0 {
		proposedRule.RuleNumber.CoordinatorTerm = rev.GetRevokedBelowTerm()
	}
	if proposedRule.GetCoordinatorId() == nil {
		proposedRule.CoordinatorId = orchID
	}
	if proposedRule.GetCreationTime() == nil {
		proposedRule.CreationTime = now
	}
	return nil
}
