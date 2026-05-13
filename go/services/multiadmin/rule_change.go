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
func (s *MultiAdminServer) ApplyCertifiedRuleChange(ctx context.Context, req *multiadminpb.ApplyCertifiedRuleChangeRequest) (*multiadminpb.ApplyCertifiedRuleChangeResponse, error) {
	if req.GetShardKey() == nil {
		return nil, status.Error(codes.InvalidArgument, "shard_key is required")
	}
	if req.GetProposedRule() == nil {
		return nil, status.Error(codes.InvalidArgument, "proposed_rule is required")
	}
	if req.GetCertSource() == nil {
		return nil, status.Error(codes.InvalidArgument, "exactly one of cert or unsafe_derive_cert must be set")
	}

	orch, err := s.pickOrch(ctx, req.GetProposedRule().GetLeaderId())
	if err != nil {
		return nil, err
	}

	proposedRule := proto.Clone(req.GetProposedRule()).(*clustermetadatapb.ShardRule)
	cert, err := s.buildCert(ctx, req, proposedRule)
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

	if _, err := multiorchpb.NewMultiOrchServiceClient(conn).ApplyCertifiedRuleChange(ctx, &multiorchpb.ApplyCertifiedRuleChangeRequest{
		ShardKey:     req.GetShardKey(),
		ProposedRule: proposedRule,
		Cert:         cert,
		Reason:       req.GetReason(),
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
// Recruit/Propose is wasteful) and falls back to any orch in any other cell.
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
func (s *MultiAdminServer) pickOrch(ctx context.Context, leaderID *clustermetadatapb.ID) (*clustermetadatapb.MultiOrch, error) {
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
		orchs, err := s.ts.GetMultiOrchsByCell(ctx, cell)
		if err != nil {
			s.logger.WarnContext(ctx, "failed to list orchs in cell while picking one",
				"cell", cell, "error", err)
			lastErr = err
			continue
		}
		for _, o := range orchs {
			if o.MultiOrch != nil {
				return o.MultiOrch, nil
			}
		}
	}
	if lastErr != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no multiorch found in any reachable cell; last error: %v", lastErr)
	}
	return nil, status.Error(codes.FailedPrecondition, "no multiorch registered in any cell")
}

// dialOrch opens a gRPC connection to a multiorch using its grpc port.
func (s *MultiAdminServer) dialOrch(ctx context.Context, orch *clustermetadatapb.MultiOrch) (*grpc.ClientConn, error) {
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

// buildCert returns the ExternallyCertifiedRevocation to forward. For an
// explicit cert this is a clone of the caller's input. For unsafe_derive_cert
// it probes the proposed cohort and computes outgoing_rule_number and
// frozen_lsn from the most-advanced response.
func (s *MultiAdminServer) buildCert(
	ctx context.Context,
	req *multiadminpb.ApplyCertifiedRuleChangeRequest,
	proposedRule *clustermetadatapb.ShardRule,
) (*clustermetadatapb.ExternallyCertifiedRevocation, error) {
	switch cs := req.GetCertSource().(type) {
	case *multiadminpb.ApplyCertifiedRuleChangeRequest_Cert:
		if cs.Cert == nil {
			return nil, status.Error(codes.InvalidArgument, "cert source is empty")
		}
		return proto.Clone(cs.Cert).(*clustermetadatapb.ExternallyCertifiedRevocation), nil

	case *multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert:
		outgoingRule, frozenLSN, err := s.probeMostAdvanced(ctx, proposedRule.GetCohortMembers(), proposedRule.GetDurabilityPolicy())
		if err != nil {
			return nil, err
		}
		return &clustermetadatapb.ExternallyCertifiedRevocation{
			OutgoingRuleNumber: outgoingRule,
			FrozenLsn:          frozenLSN,
		}, nil

	default:
		return nil, status.Error(codes.InvalidArgument, "unknown cert_source variant")
	}
}

// probeMostAdvanced calls MultiPoolerManager.Status on every proposed cohort
// member and returns the highest (rule_number, lsn) pair observed across the
// reachable subset.
//
// Hard failures:
//   - A cohort member ID that does not resolve in topology — we refuse to
//     silently exclude it.
//   - Insufficient responses: per durabilityPolicy.CheckSufficientRecruitment,
//     the reachable subset must be enough to satisfy the new rule's quorum.
//     If not, the cert we'd derive could be missing the most-advanced node
//     in the proposed cohort.
//
// Soft failures: individual RPC errors are logged and skipped. The operator
// chose unsafe_derive_cert; the reachable subset is what we derive from.
func (s *MultiAdminServer) probeMostAdvanced(
	ctx context.Context,
	cohortMembers []*clustermetadatapb.ID,
	durabilityPolicy *clustermetadatapb.DurabilityPolicy,
) (*clustermetadatapb.RuleNumber, string, error) {
	if len(cohortMembers) == 0 {
		return nil, "", status.Error(codes.InvalidArgument, "cohort_members is required for unsafe_derive_cert")
	}
	policy, err := commonconsensus.NewPolicyFromProto(durabilityPolicy)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "invalid durability_policy: %v", err)
	}

	// Resolve every cohort member up front. A missing pooler is a hard
	// failure — proceeding would mean deriving the cert from a strict
	// subset of the cohort that the caller did not opt into.
	poolers := make([]*clustermetadatapb.MultiPooler, 0, len(cohortMembers))
	for _, id := range cohortMembers {
		info, err := s.ts.GetMultiPooler(ctx, id)
		if err != nil {
			return nil, "", status.Errorf(codes.NotFound, "pooler %s not found in topology: %v",
				topoclient.ClusterIDString(id), err)
		}
		poolers = append(poolers, info.MultiPooler)
	}

	type probeResult struct {
		pooler *clustermetadatapb.MultiPooler
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

	var (
		best         *clustermetadatapb.PoolerPosition
		reachable    []*clustermetadatapb.ID
		lastProbeErr error
	)
	for range poolers {
		r := <-results
		if r.err != nil {
			s.logger.WarnContext(ctx, "Status probe failed",
				"pooler", topoclient.ClusterIDString(r.pooler.GetId()), "error", r.err)
			lastProbeErr = r.err
			continue
		}
		if r.pos == nil {
			s.logger.WarnContext(ctx, "pooler returned no current position; skipping",
				"pooler", topoclient.ClusterIDString(r.pooler.GetId()))
			continue
		}
		reachable = append(reachable, r.pooler.GetId())
		if best == nil || commonconsensus.ComparePosition(r.pos, best) > 0 {
			best = r.pos
		}
	}

	// Require a quorum of the proposed cohort to respond. Without that,
	// the derived cert could understate the cohort's most-advanced position.
	if err := policy.CheckSufficientRecruitment(cohortMembers, reachable); err != nil {
		if lastProbeErr != nil {
			return nil, "", status.Errorf(codes.Unavailable,
				"insufficient cohort responses to derive cert: %v (last probe error: %v)", err, lastProbeErr)
		}
		return nil, "", status.Errorf(codes.Unavailable, "insufficient cohort responses to derive cert: %v", err)
	}

	s.logger.InfoContext(ctx, "derived cert from reachable cohort",
		"reachable", len(reachable), "total", len(poolers))

	outgoingRule := best.GetRule().GetRuleNumber()
	if outgoingRule == nil {
		// Fresh bootstrap: no rule recorded anywhere. Bootstrap convention.
		outgoingRule = &clustermetadatapb.RuleNumber{}
	}
	frozenLSN := best.GetLsn()
	if frozenLSN == "" {
		return nil, "", status.Error(codes.Unavailable, "most-advanced cohort member reported an empty LSN")
	}
	return outgoingRule, frozenLSN, nil
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
		rev.RevokedBelowTerm = cert.GetOutgoingRuleNumber().GetCoordinatorTerm() + 1
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
