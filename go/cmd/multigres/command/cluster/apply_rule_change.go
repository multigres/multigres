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

package cluster

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/constants"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/viperutil"
)

type applyRuleChangeCmd struct {
	database              viperutil.Value[string]
	tableGroup            viperutil.Value[string]
	shard                 viperutil.Value[string]
	leader                viperutil.Value[string]
	cohort                viperutil.Value[[]string]
	durability            viperutil.Value[string]
	outgoingRuleTerm      viperutil.Value[int64]
	outgoingLeaderSubterm viperutil.Value[int64]
	frozenLSN             viperutil.Value[string]
	unsafeDeriveCert      viperutil.Value[bool]
	reason                viperutil.Value[string]
	yes                   viperutil.Value[bool]
	timeout               viperutil.Value[time.Duration]
}

// AddApplyRuleChangeCommand registers the apply-rule-change subcommand.
//
// Used for both initial leader appointment (cohort has no committed rule yet
// — pass zero outgoing-rule-term + --frozen-lsn=0/0) and stuck-quorum
// recovery (cohort has a rule but quorum is unreachable — pass the
// outgoing rule's term and frozen LSN, or use --unsafe-derive-cert-from-reachable
// to have multiadmin probe the proposed cohort and derive them).
func AddApplyRuleChangeCommand(clusterCmd *cobra.Command) {
	reg := viperutil.NewRegistry()

	a := &applyRuleChangeCmd{
		database: viperutil.Configure(reg, "database", viperutil.Options[string]{
			Default: "postgres", FlagName: "database",
		}),
		tableGroup: viperutil.Configure(reg, "table-group", viperutil.Options[string]{
			Default: constants.DefaultTableGroup, FlagName: "table-group",
		}),
		shard: viperutil.Configure(reg, "shard", viperutil.Options[string]{
			Default: constants.DefaultShard, FlagName: "shard",
		}),
		leader: viperutil.Configure(reg, "leader", viperutil.Options[string]{
			Default: "", FlagName: "leader",
		}),
		cohort: viperutil.Configure(reg, "cohort", viperutil.Options[[]string]{
			Default: nil, FlagName: "cohort",
		}),
		durability: viperutil.Configure(reg, "durability", viperutil.Options[string]{
			Default: "", FlagName: "durability",
		}),
		outgoingRuleTerm: viperutil.Configure(reg, "outgoing-rule-term", viperutil.Options[int64]{
			Default: 0, FlagName: "outgoing-rule-term",
		}),
		outgoingLeaderSubterm: viperutil.Configure(reg, "outgoing-leader-subterm", viperutil.Options[int64]{
			Default: 0, FlagName: "outgoing-leader-subterm",
		}),
		frozenLSN: viperutil.Configure(reg, "frozen-lsn", viperutil.Options[string]{
			Default: "", FlagName: "frozen-lsn",
		}),
		unsafeDeriveCert: viperutil.Configure(reg, "unsafe-derive-cert-from-reachable", viperutil.Options[bool]{
			Default: false, FlagName: "unsafe-derive-cert-from-reachable",
		}),
		reason: viperutil.Configure(reg, "reason", viperutil.Options[string]{
			Default: "", FlagName: "reason",
		}),
		yes: viperutil.Configure(reg, "yes", viperutil.Options[bool]{
			Default: false, FlagName: "yes",
		}),
		timeout: viperutil.Configure(reg, "timeout", viperutil.Options[time.Duration]{
			Default: 30 * time.Second, FlagName: "timeout",
		}),
	}

	cmd := &cobra.Command{
		Use:   "apply-rule-change",
		Short: "Install a new shard rule via an externally certified revocation",
		Long: `Install a new shard rule (leader, cohort, durability) using an externally
certified revocation. Handles both initial leader appointment (zero outgoing rule)
and stuck-quorum recovery (existing outgoing rule, supplied explicitly or derived
from a probe of the proposed cohort).

The cert is the operator's load-bearing safety attestation: by submitting,
the operator asserts that no member of the outgoing cohort will commit
further writes past the outgoing rule. If that assertion is wrong, data
loss is possible. Use --unsafe-derive-cert-from-reachable only when you
accept that risk explicitly.

Examples:

  # Initial leader appointment on a fresh shard
  multigres cluster apply-rule-change \
    --database=postgres --leader=zone1/mp1 \
    --cohort=zone1/mp1,zone1/mp2,zone1/mp3 \
    --durability=AT_LEAST_2 --frozen-lsn=0/0 \
    --reason="initial appointment"

  # Recovery from stuck quorum, explicit cert
  multigres cluster apply-rule-change \
    --database=postgres --leader=zone1/mp1 \
    --cohort=zone1/mp1 --durability=AT_LEAST_1 \
    --outgoing-rule-term=5 --frozen-lsn=0/16D2A40 \
    --reason="2 of 3 cohort members lost"

  # Recovery, multiadmin probes the proposed cohort to derive the cert
  multigres cluster apply-rule-change \
    --database=postgres --leader=zone1/mp1 \
    --cohort=zone1/mp1 --durability=AT_LEAST_1 \
    --unsafe-derive-cert-from-reachable \
    --reason="2 of 3 cohort members lost"`,
		RunE: a.run,
	}

	cmd.Flags().String("database", a.database.Default(), "Database name")
	cmd.Flags().String("table-group", a.tableGroup.Default(), "Table group name")
	cmd.Flags().String("shard", a.shard.Default(), "Shard name")
	cmd.Flags().String("leader", a.leader.Default(), "Proposed leader, format 'cell/name' (required)")
	cmd.Flags().StringSlice("cohort", a.cohort.Default(), "Proposed cohort members, comma-separated 'cell/name' (required)")
	cmd.Flags().String("durability", a.durability.Default(), "Durability policy, e.g. AT_LEAST_2 or MULTI_CELL_AT_LEAST_2 (required)")
	cmd.Flags().Int64("outgoing-rule-term", a.outgoingRuleTerm.Default(), "Coordinator term of the outgoing rule being revoked (0 for initial appointment)")
	cmd.Flags().Int64("outgoing-leader-subterm", a.outgoingLeaderSubterm.Default(), "Leader subterm of the outgoing rule")
	cmd.Flags().String("frozen-lsn", a.frozenLSN.Default(), "WAL LSN at which the outgoing cohort is certified frozen (use '0/0' for initial appointment). Required unless --unsafe-derive-cert-from-reachable is set")
	cmd.Flags().Bool("unsafe-derive-cert-from-reachable", a.unsafeDeriveCert.Default(), "Ask multiadmin to probe the proposed cohort and derive outgoing rule + frozen LSN. UNSAFE: if an unreachable node is more advanced, data loss is possible")
	cmd.Flags().String("reason", a.reason.Default(), "Free-text reason for the rule change (recorded for audit)")
	cmd.Flags().Bool("yes", a.yes.Default(), "Skip the interactive confirmation prompt")
	cmd.Flags().Duration("timeout", a.timeout.Default(), "Timeout for the RPC call")
	cmd.Flags().String("admin-server", "", "host:port of the multiadmin server (overrides config)")

	viperutil.BindFlags(cmd.Flags(),
		a.database, a.tableGroup, a.shard,
		a.leader, a.cohort, a.durability,
		a.outgoingRuleTerm, a.outgoingLeaderSubterm, a.frozenLSN,
		a.unsafeDeriveCert, a.reason, a.yes, a.timeout)

	clusterCmd.AddCommand(cmd)
}

func (a *applyRuleChangeCmd) run(cmd *cobra.Command, _ []string) error {
	leader := a.leader.Get()
	cohortRaw := a.cohort.Get()
	durabilityName := a.durability.Get()
	if leader == "" || len(cohortRaw) == 0 || durabilityName == "" {
		return errors.New("--leader, --cohort, and --durability are required")
	}
	unsafeDerive := a.unsafeDeriveCert.Get()
	frozenLSN := a.frozenLSN.Get()
	if !unsafeDerive && frozenLSN == "" {
		return errors.New("--frozen-lsn is required unless --unsafe-derive-cert-from-reachable is set (use '0/0' for initial appointment)")
	}
	if unsafeDerive && frozenLSN != "" {
		return errors.New("--frozen-lsn and --unsafe-derive-cert-from-reachable are mutually exclusive")
	}

	leaderID, err := parsePoolerID(leader)
	if err != nil {
		return fmt.Errorf("invalid --leader: %w", err)
	}
	cohortIDs := make([]*clustermetadatapb.ID, 0, len(cohortRaw))
	for _, raw := range cohortRaw {
		id, err := parsePoolerID(raw)
		if err != nil {
			return fmt.Errorf("invalid --cohort entry %q: %w", raw, err)
		}
		cohortIDs = append(cohortIDs, id)
	}

	durability, err := parseDurabilityPolicy(durabilityName)
	if err != nil {
		return fmt.Errorf("invalid --durability: %w", err)
	}

	req := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{
			Database:   a.database.Get(),
			TableGroup: a.tableGroup.Get(),
			Shard:      a.shard.Get(),
		},
		ProposedRule: &clustermetadatapb.ShardRule{
			LeaderId:         leaderID,
			CohortMembers:    cohortIDs,
			DurabilityPolicy: durability,
		},
		Reason: a.reason.Get(),
	}
	if unsafeDerive {
		req.CertSource = &multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert{
			UnsafeDeriveCert: &multiadminpb.UnsafeDeriveCertOptions{},
		}
	} else {
		req.CertSource = &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{
			Cert: &clustermetadatapb.ExternallyCertifiedRevocation{
				TermRevocation: &clustermetadatapb.TermRevocation{
					OutgoingRule: &clustermetadatapb.RuleNumber{
						CoordinatorTerm: a.outgoingRuleTerm.Get(),
						LeaderSubterm:   a.outgoingLeaderSubterm.Get(),
					},
				},
				FrozenLsn: frozenLSN,
			},
		}
	}

	if !a.yes.Get() {
		if err := confirm(cmd, req); err != nil {
			return err
		}
	}

	client, err := admin.NewClient(cmd)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(cmd.Context(), a.timeout.Get())
	defer cancel()

	resp, err := client.ApplyCertifiedRuleChange(ctx, req)
	if err != nil {
		return fmt.Errorf("ApplyCertifiedRuleChange failed: %w", err)
	}

	cmd.Printf("Installed rule term=%d, leader=%s/%s\n",
		resp.GetInstalledRule().GetRuleNumber().GetCoordinatorTerm(),
		resp.GetInstalledRule().GetLeaderId().GetCell(),
		resp.GetInstalledRule().GetLeaderId().GetName(),
	)
	cmd.Printf("Cert outgoing_rule_term=%d, frozen_lsn=%s\n",
		resp.GetCertUsed().GetTermRevocation().GetOutgoingRule().GetCoordinatorTerm(),
		resp.GetCertUsed().GetFrozenLsn(),
	)
	return nil
}

// parsePoolerID accepts "cell/name" and returns a fully-qualified pooler ID.
func parsePoolerID(raw string) (*clustermetadatapb.ID, error) {
	parts := strings.SplitN(strings.TrimSpace(raw), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("expected 'cell/name', got %q", raw)
	}
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      parts[0],
		Name:      parts[1],
	}, nil
}

// parseDurabilityPolicy maps a user-supplied policy name (e.g. "AT_LEAST_2")
// to a DurabilityPolicy proto. Mirrors the names accepted by
// ParseUserSpecifiedDurabilityPolicy in go/common/consensus/durability.go.
func parseDurabilityPolicy(name string) (*clustermetadatapb.DurabilityPolicy, error) {
	name = strings.TrimSpace(name)
	if rest, ok := strings.CutPrefix(name, "MULTI_CELL_AT_LEAST_"); ok {
		n, err := parseInt32(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid MULTI_CELL_AT_LEAST count: %w", err)
		}
		return &clustermetadatapb.DurabilityPolicy{
			PolicyName:    name,
			PolicyVersion: 1,
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_MULTI_CELL_AT_LEAST_N,
			RequiredCount: n,
			Description:   fmt.Sprintf("at least %d cells", n),
		}, nil
	}
	if rest, ok := strings.CutPrefix(name, "AT_LEAST_"); ok {
		n, err := parseInt32(rest)
		if err != nil {
			return nil, fmt.Errorf("invalid AT_LEAST count: %w", err)
		}
		return &clustermetadatapb.DurabilityPolicy{
			PolicyName:    name,
			PolicyVersion: 1,
			QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
			RequiredCount: n,
			Description:   fmt.Sprintf("at least %d nodes", n),
		}, nil
	}
	return nil, fmt.Errorf("unsupported durability policy %q (expected AT_LEAST_N or MULTI_CELL_AT_LEAST_N)", name)
}

func parseInt32(s string) (int32, error) {
	var n int32
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not a positive integer: %q", s)
		}
		n = n*10 + (r - '0')
	}
	if n == 0 {
		return 0, fmt.Errorf("must be positive: %q", s)
	}
	return n, nil
}

// confirm prints a summary of the operation and prompts the operator to type
// the shard name to proceed. Bypassed by --yes.
func confirm(cmd *cobra.Command, req *multiadminpb.ApplyCertifiedRuleChangeRequest) error {
	sk := req.GetShardKey()
	cohort := make([]string, 0, len(req.GetProposedRule().GetCohortMembers()))
	for _, id := range req.GetProposedRule().GetCohortMembers() {
		cohort = append(cohort, fmt.Sprintf("%s/%s", id.GetCell(), id.GetName()))
	}
	cmd.Printf("\nShard:       %s/%s/%s\n", sk.GetDatabase(), sk.GetTableGroup(), sk.GetShard())
	cmd.Printf("Leader:      %s/%s\n", req.GetProposedRule().GetLeaderId().GetCell(), req.GetProposedRule().GetLeaderId().GetName())
	cmd.Printf("Cohort:      %s\n", strings.Join(cohort, ", "))
	cmd.Printf("Durability:  %s\n", req.GetProposedRule().GetDurabilityPolicy().GetPolicyName())
	switch cs := req.GetCertSource().(type) {
	case *multiadminpb.ApplyCertifiedRuleChangeRequest_Cert:
		cmd.Printf("Cert mode:   explicit (outgoing_rule_term=%d, frozen_lsn=%s)\n",
			cs.Cert.GetTermRevocation().GetOutgoingRule().GetCoordinatorTerm(),
			cs.Cert.GetFrozenLsn())
	case *multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert:
		cmd.Printf("Cert mode:   UNSAFE derive from reachable cohort\n")
	}
	cmd.Printf("Reason:      %s\n", req.GetReason())

	cmd.Print("\nYou are attesting that any unreachable cohort members under\n" +
		"the outgoing rule will not commit further writes. If they resume\n" +
		"and accept writes under the outgoing rule, data loss may occur.\n\n")
	cmd.Printf("Type the shard name (%s) to confirm: ", sk.GetShard())

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return errors.New("aborted")
	}
	if strings.TrimSpace(scanner.Text()) != sk.GetShard() {
		return errors.New("aborted: confirmation did not match shard name")
	}
	return nil
}
