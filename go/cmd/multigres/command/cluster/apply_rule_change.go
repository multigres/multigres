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
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/multigres/multigres/go/cmd/multigres/command/admin"
	"github.com/multigres/multigres/go/common/consensus"
	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/topoclient"
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
// newApplyRuleChangeCmd constructs the applyRuleChangeCmd struct with all its
// viperutil.Value flag handles. Shared by the public command constructor and
// by tests that need a struct whose .Set methods feed into buildRequest's
// .Get reads.
func newApplyRuleChangeCmd() *applyRuleChangeCmd {
	reg := viperutil.NewRegistry()
	return &applyRuleChangeCmd{
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
}

func AddApplyRuleChangeCommand(clusterCmd *cobra.Command) {
	a := newApplyRuleChangeCmd()

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
    --database=postgres --leader=zone1_mp1 \
    --cohort=zone1_mp1,zone1_mp2,zone1_mp3 \
    --durability=AT_LEAST_2 --frozen-lsn=0/0 \
    --reason="initial appointment"

  # Recovery from stuck quorum, explicit cert
  multigres cluster apply-rule-change \
    --database=postgres --leader=zone1_mp1 \
    --cohort=zone1_mp1,zone1_mp2 --durability=AT_LEAST_2 \
    --outgoing-rule-term=5 --frozen-lsn=0/16D2A40 \
    --reason="too many cohort members lost"

  # Recovery, multiadmin probes the proposed cohort to derive the cert
  multigres cluster apply-rule-change \
    --database=postgres --leader=zone1_mp1 \
    --cohort=zone1_mp1,zone1_mp2 --durability=AT_LEAST_2 \
    --unsafe-derive-cert-from-reachable \
    --reason="too many cohort members lost"`,
		RunE: a.run,
	}

	cmd.Flags().String("database", a.database.Default(), "Database name")
	cmd.Flags().String("table-group", a.tableGroup.Default(), "Table group name")
	cmd.Flags().String("shard", a.shard.Default(), "Shard name")
	cmd.Flags().String("leader", a.leader.Default(), "Proposed leader, format 'cell_name' (required)")
	cmd.Flags().StringSlice("cohort", a.cohort.Default(), "Proposed cohort members, comma-separated 'cell_name' (required)")
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

// buildRequest validates the configured flags and assembles the
// ApplyCertifiedRuleChangeRequest the CLI sends to multiadmin. Extracted from
// run so the flag validation + cert-vs-derive switch can be unit-tested
// without dialing multiadmin or reading stdin.
func (a *applyRuleChangeCmd) buildRequest() (*multiadminpb.ApplyCertifiedRuleChangeRequest, error) {
	leader := a.leader.Get()
	cohortRaw := a.cohort.Get()
	durabilityName := a.durability.Get()
	if leader == "" || len(cohortRaw) == 0 || durabilityName == "" {
		return nil, errors.New("--leader, --cohort, and --durability are required")
	}
	unsafeDerive := a.unsafeDeriveCert.Get()
	frozenLSN := a.frozenLSN.Get()
	if !unsafeDerive && frozenLSN == "" {
		return nil, errors.New("--frozen-lsn is required unless --unsafe-derive-cert-from-reachable is set (use '0/0' for initial appointment)")
	}
	if unsafeDerive && frozenLSN != "" {
		return nil, errors.New("--frozen-lsn and --unsafe-derive-cert-from-reachable are mutually exclusive")
	}

	leaderID, err := parsePoolerID(leader)
	if err != nil {
		return nil, fmt.Errorf("invalid --leader: %w", err)
	}
	cohortIDs := make([]*clustermetadatapb.ID, 0, len(cohortRaw))
	for _, raw := range cohortRaw {
		id, err := parsePoolerID(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid --cohort entry %q: %w", raw, err)
		}
		cohortIDs = append(cohortIDs, id)
	}

	durability, err := consensus.ParseUserSpecifiedDurabilityPolicy(durabilityName)
	if err != nil {
		return nil, fmt.Errorf("invalid --durability: %w", err)
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
	return req, nil
}

func (a *applyRuleChangeCmd) run(cmd *cobra.Command, _ []string) error {
	req, err := a.buildRequest()
	if err != nil {
		return err
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

// parsePoolerID accepts "cell_name" (the canonical encoding used by
// topoclient.ClusterIDString) and returns a fully-qualified MULTIPOOLER ID.
func parsePoolerID(raw string) (*clustermetadatapb.ID, error) {
	cell, name, err := topoclient.SplitClusterID(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	return &clustermetadatapb.ID{
		Component: clustermetadatapb.ID_MULTIPOOLER,
		Cell:      cell,
		Name:      name,
	}, nil
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

	scanner := bufio.NewScanner(cmd.InOrStdin())
	if !scanner.Scan() {
		return errors.New("aborted")
	}
	if strings.TrimSpace(scanner.Text()) != sk.GetShard() {
		return errors.New("aborted: confirmation did not match shard name")
	}
	return nil
}
