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
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/tools/prototest"
)

// configureValidCmd returns an applyRuleChangeCmd preloaded with values that
// pass buildRequest's validation in cert mode (explicit cert, not unsafe-derive).
// Individual tests mutate one field to trigger the case they care about.
func configureValidCmd() *applyRuleChangeCmd {
	a := newApplyRuleChangeCmd()
	a.database.Set("postgres")
	a.tableGroup.Set("default")
	a.shard.Set("0-inf")
	a.leader.Set("zone1_mp1")
	a.cohort.Set([]string{"zone1_mp1", "zone1_mp2"})
	a.durability.Set("AT_LEAST_2")
	a.frozenLSN.Set("0/0")
	a.outgoingRuleTerm.Set(5)
	a.outgoingLeaderSubterm.Set(3)
	a.reason.Set("test")
	return a
}

func TestBuildRequest_CertModeHappyPath(t *testing.T) {
	a := configureValidCmd()

	req, err := a.buildRequest()
	require.NoError(t, err)

	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"}
	mp2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"}
	want := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"},
		ProposedTransition: &clustermetadatapb.RulePosition{
			Proposal: &clustermetadatapb.ShardRule{
				LeaderId:      leaderID,
				CohortMembers: []*clustermetadatapb.ID{leaderID, mp2ID},
				DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
					PolicyName:    "AT_LEAST_2",
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
					RequiredCount: 2,
					Description:   "At least 2 nodes must acknowledge",
				},
			},
		},
		Reason: "test",
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{
			Cert: &clustermetadatapb.ExternallyCertifiedRevocation{
				FrozenLsn: "0/0",
				TermRevocation: &clustermetadatapb.TermRevocation{
					OutgoingRule: &clustermetadatapb.RuleNumber{
						CoordinatorTerm: 5,
						LeaderSubterm:   3,
					},
				},
			},
		},
	}
	prototest.AssertEqual(t, want, req)
}

func TestBuildRequest_UnsafeDeriveMode(t *testing.T) {
	a := configureValidCmd()
	a.frozenLSN.Set("")
	a.outgoingRuleTerm.Set(0)
	a.outgoingLeaderSubterm.Set(0)
	a.unsafeDeriveCert.Set(true)

	req, err := a.buildRequest()
	require.NoError(t, err)

	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"}
	mp2ID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp2"}
	want := &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"},
		ProposedTransition: &clustermetadatapb.RulePosition{
			Proposal: &clustermetadatapb.ShardRule{
				LeaderId:      leaderID,
				CohortMembers: []*clustermetadatapb.ID{leaderID, mp2ID},
				DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
					PolicyName:    "AT_LEAST_2",
					QuorumType:    clustermetadatapb.QuorumType_QUORUM_TYPE_AT_LEAST_N,
					RequiredCount: 2,
					Description:   "At least 2 nodes must acknowledge",
				},
			},
		},
		Reason: "test",
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert{
			UnsafeDeriveCert: &multiadminpb.UnsafeDeriveCertOptions{},
		},
	}
	prototest.AssertEqual(t, want, req)
}

func TestBuildRequest_ValidationErrors(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*applyRuleChangeCmd)
		wantErrSub string
	}{
		{
			name:       "missing --leader",
			mutate:     func(a *applyRuleChangeCmd) { a.leader.Set("") },
			wantErrSub: "--leader, --cohort, and --durability are required",
		},
		{
			name:       "missing --cohort",
			mutate:     func(a *applyRuleChangeCmd) { a.cohort.Set(nil) },
			wantErrSub: "--leader, --cohort, and --durability are required",
		},
		{
			name:       "missing --durability",
			mutate:     func(a *applyRuleChangeCmd) { a.durability.Set("") },
			wantErrSub: "--leader, --cohort, and --durability are required",
		},
		{
			name:       "missing --frozen-lsn without unsafe-derive",
			mutate:     func(a *applyRuleChangeCmd) { a.frozenLSN.Set("") },
			wantErrSub: "--frozen-lsn is required",
		},
		{
			name: "--frozen-lsn + --unsafe-derive mutually exclusive",
			mutate: func(a *applyRuleChangeCmd) {
				a.unsafeDeriveCert.Set(true)
			},
			wantErrSub: "mutually exclusive",
		},
		{
			name:       "bad leader format",
			mutate:     func(a *applyRuleChangeCmd) { a.leader.Set("not-a-cell-name") },
			wantErrSub: "invalid --leader",
		},
		{
			name:       "bad cohort entry format",
			mutate:     func(a *applyRuleChangeCmd) { a.cohort.Set([]string{"zone1_mp1", "broken"}) },
			wantErrSub: `invalid --cohort entry "broken"`,
		},
		{
			name:       "unsupported durability policy",
			mutate:     func(a *applyRuleChangeCmd) { a.durability.Set("MAJORITY") },
			wantErrSub: "invalid --durability",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := configureValidCmd()
			tt.mutate(a)

			_, err := a.buildRequest()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrSub)
		})
	}
}

// makeConfirmCmd returns a cobra command with stdin/stdout buffers swapped in
// so confirm can be exercised without touching the real os.Stdin.
func makeConfirmCmd(stdin string) (*cobra.Command, *bytes.Buffer) {
	cmd := &cobra.Command{}
	cmd.SetIn(strings.NewReader(stdin))
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	return cmd, out
}

func testConfirmRequest() *multiadminpb.ApplyCertifiedRuleChangeRequest {
	leaderID := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIPOOLER, Cell: "zone1", Name: "mp1"}
	return &multiadminpb.ApplyCertifiedRuleChangeRequest{
		ShardKey: &clustermetadatapb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"},
		ProposedTransition: &clustermetadatapb.RulePosition{
			Proposal: &clustermetadatapb.ShardRule{
				LeaderId:      leaderID,
				CohortMembers: []*clustermetadatapb.ID{leaderID},
				DurabilityPolicy: &clustermetadatapb.DurabilityPolicy{
					PolicyName: "AT_LEAST_2",
				},
			},
		},
		Reason: "test",
		CertSource: &multiadminpb.ApplyCertifiedRuleChangeRequest_Cert{
			Cert: &clustermetadatapb.ExternallyCertifiedRevocation{
				FrozenLsn: "0/0",
				TermRevocation: &clustermetadatapb.TermRevocation{
					OutgoingRule: &clustermetadatapb.RuleNumber{CoordinatorTerm: 5},
				},
			},
		},
	}
}

func TestConfirm_AcceptsMatchingShardName(t *testing.T) {
	cmd, out := makeConfirmCmd("0-inf\n")
	require.NoError(t, confirm(cmd, testConfirmRequest()))

	// Summary should mention the load-bearing fields the operator needs to
	// see before confirming.
	output := out.String()
	assert.Contains(t, output, "Shard:       postgres/default/0-inf")
	assert.Contains(t, output, "Leader:      zone1/mp1")
	assert.Contains(t, output, "Durability:  AT_LEAST_2")
	assert.Contains(t, output, "Cert mode:   explicit")
	assert.Contains(t, output, "outgoing_rule_term=5")
}

func TestConfirm_RejectsMismatchedShardName(t *testing.T) {
	cmd, _ := makeConfirmCmd("wrong-name\n")
	err := confirm(cmd, testConfirmRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "confirmation did not match")
}

func TestConfirm_RejectsEmptyInput(t *testing.T) {
	cmd, _ := makeConfirmCmd("")
	err := confirm(cmd, testConfirmRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "aborted")
}

func TestConfirm_UnsafeDeriveModeLabeled(t *testing.T) {
	// Unsafe-derive mode should be visually distinct from explicit cert mode
	// so the operator can't miss it.
	req := testConfirmRequest()
	req.CertSource = &multiadminpb.ApplyCertifiedRuleChangeRequest_UnsafeDeriveCert{
		UnsafeDeriveCert: &multiadminpb.UnsafeDeriveCertOptions{},
	}

	cmd, out := makeConfirmCmd("0-inf\n")
	require.NoError(t, confirm(cmd, req))
	assert.Contains(t, out.String(), "UNSAFE derive from reachable cohort")
}

func TestAddApplyRuleChangeCommand(t *testing.T) {
	root := &cobra.Command{Use: "test-root"}
	AddApplyRuleChangeCommand(root)

	sub, _, err := root.Find([]string{"apply-rule-change"})
	require.NoError(t, err)
	require.NotEqual(t, root, sub, "apply-rule-change subcommand should be registered")

	// Spot-check that the operator-facing flags are registered. We don't
	// enumerate every flag — just the ones whose absence would silently break
	// the documented invocation.
	for _, name := range []string{
		"leader", "cohort", "durability", "frozen-lsn",
		"unsafe-derive-cert-from-reachable", "outgoing-rule-term",
		"reason", "yes", "admin-server",
	} {
		assert.NotNil(t, sub.Flags().Lookup(name), "flag --%s should be registered", name)
	}
}
