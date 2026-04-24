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

package multiorch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	consensusdatapb "github.com/multigres/multigres/go/pb/consensusdata"
)

// TestBeginTerm_SameCoordinatorRestartIsRejectedAtSameTerm asserts the
// restart-detection invariant across a real gRPC boundary: a second BeginTerm
// at the same term from the same coordinator ID with a different
// coordinator_initiated_at (the signature of a restarted coordinator that
// lost its in-memory claim) must be rejected by the pooler.
//
// We drive BeginTerm directly against a pooler (bypassing multiorch) to
// exercise the pooler-side admission logic in isolation. A real restart would
// additionally have to win the recruitment quorum; this test covers the
// defensive rejection at the pooler.
func TestBeginTerm_SameCoordinatorRestartIsRejectedAtSameTerm(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end test (short mode)")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("Skipping end-to-end test (no postgres binaries)")
	}
	t.Setenv("KEEP_TEMP_DIRS", "1")

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiOrchCount(0),
		shardsetup.WithCellName("test-cell"),
	)
	defer cleanup()

	primary := setup.GetMultipoolerInstance(setup.PrimaryName)
	require.NotNil(t, primary)
	client, err := shardsetup.NewMultipoolerClient(primary.Multipooler.GrpcPort)
	require.NoError(t, err)
	defer client.Close()

	// Use a term well above any bootstrap term so the first BeginTerm always
	// lands as a new (strictly higher) term.
	ctx := utils.WithTimeout(t, 30*time.Second)
	currentTerm := shardsetup.MustGetCurrentTerm(t, ctx, client.Consensus)
	newTerm := currentTerm + 100

	coord := &clustermetadatapb.ID{Component: clustermetadatapb.ID_MULTIORCH, Cell: "test-cell", Name: "mo-restart-test"}
	t0 := timestamppb.New(time.Unix(20000, 0))
	t1 := timestamppb.New(time.Unix(20001, 0))

	first := &consensusdatapb.BeginTermRequest{
		Term:                   newTerm,
		CandidateId:            coord,
		ShardId:                constants.DefaultShard,
		Action:                 consensusdatapb.BeginTermAction_BEGIN_TERM_ACTION_NO_ACTION,
		CoordinatorInitiatedAt: t0,
	}
	resp, err := client.Consensus.BeginTerm(utils.WithTimeout(t, 10*time.Second), first)
	require.NoError(t, err)
	require.True(t, resp.Accepted, "first BeginTerm should be accepted")

	second := proto.Clone(first).(*consensusdatapb.BeginTermRequest)
	second.CoordinatorInitiatedAt = t1
	resp, err = client.Consensus.BeginTerm(utils.WithTimeout(t, 10*time.Second), second)
	require.NoError(t, err)
	require.False(t, resp.Accepted,
		"restarted coordinator (same term, same coord, different coordinator_initiated_at) must be rejected")
}
