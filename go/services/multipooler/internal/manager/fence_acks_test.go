// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	multipoolermanagerdatapb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor/mock"
)

var (
	lockAckedFencePattern = "^" + regexp.QuoteMeta(lockAckedFenceSQL) + "$"
	upsertFenceAckPattern = "^" + regexp.QuoteMeta(upsertFenceAckSQL) + "$"
)

func testAck(fenceID, shardID int64) *multipoolermanagerdatapb.FenceAck {
	return &multipoolermanagerdatapb.FenceAck{FenceId: fenceID, ShardId: shardID, PoolerId: "multipooler-cell1-p1", ConsensusRule: "1.0"}
}

// expectAckedFence answers the next fence lock of an ack: rows nil means the
// fence does not exist, else whether the shard is in its scope.
func (env *tableGroupTestEnv) expectAckedFence(rows [][]any) {
	env.qs.AddQueryPatternOnce(lockAckedFencePattern, mock.MakeQueryResult([]string{"in_scope"}, rows))
}

func (env *tableGroupTestEnv) ackFences(t *testing.T, acks ...*multipoolermanagerdatapb.FenceAck) error {
	t.Helper()
	_, err := env.pm.AckFences(t.Context(), &multipoolermanagerdatapb.AckFencesRequest{Acks: acks})
	return err
}

func TestAckFences_RecordsAcks(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expect(upsertFenceAckPattern, nil, nil)
	env.expectCommit()
	env.expectAckedFence([][]any{{true}})
	env.expectAckedFence([][]any{{true}})

	require.NoError(t, env.ackFences(t, testAck(50, 2), testAck(51, 2)))
	assert.Len(t, env.queriesMatching("INSERT INTO multigres.admission_fence_ack"), 2)
	assert.Equal(t, []string{"COMMIT"}, env.queriesMatching("COMMIT"))
	assert.Empty(t, env.queriesMatching("catalog_state"), "an ack takes no catalog_state lock and advances no version")
	require.NoError(t, env.qs.ExpectationsWereMet())

	// The upsert replaces a recorded ack, and the lock keeps a concurrent
	// unfence from deleting the fence under it.
	assert.Contains(t, upsertFenceAckSQL, "ON CONFLICT (fence_id, shard_id) DO UPDATE")
	assert.Contains(t, lockAckedFenceSQL, "FOR KEY SHARE OF f")
}

// An ack of a fence that was lifted is skipped, and the other acks of the
// call are still recorded.
func TestAckFences_SkipsALiftedFence(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expect(upsertFenceAckPattern, nil, nil)
	env.expectCommit()
	env.expectAckedFence(nil)
	env.expectAckedFence([][]any{{true}})

	require.NoError(t, env.ackFences(t, testAck(50, 2), testAck(51, 2)))
	assert.Len(t, env.queriesMatching("INSERT INTO multigres.admission_fence_ack"), 1)
	assert.Len(t, env.queriesMatching("COMMIT"), 1)
}

// An ack for a shard outside its fence's scope fails the whole call, writing
// nothing.
func TestAckFences_RefusesAShardOutsideTheScope(t *testing.T) {
	env := newCatalogTestEnv(t)
	env.expectPrimary()
	env.expect(upsertFenceAckPattern, nil, nil)
	env.expectCommit()
	env.expectAckedFence([][]any{{true}})
	env.expectAckedFence([][]any{{false}})

	err := env.ackFences(t, testAck(50, 2), testAck(51, 3))
	assert.Equal(t, mtrpcpb.Code_FAILED_PRECONDITION, mterrors.Code(err), "%v", err)
	assert.Empty(t, env.queriesMatching("COMMIT"))
}

func TestAckFences_RejectsInvalidInput(t *testing.T) {
	ack := func(edit func(a *multipoolermanagerdatapb.FenceAck)) *multipoolermanagerdatapb.FenceAck {
		a := testAck(50, 2)
		edit(a)
		return a
	}
	for name, acks := range map[string][]*multipoolermanagerdatapb.FenceAck{
		"no acks":             nil,
		"no fence_id":         {ack(func(a *multipoolermanagerdatapb.FenceAck) { a.FenceId = 0 })},
		"a negative shard_id": {ack(func(a *multipoolermanagerdatapb.FenceAck) { a.ShardId = -1 })},
		"no pooler_id":        {ack(func(a *multipoolermanagerdatapb.FenceAck) { a.PoolerId = "" })},
		"one bad ack of many": {testAck(50, 2), ack(func(a *multipoolermanagerdatapb.FenceAck) { a.FenceId = -3 })},
	} {
		t.Run(name, func(t *testing.T) {
			env := newCatalogTestEnv(t)
			env.expectPrimary()
			err := env.ackFences(t, acks...)
			assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err), "%v", err)
			assert.Empty(t, env.queriesMatching("admission_fence"))
		})
	}

	t.Run("an empty consensus_rule is valid: an unmanaged pooler has none", func(t *testing.T) {
		env := newCatalogTestEnv(t)
		env.expectPrimary()
		env.expect(upsertFenceAckPattern, nil, nil)
		env.expectAckedFence([][]any{{true}})
		require.NoError(t, env.ackFences(t, ack(func(a *multipoolermanagerdatapb.FenceAck) { a.ConsensusRule = "" })))
	})
}

// Only the catalog primary records acks: another pooler of the catalog shard
// answers UNAVAILABLE, for the sender to find the primary, and a pooler off
// the catalog shard FAILED_PRECONDITION; neither opens a transaction.
func TestAckFences_OnlyOnCatalogPrimary(t *testing.T) {
	req := &multipoolermanagerdatapb.AckFencesRequest{Acks: []*multipoolermanagerdatapb.FenceAck{testAck(50, 2)}}
	for _, tc := range []struct {
		name              string
		tableGroup, shard string
		code              mtrpcpb.Code
	}{
		{"a standby of the catalog shard", constants.DefaultTableGroup, constants.DefaultShard, mtrpcpb.Code_UNAVAILABLE},
		{"a pooler off the catalog shard", "commerce", "0-80", mtrpcpb.Code_FAILED_PRECONDITION},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pm, m := newRoutingTestManager(t, tc.tableGroup, tc.shard, false)
			pm.state = ManagerStateReady
			began := false
			m.AddQueryPatternWithCallback("^BEGIN$", mock.MakeQueryResult(nil, nil), func(string) { began = true })
			_, err := pm.AckFences(context.Background(), req)
			assert.Equal(t, tc.code, mterrors.Code(err), "%v", err)
			assert.False(t, began, "a refused AckFences must not open a transaction")
		})
	}
}
