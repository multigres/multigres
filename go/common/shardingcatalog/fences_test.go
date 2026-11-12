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

package shardingcatalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	mtrpcpb "github.com/multigres/multigres/go/pb/mtrpc"
	pb "github.com/multigres/multigres/go/pb/shardingcatalog"
)

const (
	fenceFencing  = pb.FenceState_FENCE_STATE_FENCING
	fenceFenced   = pb.FenceState_FENCE_STATE_FENCED
	fenceUnfenced = pb.FenceState_FENCE_STATE_UNFENCED
)

// fencedSnapshot is validSnapshot with fences: one on the whole of
// analytics, one on commerce's 0-80, and a lifted one on commerce's 80-inf.
func fencedSnapshot(t *testing.T) *pb.Snapshot {
	t.Helper()
	p := validSnapshot(t)
	p.Fences = []*pb.Fence{
		// Listed out of id order: the snapshot sorts them.
		{FenceId: 52, OperationId: testOperation, TablegroupId: 2, ShardId: new(int64(3)), State: fenceUnfenced, Version: 12},
		{FenceId: 50, OperationId: testOperation, TablegroupId: 3, State: fenceFenced, Version: 10},
		{FenceId: 51, OperationId: testOperation, TablegroupId: 2, ShardId: new(int64(2)), State: fenceFencing, Version: 11},
	}
	return p
}

func fenceIDs(fences []*Fence) []int64 {
	ids := make([]int64, len(fences))
	for i, f := range fences {
		ids[i] = f.ID()
	}
	return ids
}

func TestNew_Fences(t *testing.T) {
	s, err := New(fencedSnapshot(t))
	require.NoError(t, err)

	assert.Equal(t, []int64{50, 51, 52}, fenceIDs(s.Fences()), "every fence, in id order")

	commerce, _ := s.TableGroupByName("commerce")
	analytics, _ := s.TableGroupByName("analytics")
	byName := func(g *TableGroup, name string) *Shard {
		for _, sh := range g.Shards() {
			if sh.Name() == name {
				return sh
			}
		}
		t.Fatalf("no shard %q", name)
		return nil
	}

	f := s.Fences()[1]
	assert.Equal(t, testOperation, f.OperationID())
	assert.Same(t, commerce, f.TableGroup())
	assert.Same(t, byName(commerce, "0-80"), f.Shard())
	assert.Equal(t, fenceFencing, f.State())
	assert.Equal(t, int64(11), f.Version())

	whole := s.Fences()[0]
	assert.Nil(t, whole.Shard(), "a whole-tablegroup fence has no shard")

	t.Run("FencesFor", func(t *testing.T) {
		assert.Equal(t, []int64{51}, fenceIDs(commerce.FencesFor(byName(commerce, "0-80"))))
		assert.Equal(t, []int64{52}, fenceIDs(commerce.FencesFor(byName(commerce, "80-inf"))), "a lifted fence is still returned")
		assert.Empty(t, commerce.FencesFor(byName(commerce, "0-inf")), "no fence on the retired shard")
		assert.Empty(t, commerce.FencesFor(nil))
		assert.Equal(t, []int64{50}, fenceIDs(analytics.FencesFor(byName(analytics, "0-40"))), "a whole-tablegroup fence applies to every shard")
		assert.Equal(t, []int64{50}, fenceIDs(analytics.FencesFor(nil)))
		assert.Equal(t, []int64{50}, fenceIDs(analytics.FencesFor(byName(commerce, "0-80"))), "another tablegroup's shard matches only the whole-tablegroup fence")
	})
	t.Run("both a whole-tablegroup and a shard fence apply", func(t *testing.T) {
		p := fencedSnapshot(t)
		p.Fences = append(p.Fences, &pb.Fence{FenceId: 54, OperationId: testOperation, TablegroupId: 2, State: fenceFenced, Version: 12})
		s, err := New(p)
		require.NoError(t, err)
		commerce, _ := s.TableGroupByName("commerce")
		assert.Equal(t, []int64{54, 51}, fenceIDs(commerce.FencesFor(byName(commerce, "0-80"))), "the whole-tablegroup fence first")
	})
}

func TestNew_RejectsFences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(p *pb.Snapshot)
		want   string
	}{
		{"repeated fence id", func(p *pb.Snapshot) {
			p.Fences[1].FenceId = 52
		}, "fence id 52 is repeated"},
		{"dangling tablegroup", func(p *pb.Snapshot) {
			p.Fences[1].TablegroupId = 99
		}, "fence 50 refers to tablegroup 99, which does not exist"},
		{"dangling shard", func(p *pb.Snapshot) {
			p.Fences[2].ShardId = new(int64(99))
		}, "fence 51 refers to shard 99, which does not exist"},
		{"shard of another tablegroup", func(p *pb.Snapshot) {
			p.Fences[2].ShardId = new(int64(5))
		}, `fence 51 fences shard 5 of tablegroup "analytics", not of its tablegroup "commerce"`},
		{"unspecified state", func(p *pb.Snapshot) {
			p.Fences[1].State = pb.FenceState_FENCE_STATE_UNSPECIFIED
		}, "fence 50 has unknown state FENCE_STATE_UNSPECIFIED"},
		{"unknown state", func(p *pb.Snapshot) {
			p.Fences[1].State = pb.FenceState(42)
		}, "fence 50 has unknown state"},
		{"two fences on a shard", func(p *pb.Snapshot) {
			p.Fences = append(p.Fences, &pb.Fence{FenceId: 60, OperationId: "22222222-2222-2222-2222-222222222222", TablegroupId: 2, ShardId: new(int64(2)), State: fenceFenced})
		}, "fences 51 and 60 have the same scope"},
		{"two fences on a whole tablegroup", func(p *pb.Snapshot) {
			p.Fences = append(p.Fences, &pb.Fence{FenceId: 60, OperationId: testOperation, TablegroupId: 3, State: fenceUnfenced})
		}, "fences 50 and 60 have the same scope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := fencedSnapshot(t)
			tc.mutate(p)
			_, err := New(p)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Equal(t, mtrpcpb.Code_INVALID_ARGUMENT, mterrors.Code(err))
		})
	}
}
