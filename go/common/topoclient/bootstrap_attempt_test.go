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

package topoclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	"github.com/multigres/multigres/go/common/types"
)

// Fixed timestamps used across tests — no dependency on real clock time.
var (
	fixedTime1 = time.Unix(1700000000, 0)
	fixedTime2 = time.Unix(1700000001, 0)
)

func testBootstrapShardKey() types.ShardKey {
	return types.ShardKey{
		Database:   "mydb",
		TableGroup: "mygroup",
		Shard:      "0",
	}
}

// readBootstrapAttempt reads the current record without modifying it.
func readBootstrapAttempt(t *testing.T, ts topoclient.Store, shardKey types.ShardKey) *clustermetadatapb.BootstrapAttempt {
	t.Helper()
	var current *clustermetadatapb.BootstrapAttempt
	_, err := ts.UpdateBootstrapAttempt(context.Background(), shardKey,
		func(existing *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
			current = existing
			return nil, nil // read-only, don't write
		})
	require.NoError(t, err)
	return current
}

// seedBootstrapAttempt injects a BootstrapAttempt unconditionally.
func seedBootstrapAttempt(t *testing.T, ts topoclient.Store, shardKey types.ShardKey, orchID string, startedAt time.Time) {
	t.Helper()
	_, err := ts.UpdateBootstrapAttempt(context.Background(), shardKey,
		func(_ *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
			return &clustermetadatapb.BootstrapAttempt{
				OrchId:    orchID,
				StartedAt: timestamppb.New(startedAt),
			}, nil
		})
	require.NoError(t, err)
}

// TestUpdateBootstrapAttempt_NoExistingRecord: fn receives nil when no record exists, write succeeds.
func TestUpdateBootstrapAttempt_NoExistingRecord(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()

	wrote, err := ts.UpdateBootstrapAttempt(ctx, shardKey, func(existing *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
		require.Nil(t, existing, "should receive nil when no record exists")
		return &clustermetadatapb.BootstrapAttempt{
			OrchId:    "orch-1",
			StartedAt: timestamppb.New(fixedTime1),
		}, nil
	})
	require.NoError(t, err)
	require.True(t, wrote)

	attempt := readBootstrapAttempt(t, ts, shardKey)
	require.NotNil(t, attempt)
	require.Equal(t, "orch-1", attempt.OrchId)
	require.Equal(t, fixedTime1.Unix(), attempt.StartedAt.AsTime().Unix())
}

// TestUpdateBootstrapAttempt_FnReturnsNil_NoExistingRecord: fn returning nil leaves topo untouched.
func TestUpdateBootstrapAttempt_FnReturnsNil_NoExistingRecord(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()

	wrote, err := ts.UpdateBootstrapAttempt(ctx, shardKey, func(_ *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
		return nil, nil
	})
	require.NoError(t, err)
	require.False(t, wrote)

	require.Nil(t, readBootstrapAttempt(t, ts, shardKey), "record should not have been created")
}

// TestUpdateBootstrapAttempt_FnReturnsNil_ExistingRecord: fn returning nil after seeing existing record leaves it unchanged.
func TestUpdateBootstrapAttempt_FnReturnsNil_ExistingRecord(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()
	seedBootstrapAttempt(t, ts, shardKey, "orch-1", fixedTime1)

	wrote, err := ts.UpdateBootstrapAttempt(ctx, shardKey, func(existing *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
		require.NotNil(t, existing)
		return nil, nil // caller chooses not to overwrite
	})
	require.NoError(t, err)
	require.False(t, wrote)

	attempt := readBootstrapAttempt(t, ts, shardKey)
	require.Equal(t, "orch-1", attempt.OrchId, "original record should be unchanged")
}

// TestUpdateBootstrapAttempt_ExistingRecord_Overwrite: fn receives existing record and replaces it.
func TestUpdateBootstrapAttempt_ExistingRecord_Overwrite(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()
	seedBootstrapAttempt(t, ts, shardKey, "orch-old", fixedTime1)

	wrote, err := ts.UpdateBootstrapAttempt(ctx, shardKey, func(existing *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
		require.NotNil(t, existing)
		require.Equal(t, "orch-old", existing.OrchId)
		return &clustermetadatapb.BootstrapAttempt{
			OrchId:    "orch-new",
			StartedAt: timestamppb.New(fixedTime2),
		}, nil
	})
	require.NoError(t, err)
	require.True(t, wrote)

	attempt := readBootstrapAttempt(t, ts, shardKey)
	require.Equal(t, "orch-new", attempt.OrchId)
	require.Equal(t, fixedTime2.Unix(), attempt.StartedAt.AsTime().Unix())
}

// TestUpdateBootstrapAttempt_DifferentShards_Independent: writes to distinct shards don't interfere.
func TestUpdateBootstrapAttempt_DifferentShards_Independent(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shard0 := types.ShardKey{Database: "mydb", TableGroup: "mygroup", Shard: "0"}
	shard1 := types.ShardKey{Database: "mydb", TableGroup: "mygroup", Shard: "1"}

	write := func(shardKey types.ShardKey, orchID string) (bool, error) {
		return ts.UpdateBootstrapAttempt(ctx, shardKey, func(_ *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
			return &clustermetadatapb.BootstrapAttempt{
				OrchId:    orchID,
				StartedAt: timestamppb.New(fixedTime1),
			}, nil
		})
	}

	wrote, err := write(shard0, "orch-1")
	require.NoError(t, err)
	require.True(t, wrote)

	wrote, err = write(shard1, "orch-2")
	require.NoError(t, err)
	require.True(t, wrote, "different shard should be independent")

	require.Equal(t, "orch-1", readBootstrapAttempt(t, ts, shard0).OrchId)
	require.Equal(t, "orch-2", readBootstrapAttempt(t, ts, shard1).OrchId)
}

// TestUpdateBootstrapAttempt_FnError_Propagated: error returned by fn surfaces to caller.
func TestUpdateBootstrapAttempt_FnError_Propagated(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()
	fnErr := &topoclient.TopoError{Code: topoclient.NoNode}

	wrote, err := ts.UpdateBootstrapAttempt(ctx, shardKey, func(_ *clustermetadatapb.BootstrapAttempt) (*clustermetadatapb.BootstrapAttempt, error) {
		return nil, fnErr
	})
	require.ErrorIs(t, err, fnErr)
	require.False(t, wrote)
}

// TestBootstrapAttempt_RoundTrip: seeded fields are read back correctly.
func TestBootstrapAttempt_RoundTrip(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()
	seedBootstrapAttempt(t, ts, shardKey, "orch-seed", fixedTime1)

	attempt := readBootstrapAttempt(t, ts, shardKey)
	require.NotNil(t, attempt)
	require.Equal(t, "orch-seed", attempt.OrchId)
	require.Equal(t, fixedTime1.Unix(), attempt.StartedAt.AsTime().Unix())
}

// TestBootstrapAttempt_NoRecord: reading from empty topo returns nil.
func TestBootstrapAttempt_NoRecord(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	require.Nil(t, readBootstrapAttempt(t, ts, testBootstrapShardKey()))
}

// TestBootstrapAttempt_CorrectProtoFields: specific field values survive proto serialization.
func TestBootstrapAttempt_CorrectProtoFields(t *testing.T) {
	ctx := context.Background()
	ts, _ := memorytopo.NewServerAndFactory(ctx, "zone-1")
	defer ts.Close()

	shardKey := testBootstrapShardKey()
	seedBootstrapAttempt(t, ts, shardKey, "cell1_orch1", time.Unix(1700000000, 0))

	got := readBootstrapAttempt(t, ts, shardKey)
	require.NotNil(t, got)
	require.Equal(t, "cell1_orch1", got.OrchId)
	require.Equal(t, int64(1700000000), got.StartedAt.AsTime().Unix())
}
