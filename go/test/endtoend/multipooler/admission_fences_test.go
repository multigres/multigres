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

package multipooler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/multigres/multigres/go/common/shardingcatalog"
	mdpb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	shardingcatalogpb "github.com/multigres/multigres/go/pb/shardingcatalog"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
	"github.com/multigres/multigres/go/tools/testpoll"
)

const routingFenceOtherOp = "aaaaaaaa-0000-0000-0000-0000000000f0"

// fenceState is a fence as the test compares it: its scope by name, its
// operation, state and version.
type fenceState struct {
	tablegroup, shard string
	operationID       string
	state             shardingcatalogpb.FenceState
	version           int64
}

// fences reads the pooler's snapshot and returns its routing version and
// fences, after checking the snapshot validates as a gateway would.
func (c routingClients) fences(t *testing.T) (int64, []fenceState) {
	t.Helper()
	resp, err := c.manager.GetRoutingSnapshot(t.Context(), &mdpb.GetRoutingSnapshotRequest{})
	require.NoError(t, err)
	s, err := shardingcatalog.New(resp.GetSnapshot())
	require.NoError(t, err, "the served snapshot validates")
	var out []fenceState
	for _, f := range s.Fences() {
		fs := fenceState{tablegroup: f.TableGroup().Name(), operationID: f.OperationID(), state: f.State(), version: f.Version()}
		if f.Shard() != nil {
			fs.shard = f.Shard().Name()
		}
		out = append(out, fs)
	}
	return s.RoutingVersion(), out
}

// TestAdmissionFences_RPCs drives FenceScopes and UnfenceScopes against a
// real catalog: an import fences both sides, sets its rule, deletes it and
// unfences, with the snapshot showing the fences at each step; retries,
// malformed lists, all-or-nothing refusals and overlapping fences of another
// operation are checked on the way.
//
// Uses an isolated shard: it writes catalog rows.
func TestAdmissionFences_RPCs(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t, shardsetup.WithMultipoolerCount(2))
	defer cleanup()

	primary := setup.GetPrimary(t)
	pc := newRoutingClients(t, primary.Multipooler.GrpcPort)
	db := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer db.Close()
	ctx := t.Context()

	_, err := db.ExecContext(ctx, importFixture)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO multigres.operation (operation_id, kind, state) VALUES ($1::uuid, 'reshard', 'preparing')`, routingFenceOtherOp)
	require.NoError(t, err)

	version, fences := pc.fences(t)
	require.Zero(t, version)
	require.Empty(t, fences)
	importRule := []*mdpb.ShardRoutingRuleSpec{shardSpec("default", "srctg", "0-inf")}

	t.Run("fence the source", func(t *testing.T) {
		version = pc.fence(t, routingImportOp, version, fenceSpec("default", ""))
		assert.Equal(t, int64(1), version)
		// A retry with the now stale version is a no-op.
		assert.Equal(t, version, pc.fence(t, routingImportOp, 0, fenceSpec("default", "")))

		v, fences := pc.fences(t)
		assert.Equal(t, version, v)
		assert.Equal(t, []fenceState{
			{tablegroup: "default", operationID: routingImportOp, state: shardingcatalogpb.FenceState_FENCE_STATE_FENCED, version: 1},
		}, fences)

		// The target is not fenced yet.
		_, err := pc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, Rules: importRule, ExpectedRoutingVersion: version,
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), `shard "0-inf" of tablegroup "srctg" is not fenced by operation `+routingImportOp)
	})

	t.Run("refused fences", func(t *testing.T) {
		// Another unfinished operation cannot fence a scope that overlaps
		// the import's whole-tablegroup fence, and one such scope refuses
		// the whole list: srctg is not fenced either.
		_, err := pc.manager.FenceScopes(ctx, &mdpb.FenceScopesRequest{
			Database: routingE2EDatabase, OperationId: routingFenceOtherOp, ExpectedRoutingVersion: version,
			Scopes: []*mdpb.FenceScopeSpec{fenceSpec("srctg", "0-inf"), fenceSpec("default", "0-inf")},
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), `tablegroup "default" is fenced by operation `+routingImportOp)

		_, err = pc.manager.FenceScopes(ctx, &mdpb.FenceScopesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version - 1,
			Scopes: []*mdpb.FenceScopeSpec{fenceSpec("srctg", "")},
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), "routing version is 1, expected 0")

		// Two scopes of one list that overlap each other are malformed.
		_, err = pc.manager.FenceScopes(ctx, &mdpb.FenceScopesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version,
			Scopes: []*mdpb.FenceScopeSpec{fenceSpec("srctg", ""), fenceSpec("srctg", "0-inf")},
		})
		assert.Equal(t, codes.InvalidArgument, status.Code(err), "overlapping list: %v", err)

		v, fences := pc.fences(t)
		assert.Equal(t, version, v, "a refused fence changes nothing")
		assert.Len(t, fences, 1)
	})

	t.Run("fence the target and set the rule", func(t *testing.T) {
		version = pc.fence(t, routingImportOp, version, fenceSpec("srctg", "0-inf"))
		resp, err := pc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, Rules: importRule, ExpectedRoutingVersion: version,
		})
		require.NoError(t, err)
		version = resp.GetRoutingVersion()
		assert.Equal(t, int64(3), version)

		snap := pc.snapshot(t)
		require.Len(t, snap.GetRules(), 1)
		_, fences := pc.fences(t)
		assert.Equal(t, []fenceState{
			{tablegroup: "default", operationID: routingImportOp, state: shardingcatalogpb.FenceState_FENCE_STATE_FENCED, version: 1},
			{tablegroup: "srctg", shard: "0-inf", operationID: routingImportOp, state: shardingcatalogpb.FenceState_FENCE_STATE_FENCED, version: 2},
		}, fences)
	})

	t.Run("unfencing a side refuses deleting the rule until it is fenced again", func(t *testing.T) {
		version = pc.unfence(t, routingImportOp, version, fenceSpec("srctg", "0-inf"))
		_, fences := pc.fences(t)
		require.Len(t, fences, 1, "lifting a fence deletes its row")
		assert.Equal(t, "default", fences[0].tablegroup)

		_, err := pc.manager.DeleteRoutingRules(ctx, &mdpb.DeleteRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version,
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), `shard "0-inf" of tablegroup "srctg" is not fenced`)

		// A retry of the rule change that committed still succeeds.
		resp, err := pc.manager.SetRoutingRules(ctx, &mdpb.SetRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, Rules: importRule, ExpectedRoutingVersion: 2,
		})
		require.NoError(t, err)
		assert.Equal(t, version, resp.GetRoutingVersion(), "a no-op needs no fences")

		// Fencing again writes a new row at a new version.
		version = pc.fence(t, routingImportOp, version, fenceSpec("srctg", "0-inf"))
		_, fences = pc.fences(t)
		require.Len(t, fences, 2)
		assert.Equal(t, fenceState{
			tablegroup: "srctg", shard: "0-inf", operationID: routingImportOp, state: shardingcatalogpb.FenceState_FENCE_STATE_FENCED, version: version,
		}, fences[1])

		// A delayed unfence, based on a version from before the fence was
		// raised again, does not lift it.
		_, err = pc.manager.UnfenceScopes(ctx, &mdpb.UnfenceScopesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version - 1,
			Scopes: []*mdpb.FenceScopeSpec{fenceSpec("srctg", "0-inf")},
		})
		require.Error(t, err)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err))
		assert.Contains(t, err.Error(), `the fence on shard "0-inf" of tablegroup "srctg" was set at routing version`)
	})

	t.Run("delete the rule and unfence", func(t *testing.T) {
		fenced := version
		resp, err := pc.manager.DeleteRoutingRules(ctx, &mdpb.DeleteRoutingRulesRequest{
			Database: routingE2EDatabase, OperationId: routingImportOp, ExpectedRoutingVersion: version,
		})
		require.NoError(t, err)
		version = resp.GetRoutingVersion()
		assert.Empty(t, pc.snapshot(t).GetRules())

		// Both sides are lifted in one call, at one version. The lift is
		// based on the version the fences were read at: the rule delete
		// since does not force a retry.
		deleted := version
		both := []*mdpb.FenceScopeSpec{fenceSpec("default", ""), fenceSpec("srctg", "0-inf")}
		version = pc.unfence(t, routingImportOp, fenced, both...)
		assert.Equal(t, deleted+1, version)
		// Unfencing again, or a scope never fenced, is a no-op.
		assert.Equal(t, version, pc.unfence(t, routingImportOp, 0, both...))
		assert.Equal(t, version, pc.unfence(t, routingImportOp, 0, fenceSpec("srctg", "")))

		v, fences := pc.fences(t)
		assert.Equal(t, version, v)
		assert.Empty(t, fences, "lifting a fence deletes its row")
	})

	t.Run("another operation may fence once the import's fences are lifted", func(t *testing.T) {
		version = pc.fence(t, routingFenceOtherOp, version, fenceSpec("default", "0-inf"))
		_, fences := pc.fences(t)
		assert.Equal(t, []fenceState{
			{tablegroup: "default", shard: "0-inf", operationID: routingFenceOtherOp, state: shardingcatalogpb.FenceState_FENCE_STATE_FENCED, version: version},
		}, fences)
	})

	t.Run("dropping a tablegroup waits for its fences' operations, then deletes them", func(t *testing.T) {
		// srctg has no poolers and no provisioner claim, so once it is
		// dropping the tablegroup loop deletes it, unless a fence of an
		// unfinished operation still holds it.
		pc.fence(t, routingFenceOtherOp, pc.snapshot(t).GetRoutingVersion(), fenceSpec("srctg", "0-inf"))
		_, err := db.ExecContext(ctx, `UPDATE multigres.tablegroup SET state = 'dropping' WHERE name = 'srctg'`)
		require.NoError(t, err)
		tablegroupGone := func() bool {
			var n int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM multigres.tablegroup WHERE name = 'srctg'`).Scan(&n))
			return n == 0
		}
		// The loop passes every few seconds; give it a few passes.
		testpoll.Never(t, tablegroupGone, 10*time.Second, 500*time.Millisecond, "the fence of an unfinished operation keeps the tablegroup")

		_, err = db.ExecContext(ctx, `UPDATE multigres.operation SET state = 'done' WHERE operation_id = $1::uuid`, routingFenceOtherOp)
		require.NoError(t, err)
		require.Eventually(t, tablegroupGone, utils.ScaleTimeout(30*time.Second), 500*time.Millisecond, "the drop never completed")
		var left int
		require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM multigres.admission_fence f
			WHERE NOT EXISTS (SELECT 1 FROM multigres.tablegroup t WHERE t.tablegroup_id = f.tablegroup_id)`).Scan(&left))
		assert.Zero(t, left)
		_, fences := pc.fences(t)
		assert.Equal(t, []fenceState{
			{tablegroup: "default", shard: "0-inf", operationID: routingFenceOtherOp, state: shardingcatalogpb.FenceState_FENCE_STATE_FENCED, version: version},
		}, fences, "only the dropped tablegroup's fence went with it")
	})
}
