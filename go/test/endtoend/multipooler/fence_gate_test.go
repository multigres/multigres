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
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/multigres/multigres/go/common/shardingcatalog"
	mdpb "github.com/multigres/multigres/go/pb/multipoolermanagerdata"
	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// fenceGateOp is the operation that fences the default tablegroup.
const fenceGateOp = "aaaaaaaa-0000-0000-0000-0000000000f1"

// fenceGateWindow is the gateway's --buffer-window: how long a fenced
// statement waits for the fence to lift before the client gets MTF02.
const fenceGateWindow = 3 * time.Second

// isFenced reports whether err is the pooler's MTF02, as the client sees it.
func isFenced(err error) bool {
	if err == nil {
		return false
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && string(pqErr.Code) == "MTF02" {
		return true
	}
	return strings.Contains(err.Error(), "MTF02") || strings.Contains(err.Error(), "fenced")
}

// sqlState returns err's SQLSTATE, "" when it has none.
func sqlState(err error) string {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}
	return ""
}

// refresh calls RefreshFences on the pooler and returns its answer.
func (c routingClients) refresh(t *testing.T, minVersion int64, drainTimeout time.Duration) *mdpb.RefreshFencesResponse {
	t.Helper()
	resp, err := c.manager.RefreshFences(t.Context(), &mdpb.RefreshFencesRequest{
		MinRoutingVersion: minVersion, DrainTimeout: durationpb.New(drainTimeout),
	})
	require.NoError(t, err, "RefreshFences")
	return resp
}

// TestFenceGate drives the primary's fence gate end to end through a
// gateway: fencing default and refreshing refuses writes with MTF02 (or the
// gateway retries them when the fence lifts in time); the drain lets an open
// transaction finish or force-closes it at the timeout; the fence survives a
// pooler restart and a failover; unfencing restores traffic; and finishing an
// operation removes its fences from the snapshot.
//
// Uses an isolated shard: it fences the catalog shard and fails it over.
func TestFenceGate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("skipping: PostgreSQL binaries not found")
	}

	setup, cleanup := shardsetup.NewIsolated(t,
		shardsetup.WithMultipoolerCount(3),
		shardsetup.WithMultiorchCount(1),
		shardsetup.WithDatabase(routingE2EDatabase),
		shardsetup.WithCellName("test-cell"),
		shardsetup.WithMultigatewayExtraArgs("--buffer-window="+fenceGateWindow.String()),
	)
	defer cleanup()
	setup.StartMultiorchs(t.Context(), t)
	setup.WaitForMultigatewayQueryServing(t)

	ctx := t.Context()
	// Clients for every pooler, made here so they outlive the subtests: the
	// primary changes in the failover subtest.
	clients := map[string]routingClients{}
	for name, inst := range setup.Multipoolers {
		clients[name] = newRoutingClients(t, inst.Multipooler.GrpcPort)
	}
	primary := setup.GetPrimary(t)
	pc := clients[primary.Name]
	direct := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
	defer direct.Close()
	_, err := direct.ExecContext(ctx, `INSERT INTO multigres.operation (operation_id, kind, state) VALUES ($1::uuid, 'reshard', 'preparing')`, fenceGateOp)
	require.NoError(t, err)

	gw, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	defer gw.Close()
	_, err = gw.ExecContext(ctx, `CREATE TABLE fence_gate_t (id serial PRIMARY KEY, v text)`)
	require.NoError(t, err)
	write := func(ctx context.Context) error {
		_, err := gw.ExecContext(ctx, `INSERT INTO fence_gate_t (v) VALUES ('x')`)
		return err
	}
	require.NoError(t, write(ctx))

	var fencedAt int64
	fence := func(t *testing.T) {
		t.Helper()
		fencedAt = pc.fence(t, fenceGateOp, pc.snapshot(t).GetRoutingVersion(), fenceSpec("default", ""))
	}
	unfence := func(t *testing.T, c routingClients) int64 {
		t.Helper()
		return c.unfence(t, fenceGateOp, fencedAt, fenceSpec("default", ""))
	}

	t.Run("a fenced primary refuses writes with MTF02", func(t *testing.T) {
		fence(t)
		require.NoError(t, write(ctx), "nothing is enforced before the pooler refreshes")

		resp := pc.refresh(t, fencedAt, 10*time.Second)
		assert.True(t, resp.GetFenced())
		assert.GreaterOrEqual(t, resp.GetRoutingVersion(), fencedAt)
		// A refresh with nothing changed is a no-op.
		assert.True(t, pc.refresh(t, fencedAt, 10*time.Second).GetFenced())

		// The drained primary acknowledged the fence before the refresh
		// returned: one ack, for the catalog shard.
		acks := fenceAcksOf(t, direct, fenceGateOp)
		require.Len(t, acks, 1)
		assert.Equal(t, defaultShardID(t, direct), acks[0].shardID)
		assert.NotEmpty(t, acks[0].poolerID)
		assert.NotEmpty(t, acks[0].rule, "a managed primary records its consensus rule")

		start := time.Now()
		err := write(ctx)
		require.Error(t, err)
		assert.True(t, isFenced(err), "want MTF02, got %v", err)
		assert.GreaterOrEqual(t, time.Since(start), fenceGateWindow-500*time.Millisecond, "the gateway waited for the fence to lift")

		// Reads on the fenced primary are refused too: it is closed to all
		// new application work.
		_, err = gw.ExecContext(ctx, `SELECT count(*) FROM fence_gate_t`)
		assert.True(t, isFenced(err), "want MTF02, got %v", err)
	})

	t.Run("AckFences replaces the recorded ack and skips a lifted fence", func(t *testing.T) {
		acks := fenceAcksOf(t, direct, fenceGateOp)
		require.Len(t, acks, 1)
		ack := func(shardID int64, poolerID string) error {
			_, err := pc.manager.AckFences(ctx, &mdpb.AckFencesRequest{Acks: []*mdpb.FenceAck{{
				FenceId: acks[0].fenceID, ShardId: shardID, PoolerId: poolerID,
			}}})
			return err
		}
		// A new ack replaces the recorded one: the row names the latest sender.
		require.NoError(t, ack(acks[0].shardID, "test-replace"))
		got := fenceAcksOf(t, direct, fenceGateOp)
		require.Len(t, got, 1)
		assert.Equal(t, "test-replace", got[0].poolerID)
		assert.Empty(t, got[0].rule)

		// A shard outside the fence's scope is refused.
		err := ack(acks[0].shardID+1000, "test-out-of-scope")
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)

		// A fence that does not exist is skipped, the other acks of the call
		// recorded.
		_, err = pc.manager.AckFences(ctx, &mdpb.AckFencesRequest{Acks: []*mdpb.FenceAck{
			{FenceId: acks[0].fenceID + 1000, ShardId: acks[0].shardID, PoolerId: "test-lifted"},
			{FenceId: acks[0].fenceID, ShardId: acks[0].shardID, PoolerId: acks[0].poolerID, ConsensusRule: acks[0].rule},
		}})
		require.NoError(t, err)
		assert.Equal(t, acks, fenceAcksOf(t, direct, fenceGateOp))
	})

	t.Run("the catalog primary's own fence never blocks the catalog", func(t *testing.T) {
		// default is fenced and its primary's gate is closed. The catalog
		// lives on that same primary, and its reads and writes go through the
		// internal query service, which the gate never sees: otherwise the
		// unfence below could never be written.
		require.True(t, pc.hasFenceOf(t, fenceGateOp), "the snapshot reads")
		assert.Equal(t, fencedAt, pc.fence(t, fenceGateOp, 0, fenceSpec("default", "")), "a FenceScopes retry is a no-op")
		require.True(t, isFenced(write(ctx)), "application work is still refused")

		// Lift the fence and raise it again, without refreshing in between,
		// so the gate stays closed through both writes.
		unfence(t, pc)
		require.False(t, pc.hasFenceOf(t, fenceGateOp), "UnfenceScopes wrote")
		assert.Empty(t, fenceAcksOf(t, direct, fenceGateOp), "unfencing deletes the fence's acks")
		fence(t)
		require.True(t, pc.hasFenceOf(t, fenceGateOp), "FenceScopes wrote")
		assert.True(t, pc.refresh(t, fencedAt, 10*time.Second).GetFenced())
		acks := fenceAcksOf(t, direct, fenceGateOp)
		require.Len(t, acks, 1, "the new fence is acknowledged")
	})

	t.Run("the gateway retries a statement whose fence lifts within its window", func(t *testing.T) {
		start := time.Now()
		done := make(chan error, 1)
		go func() { done <- write(context.Background()) }()
		// The write is still waiting after a second: the gate refused it and
		// the gateway is holding it until the fence lifts. An unfenced
		// pooler would have run it in milliseconds.
		time.Sleep(time.Second)
		select {
		case err := <-done:
			t.Fatalf("the write returned before the fence lifted: %v", err)
		default:
		}
		// The gateway may see the unfence before the pooler has refreshed and
		// be refused once more; it retries with backoff until the pooler has.
		version := unfence(t, pc)
		assert.False(t, pc.refresh(t, version, 10*time.Second).GetFenced())
		select {
		case err := <-done:
			require.NoError(t, err, "the statement ran once the fence lifted")
			assert.Less(t, time.Since(start), fenceGateWindow, "it ran within the window, not after it")
		case <-time.After(2 * fenceGateWindow):
			t.Fatal("the fenced statement never returned")
		}
	})

	t.Run("an open transaction finishes before the gate closes", func(t *testing.T) {
		tx, err := gw.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `INSERT INTO fence_gate_t (v) VALUES ('in-tx')`)
		require.NoError(t, err)

		fence(t)
		type refreshResult struct {
			resp *mdpb.RefreshFencesResponse
			err  error
		}
		refreshed := make(chan refreshResult, 1)
		go func() {
			resp, err := pc.manager.RefreshFences(context.Background(), &mdpb.RefreshFencesRequest{
				MinRoutingVersion: fencedAt, DrainTimeout: durationpb.New(30 * time.Second),
			})
			refreshed <- refreshResult{resp, err}
		}()

		// Stage 1: the transaction goes on, and single queries still run.
		time.Sleep(300 * time.Millisecond)
		select {
		case r := <-refreshed:
			t.Fatalf("the refresh returned with a transaction open: %+v", r)
		default:
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fence_gate_t (v) VALUES ('in-tx-2')`)
		require.NoError(t, err, "the open transaction continues")
		require.NoError(t, write(ctx), "single queries run while transactions finish")

		require.NoError(t, tx.Commit(), "the open transaction commits")
		select {
		case r := <-refreshed:
			require.NoError(t, r.err, "RefreshFences")
			assert.True(t, r.resp.GetFenced())
		case <-time.After(10 * time.Second):
			t.Fatal("the drain did not complete once the transaction committed")
		}
		var n int
		require.NoError(t, direct.QueryRowContext(ctx, `SELECT count(*) FROM fence_gate_t WHERE v LIKE 'in-tx%'`).Scan(&n))
		assert.Equal(t, 2, n)
		assert.True(t, isFenced(write(ctx)), "once drained, single queries are refused")

		version := unfence(t, pc)
		pc.refresh(t, version, 10*time.Second)
		require.NoError(t, write(ctx))
	})

	t.Run("a single query still running at the drain timeout is killed", func(t *testing.T) {
		const marker = "fence-drain-sleep"
		queryDone := make(chan error, 1)
		go func() {
			_, err := gw.ExecContext(ctx, `SELECT pg_sleep(60), '`+marker+`'`)
			queryDone <- err
		}()
		running := func() int {
			var n int
			require.NoError(t, direct.QueryRowContext(ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state = 'active' AND query LIKE '%`+marker+`%'`).Scan(&n))
			return n
		}
		require.Eventually(t, func() bool { return running() == 1 }, 10*time.Second, 50*time.Millisecond,
			"the single query is running")

		fence(t)
		start := time.Now()
		assert.True(t, pc.refresh(t, fencedAt, time.Second).GetFenced())
		assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond, "the drain waited for its timeout")
		assert.Zero(t, running(), "the query's backend is gone when the refresh reports drained")
		select {
		case err := <-queryDone:
			require.Error(t, err, "the killed query fails")
		case <-time.After(10 * time.Second):
			t.Fatal("the killed query did not return")
		}

		version := unfence(t, pc)
		pc.refresh(t, version, 10*time.Second)
		require.NoError(t, write(ctx), "the pooler admits writes again once the fence is lifted")
	})

	t.Run("a transaction still open at the drain timeout gets 40001", func(t *testing.T) {
		tx, err := gw.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		_, err = tx.ExecContext(ctx, `INSERT INTO fence_gate_t (v) VALUES ('killed')`)
		require.NoError(t, err)

		fence(t)
		start := time.Now()
		assert.True(t, pc.refresh(t, fencedAt, time.Second).GetFenced())
		assert.GreaterOrEqual(t, time.Since(start), 900*time.Millisecond, "the drain waited for its timeout")

		_, err = tx.ExecContext(ctx, `INSERT INTO fence_gate_t (v) VALUES ('killed-2')`)
		if err == nil {
			err = tx.Commit()
		}
		require.Error(t, err, "the force-closed transaction cannot go on")
		assert.Equal(t, "40001", sqlState(err), "got %v", err)
		var n int
		require.NoError(t, direct.QueryRowContext(ctx, `SELECT count(*) FROM fence_gate_t WHERE v LIKE 'killed%'`).Scan(&n))
		assert.Zero(t, n, "nothing of the transaction committed")
	})

	t.Run("the fence survives a pooler restart", func(t *testing.T) {
		// Forget the ack, to see the restarted pooler, in a new gate epoch,
		// acknowledge the fence again from its background read.
		_, err := direct.ExecContext(ctx, `DELETE FROM multigres.admission_fence_ack`)
		require.NoError(t, err)
		// The restarted process must outlive this subtest: start it on the
		// test's context, not the subtest's.
		killMultipooler(t, primary)
		require.NoError(t, primary.Multipooler.Start(ctx, t))
		shardsetup.WaitForManagerReady(t, primary.Multipooler)
		require.Eventually(t, func() bool {
			cctx, cancel := context.WithTimeout(ctx, 2*fenceGateWindow)
			defer cancel()
			return isFenced(write(cctx))
		}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond, "the restarted primary should read its fence and refuse writes")
		require.Eventually(t, func() bool {
			acks := fenceAcksOf(t, direct, fenceGateOp)
			return len(acks) == 1
		}, utils.ScaleTimeout(30*time.Second), 200*time.Millisecond, "the restarted primary acknowledges the fence")
	})

	t.Run("the fence survives a failover", func(t *testing.T) {
		for name, c := range clients {
			_, err := c.manager.SetPostgresRestartsEnabled(ctx, &mdpb.SetPostgresRestartsEnabledRequest{Enabled: false})
			require.NoError(t, err, "disable postgres restarts on %s", name)
		}
		acked := fenceAcksOf(t, direct, fenceGateOp)
		require.Len(t, acked, 1)
		old := setup.PrimaryName
		setup.KillPostgres(t, old)
		newName := shardsetup.WaitForNewPrimary(t, setup, old, utils.ScaleTimeout(60*time.Second))
		primary = setup.GetMultipoolerInstance(newName)
		pc = clients[newName]
		require.Eventually(t, func() bool {
			cctx, cancel := context.WithTimeout(ctx, 2*fenceGateWindow)
			defer cancel()
			return isFenced(write(cctx))
		}, utils.ScaleTimeout(60*time.Second), 500*time.Millisecond, "the new primary should read the fence on promotion and refuse writes")

		// The ack survives on the new primary, which acknowledges the fence
		// again: the row then names the new primary and its consensus rule.
		db := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
		defer db.Close()
		require.Eventually(t, func() bool {
			acks := fenceAcksOf(t, db, fenceGateOp)
			return len(acks) == 1 && acks[0].poolerID != acked[0].poolerID
		}, utils.ScaleTimeout(30*time.Second), 200*time.Millisecond, "the new primary acknowledges the fence")
		acks := fenceAcksOf(t, db, fenceGateOp)
		assert.Equal(t, acked[0].fenceID, acks[0].fenceID)
		assert.Equal(t, acked[0].shardID, acks[0].shardID)
		assert.NotEqual(t, acked[0].rule, acks[0].rule, "a failover is under a newer consensus rule")
	})

	t.Run("unfencing and refreshing restores traffic", func(t *testing.T) {
		version := unfence(t, pc)
		db := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
		defer db.Close()
		assert.Empty(t, fenceAcksOf(t, db, fenceGateOp), "unfencing deletes the fence's acks")
		assert.False(t, pc.refresh(t, version, 10*time.Second).GetFenced())
		require.Eventually(t, func() bool { return write(ctx) == nil }, utils.ScaleTimeout(30*time.Second), 200*time.Millisecond,
			"writes flow again once the fence is lifted")
	})

	t.Run("finishing an operation removes its fences from the snapshot", func(t *testing.T) {
		created, err := pc.manager.CreateTableGroup(ctx, &mdpb.CreateTableGroupRequest{Name: "fencetg"})
		require.NoError(t, err)
		pc.fence(t, created.GetOperationId(), pc.snapshot(t).GetRoutingVersion(), fenceSpec("fencetg", ""))
		require.True(t, pc.hasFenceOf(t, created.GetOperationId()))

		// Cancel the creation: the tablegroup loop fails it, which deletes its
		// fences in the same write.
		db := connectToPostgresViaSocket(t, getPostgresSocketPath(primary.Pgctld.PoolerDir), primary.Pgctld.PgPort)
		defer db.Close()
		_, err = db.ExecContext(ctx, `UPDATE multigres.operation SET cancel_requested = true WHERE operation_id = $1::uuid`, created.GetOperationId())
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var state string
			return db.QueryRowContext(ctx, `SELECT state FROM multigres.operation WHERE operation_id = $1::uuid`, created.GetOperationId()).Scan(&state) == nil &&
				state == "failed"
		}, utils.ScaleTimeout(30*time.Second), 200*time.Millisecond, "the cancelled creation should fail")
		assert.False(t, pc.hasFenceOf(t, created.GetOperationId()), "a failed operation holds no fence")
	})
}

// hasFenceOf reports whether the pooler's snapshot holds a fence of op.
func (c routingClients) hasFenceOf(t *testing.T, op string) bool {
	t.Helper()
	resp, err := c.manager.GetRoutingSnapshot(t.Context(), &mdpb.GetRoutingSnapshotRequest{})
	require.NoError(t, err)
	s, err := shardingcatalog.New(resp.GetSnapshot())
	require.NoError(t, err)
	for _, f := range s.Fences() {
		if f.OperationID() == op {
			return true
		}
	}
	return false
}

// fenceAck is a multigres.admission_fence_ack row.
type fenceAck struct {
	fenceID, shardID int64
	poolerID, rule   string
}

// fenceAcksOf returns the acks of op's fences, by fence and shard.
func fenceAcksOf(t *testing.T, db *sql.DB, op string) []fenceAck {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT a.fence_id, a.shard_id, a.pooler_id, a.consensus_rule
FROM multigres.admission_fence_ack a JOIN multigres.admission_fence f USING (fence_id)
WHERE f.operation_id = $1::uuid ORDER BY a.fence_id, a.shard_id`, op)
	require.NoError(t, err)
	defer rows.Close()
	var out []fenceAck
	for rows.Next() {
		var a fenceAck
		require.NoError(t, rows.Scan(&a.fenceID, &a.shardID, &a.poolerID, &a.rule))
		out = append(out, a)
	}
	require.NoError(t, rows.Err())
	return out
}

// defaultShardID returns the catalog id of default/0-inf.
func defaultShardID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var id int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT s.shard_id FROM multigres.shard s
JOIN multigres.tablegroup g USING (tablegroup_id) WHERE g.name = 'default' AND s.shard_name = '0-inf'`).Scan(&id))
	return id
}
