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

package executor

import (
	"context"
	"time"

	"github.com/multigres/multigres/go/common/mterrors"
	pgClient "github.com/multigres/multigres/go/common/pgprotocol/client"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/queryservice"
	"github.com/multigres/multigres/go/common/sqltypes"
	multipoolerpb "github.com/multigres/multigres/go/pb/multipoolerservice"
	"github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// A pooler whose target an operation has fenced refuses new application work
// with MTF02 before running any of it. The statement's plan, not only its
// targets, may be stale when a fence lifts (a routing switch, a revert, an
// unfence): table rules apply when a statement is planned. So the executor
// does not wait for a new leader, which a fenced shard never gets: it waits
// until the catalog snapshot no longer fences the target that refused it,
// plans the statement again, and runs it again. A statement in a transaction
// is retried only while nothing of the transaction has reached a pooler;
// otherwise the client gets the MTF02, which tells it to retry.

// RoutingSnapshots is the routing snapshots a fenced statement waits on: the
// gateway's sharding catalog cache.
type RoutingSnapshots interface {
	// WaitForUnfenced fetches the database's snapshot and returns once the
	// installed one has no admission fence on shard of tableGroup, or with
	// ctx's error when ctx ends first.
	WaitForUnfenced(ctx context.Context, database, tableGroup, shard string) error
}

// Backoff between the runs of a fenced statement. The first retry runs as
// soon as the snapshot shows the fence lifted; a pooler refusing again has not
// caught up with that snapshot (it has not read its fences since it started,
// or has not been refreshed since the unfence), and each later retry waits
// twice as long, up to the cap, so a pooler that is behind is not spun on. The
// fence window bounds the total.
var (
	fenceRetryBaseDelay = 10 * time.Millisecond
	fenceRetryMaxDelay  = 250 * time.Millisecond
)

// SetFenceRetry sets the routing snapshots a fenced statement waits on, and
// window, which bounds the total wait of one statement. A window that is not
// positive means a fenced statement is retried once, and only when the
// installed snapshot already shows its fence lifted. Must be called before
// connections are accepted. Without
// it statements run once: a gateway with a single pooler (minigres) has
// nothing an operation could fence.
func (e *Executor) SetFenceRetry(snapshots RoutingSnapshots, window time.Duration) {
	e.routingSnapshots = snapshots
	e.fenceWaitWindow = window
}

// Fence retry outcomes, the outcome attribute of mg.executor.fence.wait.duration.
const (
	fenceOutcomeServed        = "served"         // a retry reached a pooler that took it
	fenceOutcomeTimeout       = "timeout"        // the fence did not lift within the window
	fenceOutcomeCanceled      = "canceled"       // the statement's context ended
	fenceOutcomeInTransaction = "in_transaction" // not retried: its transaction already reached a pooler, or it is pipelined
	fenceOutcomeStreamed      = "streamed"       // not retried: output already reached the client
	fenceOutcomeRan           = "ran"            // not retried: an earlier step already ran on a pooler
)

// runFenced runs attempt, which plans the statement and runs the plan on the
// IExecute and with the callback it is given, and runs it again when a pooler
// refused it as fenced and the statement can safely run again: nothing of the
// session's transaction, if it is in one (explicit, or the implicit one of a
// multi-statement query), has reached a pooler, it is not in an
// extended-protocol pipeline before its Sync, no earlier step of the plan ran
// on a pooler, and nothing has reached the client. A transaction holds no
// reserved connection until its first statement reaches a pooler (BEGIN is
// deferred), so one refused as fenced before then is retried like a statement
// outside a transaction: a new primary that has not read its fences yet does
// not fail the transactions a failover's buffer releases to it. The last two matter for a
// plan with several steps: an earlier step may have run the user's query
// without forwarding its rows (resolve_set_config's resolve), or sent output
// the gateway produced itself, before a later step was refused.
//
// Each retry waits until the snapshot no longer fences the target that
// refused the last attempt, then runs, the first one at once and each later
// one after a growing backoff (fenceRetryBaseDelay, doubling up to
// fenceRetryMaxDelay). A snapshot that still fences the target is not worth a
// retry; one that does not can still meet a pooler that has not caught up
// with it, which the backoff waits out. The waits share one fence window,
// which is the only bound. When the window runs out the last MTF02 is
// returned, which tells the client to retry. Without routing snapshots (see
// SetFenceRetry) the statement runs once.
func (e *Executor) runFenced(
	ctx context.Context,
	conn *server.Conn,
	state *handler.MultigatewayConnectionState,
	callback func(context.Context, *sqltypes.Result) error,
	attempt func(exec engine.IExecute, callback func(context.Context, *sqltypes.Result) error) error,
) error {
	snapshots := e.routingSnapshots
	if snapshots == nil {
		return attempt(e.exec, callback)
	}
	database := conn.Database()
	guard := &attemptGuard{IExecute: e.exec, callback: callback}
	tracked := guard.forward
	err := attempt(guard, tracked)
	if err == nil || !mterrors.IsErrorCode(err, mterrors.MTF02.ID) {
		return err
	}

	start := time.Now()
	var outcome string
	defer func() {
		if e.metrics != nil {
			e.metrics.fenceWaitDuration.Record(ctx, time.Since(start).Seconds(), database, outcome)
		}
	}()
	waitCtx, cancel := context.WithTimeout(ctx, e.fenceWaitWindow)
	defer cancel()
	// waitEnded reports how a wait that ended early ends the statement.
	waitEnded := func(werr error) error {
		if ctx.Err() != nil {
			outcome = fenceOutcomeCanceled
			return mterrors.FromContext(ctx, werr)
		}
		outcome = fenceOutcomeTimeout
		return err
	}
	var delay time.Duration
	for {
		switch {
		case (state != nil && state.Pipelining()) ||
			(conn.IsInTransaction() && (state == nil || state.HasReservedConnection())):
			outcome = fenceOutcomeInTransaction
			return err
		case guard.ran:
			outcome = fenceOutcomeRan
			return err
		case guard.streamed:
			outcome = fenceOutcomeStreamed
			return err
		}
		e.logger.DebugContext(ctx, "statement was fenced; waiting for the fence to lift",
			"database", database, "tablegroup", guard.fencedTableGroup, "shard", guard.fencedShard)
		if werr := snapshots.WaitForUnfenced(waitCtx, database, guard.fencedTableGroup, guard.fencedShard); werr != nil {
			return waitEnded(werr)
		}
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-waitCtx.Done():
				t.Stop()
				return waitEnded(waitCtx.Err())
			}
		}
		delay = min(max(2*delay, fenceRetryBaseDelay), fenceRetryMaxDelay)
		if err = attempt(guard, tracked); err == nil || !mterrors.IsErrorCode(err, mterrors.MTF02.ID) {
			outcome = fenceOutcomeServed
			return err
		}
	}
}

// attemptGuard is the IExecute and callback a fenced statement's attempts run
// on. It records whether any call reached a pooler that took it (ran) and
// whether any output reached the client (streamed), the two things that make
// running the statement again unsafe. A call refused with MTF02 ran nothing;
// any other outcome, an error included, may have. It also records the target
// of the last call refused with MTF02, whose fence the retry waits on.
type attemptGuard struct {
	engine.IExecute
	callback func(context.Context, *sqltypes.Result) error
	ran      bool
	streamed bool

	fencedTableGroup string
	fencedShard      string
}

func (g *attemptGuard) forward(ctx context.Context, res *sqltypes.Result) error {
	g.streamed = true
	return g.callback(ctx, res)
}

// note records the outcome of a call to tableGroup/shard. Calls on existing
// reserved connections, which are never fenced, pass no target.
func (g *attemptGuard) note(tableGroup, shard string, err error) {
	if err == nil || !mterrors.IsErrorCode(err, mterrors.MTF02.ID) {
		g.ran = true
		return
	}
	g.fencedTableGroup, g.fencedShard = tableGroup, shard
}

func (g *attemptGuard) StreamExecute(ctx context.Context, conn *server.Conn, tableGroup, shard, sql string, eagerParsePreparedStatement *query.PreparedStatement, state *handler.MultigatewayConnectionState, info engine.PlanExecInfo, keepStructured bool, callback func(context.Context, *sqltypes.Result) error) error {
	err := g.IExecute.StreamExecute(ctx, conn, tableGroup, shard, sql, eagerParsePreparedStatement, state, info, keepStructured, callback)
	g.note(tableGroup, shard, err)
	return err
}

func (g *attemptGuard) PortalStreamExecute(ctx context.Context, tableGroup, shard string, conn *server.Conn, state *handler.MultigatewayConnectionState, portalInfo *preparedstatement.PortalInfo, maxRows int32, includeDescribe bool, info engine.PlanExecInfo, keepStructured bool, callback func(context.Context, *sqltypes.Result) error) error {
	err := g.IExecute.PortalStreamExecute(ctx, tableGroup, shard, conn, state, portalInfo, maxRows, includeDescribe, info, keepStructured, callback)
	g.note(tableGroup, shard, err)
	return err
}

func (g *attemptGuard) Describe(ctx context.Context, tableGroup, shard string, conn *server.Conn, state *handler.MultigatewayConnectionState, portalInfo *preparedstatement.PortalInfo, preparedStatementInfo *preparedstatement.PreparedStatementInfo) (*query.StatementDescription, error) {
	desc, err := g.IExecute.Describe(ctx, tableGroup, shard, conn, state, portalInfo, preparedStatementInfo)
	g.note(tableGroup, shard, err)
	return desc, err
}

func (g *attemptGuard) ConcludeTransaction(ctx context.Context, conn *server.Conn, state *handler.MultigatewayConnectionState, conclusion multipoolerpb.TransactionConclusion, releasePortalNames []string, releaseAllPortals, chain bool, callback func(context.Context, *sqltypes.Result) error) error {
	err := g.IExecute.ConcludeTransaction(ctx, conn, state, conclusion, releasePortalNames, releaseAllPortals, chain, callback)
	g.note("", "", err)
	return err
}

func (g *attemptGuard) DiscardTempTables(ctx context.Context, conn *server.Conn, state *handler.MultigatewayConnectionState, callback func(context.Context, *sqltypes.Result) error) error {
	err := g.IExecute.DiscardTempTables(ctx, conn, state, callback)
	g.note("", "", err)
	return err
}

func (g *attemptGuard) ReleaseAllReservedConnections(ctx context.Context, conn *server.Conn, state *handler.MultigatewayConnectionState, keepStickyReservations bool) error {
	err := g.IExecute.ReleaseAllReservedConnections(ctx, conn, state, keepStickyReservations)
	g.note("", "", err)
	return err
}

func (g *attemptGuard) CopyInitiate(ctx context.Context, conn *server.Conn, tableGroup, shard, queryStr string, state *handler.MultigatewayConnectionState, callback func(context.Context, *sqltypes.Result) error) (int16, []int16, error) {
	format, columnFormats, err := g.IExecute.CopyInitiate(ctx, conn, tableGroup, shard, queryStr, state, callback)
	g.note(tableGroup, shard, err)
	return format, columnFormats, err
}

func (g *attemptGuard) CopySendData(ctx context.Context, conn *server.Conn, tableGroup, shard string, state *handler.MultigatewayConnectionState, data []byte) error {
	err := g.IExecute.CopySendData(ctx, conn, tableGroup, shard, state, data)
	g.note(tableGroup, shard, err)
	return err
}

func (g *attemptGuard) CopyFinalize(ctx context.Context, conn *server.Conn, tableGroup, shard string, state *handler.MultigatewayConnectionState, finalData []byte, callback func(context.Context, *sqltypes.Result) error) error {
	err := g.IExecute.CopyFinalize(ctx, conn, tableGroup, shard, state, finalData, callback)
	g.note(tableGroup, shard, err)
	return err
}

func (g *attemptGuard) CopyAbort(ctx context.Context, conn *server.Conn, tableGroup, shard string, state *handler.MultigatewayConnectionState) error {
	err := g.IExecute.CopyAbort(ctx, conn, tableGroup, shard, state)
	g.note(tableGroup, shard, err)
	return err
}

func (g *attemptGuard) CopyOutInitiate(ctx context.Context, conn *server.Conn, tableGroup, shard, queryStr string, state *handler.MultigatewayConnectionState) (int16, []int16, []*mterrors.PgDiagnostic, error) {
	format, columnFormats, notices, err := g.IExecute.CopyOutInitiate(ctx, conn, tableGroup, shard, queryStr, state)
	g.note(tableGroup, shard, err)
	return format, columnFormats, notices, err
}

func (g *attemptGuard) CopyOutStream(ctx context.Context, conn *server.Conn, tableGroup, shard string, state *handler.MultigatewayConnectionState, onMessage func(pgClient.CopyOutMessage) error) (*sqltypes.Result, error) {
	result, err := g.IExecute.CopyOutStream(ctx, conn, tableGroup, shard, state, onMessage)
	g.note(tableGroup, shard, err)
	return result, err
}

func (g *attemptGuard) StreamReplication(ctx context.Context, conn *server.Conn, tableGroup, shard string, state *handler.MultigatewayConnectionState, init *multipoolerpb.StreamReplicationInit) (queryservice.ReplicationStream, error) {
	stream, err := g.IExecute.StreamReplication(ctx, conn, tableGroup, shard, state, init)
	g.note(tableGroup, shard, err)
	return stream, err
}

var _ engine.IExecute = (*attemptGuard)(nil)
