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
// with MTF02 before running any of it. The change that lifts a fence (a
// routing switch, a revert, an unfence) always advances routing_version, and
// the statement's plan, not only its targets, may be stale: table rules apply
// when a statement is planned. So the executor does not wait for a new leader,
// which a fenced shard never gets: it waits until the catalog snapshot is
// newer than the one the refused attempt planned against and no longer fences
// the target that refused it, plans the statement again, and runs it again.
// As in Vitess, a statement in a transaction is not retried: the client gets
// the MTF02, which tells it to retry.

// RoutingSnapshots is the routing snapshots a fenced statement waits on: the
// gateway's sharding catalog cache.
type RoutingSnapshots interface {
	// RoutingVersion returns the routing_version of the database's current
	// snapshot, 0 when there is none.
	RoutingVersion(database string) int64
	// WaitForUnfenced returns once the database's snapshot has a
	// routing_version greater than after and no admission fence on shard of
	// tableGroup, or with ctx's error when ctx ends first.
	WaitForUnfenced(ctx context.Context, database, tableGroup, shard string, after int64) error
}

// SetFenceRetry sets the routing snapshots a fenced statement waits on, and
// window, which bounds the total wait of one statement. A window that is not
// positive means a fenced statement is retried only when a snapshot that lifts
// its fence is already installed. Must be called before connections are accepted. Without
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
	fenceOutcomeInTransaction = "in_transaction" // not retried: the session is in a transaction
	fenceOutcomeStreamed      = "streamed"       // not retried: output already reached the client
	fenceOutcomeRan           = "ran"            // not retried: an earlier step already ran on a pooler
)

// runFenced runs attempt, which plans the statement and runs the plan on the
// IExecute and with the callback it is given, and runs it again when a pooler
// refused it as fenced and the statement can safely run again: the session is
// not in a transaction, explicit or implicit (a multi-statement query, or an
// extended-protocol pipeline before its Sync), no earlier step of the plan ran
// on a pooler, and nothing has reached the client. The last two matter for a
// plan with several steps: an earlier step may have run the user's query
// without forwarding its rows (resolve_set_config's resolve), or sent output
// the gateway produced itself, before a later step was refused.
//
// Each retry waits for a snapshot newer than the one the refused attempt
// planned against, read before it planned, in which the target that refused
// it is no longer fenced. A cutover writes several routing versions (raise
// the fence, change the rule, lift the fence), so waiting for any newer one
// would spend retries on snapshots that still fence the target. The waits
// share one fence window, which is the only bound; each retry needs a newer
// snapshot, so the statement cannot spin. When the window runs out the last
// MTF02 is returned, which tells the client to retry. Without routing
// snapshots (see SetFenceRetry) the statement runs once.
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
	version := snapshots.RoutingVersion(database)
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
	for {
		switch {
		case conn.IsInTransaction() || (state != nil && state.Pipelining()):
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
			"database", database, "tablegroup", guard.fencedTableGroup, "shard", guard.fencedShard,
			"planned_routing_version", version)
		if werr := snapshots.WaitForUnfenced(waitCtx, database, guard.fencedTableGroup, guard.fencedShard, version); werr != nil {
			if ctx.Err() != nil {
				outcome = fenceOutcomeCanceled
				return mterrors.FromContext(ctx, werr)
			}
			outcome = fenceOutcomeTimeout
			return err
		}
		version = snapshots.RoutingVersion(database)
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
