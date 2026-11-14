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
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/sqltypes"
)

// A pooler whose target an operation has fenced refuses new application work
// with MTF02 before running any of it. The change that lifts a fence (a
// routing switch, a revert, an unfence) always advances routing_version, and
// the statement's plan, not only its targets, may be stale: table rules apply
// when a statement is planned. So the executor does not wait for a new leader,
// which a fenced shard never gets: it waits until the catalog snapshot is
// newer than the one the refused attempt planned against, plans the statement
// again, and runs it again. As in Vitess, a statement in a transaction is not
// retried: the client gets the MTF02, which tells it to retry.

// maxFenceRetries bounds how many times one statement is run again after
// MTF02.
const maxFenceRetries = 3

// RoutingSnapshots is the routing snapshots a fenced statement waits on: the
// gateway's sharding catalog cache.
type RoutingSnapshots interface {
	// RoutingVersion returns the routing_version of the database's current
	// snapshot, 0 when there is none.
	RoutingVersion(database string) int64
	// WaitForRoutingVersion returns once the database's snapshot has a
	// routing_version greater than after, or with ctx's error when ctx ends
	// first.
	WaitForRoutingVersion(ctx context.Context, database string, after int64) error
}

// SetFenceRetry sets the routing snapshots a fenced statement waits on, and
// window, which bounds the total wait of one statement. A window that is not
// positive means a fenced statement is retried only when a newer snapshot is
// already installed. Must be called before connections are accepted. Without
// it statements run once: a gateway with a single pooler (minigres) has
// nothing an operation could fence.
func (e *Executor) SetFenceRetry(snapshots RoutingSnapshots, window time.Duration) {
	e.routingSnapshots = snapshots
	e.fenceWaitWindow = window
}

// Fence retry outcomes, the outcome attribute of mg.executor.fence.wait.duration.
const (
	fenceOutcomeServed        = "served"         // a retry reached a pooler that took it
	fenceOutcomeExhausted     = "exhausted"      // still fenced after the last retry
	fenceOutcomeTimeout       = "timeout"        // no newer snapshot within the window
	fenceOutcomeCanceled      = "canceled"       // the statement's context ended
	fenceOutcomeInTransaction = "in_transaction" // not retried: the session is in a transaction
	fenceOutcomeStreamed      = "streamed"       // not retried: output already reached the client
)

// runFenced runs attempt, which plans the statement and runs the plan with
// the callback it is given, and runs it again when a pooler refused it as
// fenced and the statement can safely run again: the session is not in a
// transaction (explicit or implicit), and nothing has reached the client.
// The latter matters for a plan with several Routes, where an earlier Route
// may have streamed rows before a later one was refused.
//
// Each retry waits for a snapshot newer than the one the refused attempt
// planned against, read before it planned. The waits share one fence window,
// and at most maxFenceRetries retries are made. When the bounds run out the
// last MTF02 is returned, which tells the client to retry. Without routing
// snapshots (see SetFenceRetry) the statement runs once.
func (e *Executor) runFenced(
	ctx context.Context,
	conn *server.Conn,
	callback func(context.Context, *sqltypes.Result) error,
	attempt func(callback func(context.Context, *sqltypes.Result) error) error,
) error {
	snapshots := e.routingSnapshots
	if snapshots == nil {
		return attempt(callback)
	}
	database := conn.Database()
	version := snapshots.RoutingVersion(database)
	streamed := false
	tracked := func(ctx context.Context, res *sqltypes.Result) error {
		streamed = true
		return callback(ctx, res)
	}
	err := attempt(tracked)
	if err == nil || !mterrors.IsErrorCode(err, mterrors.MTF02.ID) {
		return err
	}

	start := time.Now()
	outcome := fenceOutcomeExhausted
	defer func() {
		if e.metrics != nil {
			e.metrics.fenceWaitDuration.Record(ctx, time.Since(start).Seconds(), database, outcome)
		}
	}()
	waitCtx, cancel := context.WithTimeout(ctx, e.fenceWaitWindow)
	defer cancel()
	for range maxFenceRetries {
		switch {
		case conn.IsInTransaction():
			outcome = fenceOutcomeInTransaction
			return err
		case streamed:
			outcome = fenceOutcomeStreamed
			return err
		}
		e.logger.DebugContext(ctx, "statement was fenced; waiting for a newer routing snapshot",
			"database", database, "planned_routing_version", version)
		if werr := snapshots.WaitForRoutingVersion(waitCtx, database, version); werr != nil {
			if ctx.Err() != nil {
				outcome = fenceOutcomeCanceled
				return mterrors.FromContext(ctx, werr)
			}
			outcome = fenceOutcomeTimeout
			return err
		}
		version = snapshots.RoutingVersion(database)
		if err = attempt(tracked); err == nil || !mterrors.IsErrorCode(err, mterrors.MTF02.ID) {
			outcome = fenceOutcomeServed
			return err
		}
	}
	return err
}
