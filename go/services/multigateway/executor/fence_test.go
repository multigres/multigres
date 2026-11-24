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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/constants"
	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	querypb "github.com/multigres/multigres/go/pb/query"
	"github.com/multigres/multigres/go/services/multigateway/engine"
	"github.com/multigres/multigres/go/services/multigateway/handler"
)

// fencedExec fails the i-th Route execution with errs[i] (nil or past the end
// succeeds). When streamFirst is set, a failing execution streams a result to
// the callback before it fails, as an earlier Route of a plan would have.
type fencedExec struct {
	*mockExec

	mu          sync.Mutex
	errs        []error
	executions  int
	streamFirst bool
	// onCall, when set, runs at the start of the i-th execution.
	onCall func(i int)
}

func newFencedExec(errs ...error) *fencedExec {
	return &fencedExec{mockExec: &mockExec{}, errs: errs}
}

func (f *fencedExec) run(ctx context.Context, callback func(context.Context, *sqltypes.Result) error) error {
	f.mu.Lock()
	i := f.executions
	f.executions++
	var err error
	if i < len(f.errs) {
		err = f.errs[i]
	}
	onCall, streamFirst := f.onCall, f.streamFirst
	f.mu.Unlock()
	if onCall != nil {
		onCall(i)
	}
	if err != nil {
		if streamFirst {
			if cbErr := callback(ctx, &sqltypes.Result{}); cbErr != nil {
				return cbErr
			}
		}
		return err
	}
	return callback(ctx, &sqltypes.Result{})
}

func (f *fencedExec) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executions
}

func (f *fencedExec) StreamExecute(
	ctx context.Context, _ *server.Conn, _, _ string, _ string,
	_ *querypb.PreparedStatement,
	_ *handler.MultigatewayConnectionState,
	_ engine.PlanExecInfo,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return f.run(ctx, callback)
}

func (f *fencedExec) PortalStreamExecute(
	ctx context.Context, _, _ string, _ *server.Conn,
	_ *handler.MultigatewayConnectionState,
	_ *preparedstatement.PortalInfo, _ int32, _ bool,
	_ engine.PlanExecInfo,
	_ bool,
	callback func(context.Context, *sqltypes.Result) error,
) error {
	return f.run(ctx, callback)
}

// fakeSnapshots is RoutingSnapshots over whether the installed snapshot
// fences the target.
type fakeSnapshots struct {
	mu      sync.Mutex
	fenced  bool
	changed chan struct{}
	targets []string
}

func newFakeSnapshots(fenced bool) *fakeSnapshots {
	return &fakeSnapshots{fenced: fenced, changed: make(chan struct{})}
}

// lift installs a snapshot that no longer fences the target.
func (r *fakeSnapshots) lift() { r.set(false) }

// fence installs a snapshot that fences the target.
func (r *fakeSnapshots) fence() { r.set(true) }

func (r *fakeSnapshots) set(fenced bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fenced = fenced
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *fakeSnapshots) WaitForUnfenced(ctx context.Context, _, tableGroup, shard string) error {
	r.mu.Lock()
	r.targets = append(r.targets, tableGroup+"/"+shard)
	r.mu.Unlock()
	for {
		r.mu.Lock()
		fenced, changed := r.fenced, r.changed
		r.mu.Unlock()
		if !fenced {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *fakeSnapshots) waitTargets() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.targets...)
}

// waits counts the waits the statement made.
func (r *fakeSnapshots) waits() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.targets)
}

func newFenceTestExecutor(t *testing.T, exec *fencedExec, snapshots RoutingSnapshots, window time.Duration) *Executor {
	t.Helper()
	e := newTestExecutor(exec.mockExec)
	e.exec = exec
	t.Cleanup(e.planCache.Close)
	if snapshots != nil {
		e.SetFenceRetry(snapshots, window)
	}
	return e
}

func runSelect(ctx context.Context, t *testing.T, e *Executor, conn *server.Conn, callback func(context.Context, *sqltypes.Result) error) error {
	_, err := e.StreamExecute(ctx, conn, handler.NewMultigatewayConnectionState(), "SELECT 1", parseOne(t, "SELECT 1"), callback)
	return err
}

// The statement runs again only once the snapshot no longer fences the
// target, and at once then.
func TestFence_RunsAgainOnceTheFenceLifts(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(true)
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	done := make(chan error, 1)
	go func() { done <- runSelect(t.Context(), t, e, testConn(), noopCallback) }()

	require.Eventually(t, func() bool { return snapshots.waits() == 1 }, 5*time.Second, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, exec.calls(), "no retry while the snapshot fences the target")

	snapshots.lift()
	require.NoError(t, <-done)
	assert.Equal(t, 2, exec.calls())
}

// A pooler can refuse a statement whose snapshot already shows the target
// unfenced: one that has not read its fences since it started, or that has
// not been refreshed since the unfence. The statement runs again with a
// growing backoff until the pooler catches up, without waiting for any newer
// routing version.
func TestFence_RetriesAPoolerBehindTheSnapshotWithBackoff(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New(), mterrors.MTF02.New(), mterrors.MTF02.New())
	snapshots := newFakeSnapshots(false)
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	start := time.Now()
	require.NoError(t, runSelect(t.Context(), t, e, testConn(), noopCallback))
	assert.Equal(t, 4, exec.calls())
	// The first retry runs at once, the next two after 10ms and 20ms.
	assert.GreaterOrEqual(t, time.Since(start), fenceRetryBaseDelay*3)
	assert.Equal(t, 3, snapshots.waits(), "each retry checks the snapshot first")
}

// A pooler that stays behind is retried until the window runs out; the
// statement then fails with the MTF02, and the backoff keeps the retries few.
func TestFence_APoolerBehindTheSnapshotStopsAtTheWindow(t *testing.T) {
	errs := make([]error, 100)
	for i := range errs {
		errs[i] = mterrors.MTF02.New()
	}
	exec := newFencedExec(errs...)
	e := newFenceTestExecutor(t, exec, newFakeSnapshots(false), 300*time.Millisecond)

	err := runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	// 0 + 10 + 20 + 40 + 80 + 160ms fits in 300ms; the next waits 250ms.
	assert.LessOrEqual(t, exec.calls(), 8)
	assert.GreaterOrEqual(t, exec.calls(), 5)
}

// With a window that is not positive, a statement is retried once, and only
// when the snapshot already shows its fence lifted.
func TestFence_NoWindowRetriesOnceIfAlreadyLifted(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New(), mterrors.MTF02.New())
	e := newFenceTestExecutor(t, exec, newFakeSnapshots(false), 0)
	err := runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 2, exec.calls())

	exec = newFencedExec(mterrors.MTF02.New())
	e = newFenceTestExecutor(t, exec, newFakeSnapshots(true), 0)
	err = runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls(), "not retried while fenced")
}

// The portal path retries the same way.
func TestFence_PortalRunsAgain(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(true)
	exec.onCall = func(int) { snapshots.lift() }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	res, err := e.PortalStreamExecute(t.Context(), testConn(), handler.NewMultigatewayConnectionState(),
		makePortalInfo(t, "SELECT * FROM users WHERE id = $1"), 0, false, noopCallback)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 2, exec.calls())
	assert.Equal(t, 1, snapshots.waits())
}

// The statement waits on the fence of the target that refused it, and runs
// again only on a snapshot that lifts it: a gateway behind the fence first
// learns of it from a newer snapshot that still fences the target, and that
// one is not worth a retry.
func TestFence_WaitsForTheRefusingTargetsFenceToLift(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(false)
	exec.onCall = func(i int) {
		if i == 0 {
			snapshots.fence() // the gateway learns of the fence from the refusal's fetch
		}
	}
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	done := make(chan error, 1)
	go func() { done <- runSelect(t.Context(), t, e, testConn(), noopCallback) }()
	require.Eventually(t, func() bool { return snapshots.waits() == 1 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, []string{DefaultTableGroup + "/" + constants.DefaultShard}, snapshots.waitTargets(), "waits on the target that refused it")

	snapshots.fence() // a newer snapshot that still fences it
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, exec.calls(), "not run again while the snapshot still fences the target")

	snapshots.lift()
	require.NoError(t, <-done)
	assert.Equal(t, 2, exec.calls())
}

// The total wait is bounded by the window; the statement then fails with the
// MTF02, which tells the client to retry.
func TestFence_StopsAtTheWaitWindow(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	e := newFenceTestExecutor(t, exec, newFakeSnapshots(true), 50*time.Millisecond)

	start := time.Now()
	err := runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
}

// Only the window bounds the retries: a statement refused again and again
// keeps running again while the snapshot keeps lifting its fence.
func TestFence_OnlyTheWindowBoundsRetries(t *testing.T) {
	errs := make([]error, 5)
	for i := range errs {
		errs[i] = mterrors.MTF02.New()
	}
	exec := newFencedExec(errs...)
	snapshots := newFakeSnapshots(true)
	exec.onCall = func(int) { snapshots.lift() }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	require.NoError(t, runSelect(t.Context(), t, e, testConn(), noopCallback))
	assert.Equal(t, len(errs)+1, exec.calls())
}

// Without routing snapshots (minigres, which has nothing to fence, never calls
// SetFenceRetry) a statement runs once, whatever it fails with.
func TestFence_NotRetriedWithoutSnapshots(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	e := newFenceTestExecutor(t, exec, nil, 0)
	err := runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
}

// The statement's own context ending stops the wait with the error the client
// expects for it.
func TestFence_HonoursTheStatementContext(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(true)
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runSelect(ctx, t, e, testConn(), noopCallback) }()
	require.Eventually(t, func() bool { return snapshots.waits() == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	err := <-done
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSQueryCanceled), "got %v", err)
	assert.Equal(t, 1, exec.calls())
}

// A statement in a transaction that already reached a pooler is not run
// again: the transaction's earlier work is on a backend the retry cannot
// replay. The client gets the MTF02.
func TestFence_NotRetriedInATransactionThatReachedAPooler(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(true)
	exec.onCall = func(int) { snapshots.lift() }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)
	conn := testConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)
	state := handler.NewMultigatewayConnectionState()
	state.SetReservedConnection(&querypb.Target{ShardRef: &clustermetadatapb.ShardRef{TableGroup: "other", Shard: "0-inf"}},
		&querypb.ReservedState{ReservedConnectionId: 7})

	_, err := e.StreamExecute(t.Context(), conn, state, "SELECT 1", parseOne(t, "SELECT 1"), noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
	assert.Zero(t, snapshots.waits())
}

// A transaction holds no reserved connection until its first statement
// reaches a pooler (BEGIN is deferred), so that statement, refused as fenced,
// ran nothing anywhere and is retried like one outside a transaction: a new
// primary that has not read its fences yet does not fail a transaction the
// failover buffer released to it.
func TestFence_RetriedInATransactionThatReachedNoPooler(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	e := newFenceTestExecutor(t, exec, newFakeSnapshots(false), 10*time.Second)
	conn := testConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	_, err := e.StreamExecute(t.Context(), conn, handler.NewMultigatewayConnectionState(), "SELECT 1", parseOne(t, "SELECT 1"), noopCallback)
	require.NoError(t, err)
	assert.Equal(t, 2, exec.calls())
}

// A statement that already sent output to the client is not run again: an
// earlier Route of its plan may have streamed rows before a later one was
// refused.
func TestFence_NotRetriedAfterOutputReachedTheClient(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	exec.streamFirst = true
	snapshots := newFakeSnapshots(true)
	exec.onCall = func(int) { snapshots.lift() }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	var sent int
	err := runSelect(t.Context(), t, e, testConn(), func(context.Context, *sqltypes.Result) error {
		sent++
		return nil
	})
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
	assert.Equal(t, 1, sent)
	assert.Zero(t, snapshots.waits())
}

// Errors other than MTF02 are returned at once, MTF01 included: failover
// buffering is the pooler gateway's, not this retry's.
func TestFence_OtherErrorsAreNotRetried(t *testing.T) {
	for name, failure := range map[string]error{
		"MTF01":       mterrors.MTF01.New(),
		"plain error": errors.New("boom"),
		"deadlock":    mterrors.NewPgError("ERROR", "40P01", "deadlock detected", ""),
	} {
		t.Run(name, func(t *testing.T) {
			exec := newFencedExec(failure)
			snapshots := newFakeSnapshots(true)
			e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)
			err := runSelect(t.Context(), t, e, testConn(), noopCallback)
			require.Error(t, err)
			assert.False(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID))
			assert.Equal(t, 1, exec.calls())
			assert.Zero(t, snapshots.waits())
		})
	}
}

// A statement whose earlier step already ran on a pooler is not run again,
// even though nothing reached the client: resolve_set_config runs the user's
// query through its own callback before its apply step, and running the
// statement again would run that query twice.
func TestFence_NotRetriedAfterAnEarlierStepRan(t *testing.T) {
	exec := newFencedExec(nil, mterrors.MTF02.New())
	snapshots := newFakeSnapshots(true)
	exec.onCall = func(int) { snapshots.lift() }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)
	conn := testConn()
	capture := func(context.Context, *sqltypes.Result) error { return nil }

	var sent int
	err := e.runFenced(t.Context(), conn, handler.NewMultigatewayConnectionState(), func(context.Context, *sqltypes.Result) error {
		sent++
		return nil
	}, func(exec engine.IExecute, callback func(context.Context, *sqltypes.Result) error) error {
		if err := exec.StreamExecute(t.Context(), conn, "tg", "", "SELECT resolve()", nil, nil, engine.PlanExecInfo{}, true, capture); err != nil {
			return err
		}
		return exec.StreamExecute(t.Context(), conn, "tg", "", "SELECT apply()", nil, nil, engine.PlanExecInfo{}, false, callback)
	})
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 2, exec.calls(), "the resolve step is not run again")
	assert.Zero(t, sent)
	assert.Zero(t, snapshots.waits())
}

// A statement in an extended-protocol pipeline after an earlier Execute runs
// in the pipeline's implicit transaction, and is not run again.
func TestFence_NotRetriedInAPipeline(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(true)
	exec.onCall = func(int) { snapshots.lift() }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)
	state := handler.NewMultigatewayConnectionState()
	state.SetPipelining(true)

	_, err := e.PortalStreamExecute(t.Context(), testConn(), state,
		makePortalInfo(t, "SELECT * FROM users WHERE id = $1"), 0, false, noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
	assert.Zero(t, snapshots.waits())
}

// In a transaction, the Parse that opens it on a pooler is retried when the
// pooler refuses it as fenced: nothing of the transaction has run yet.
func TestFence_PrepareInTransactionRunsAgain(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	e := newFenceTestExecutor(t, exec, newFakeSnapshots(false), 10*time.Second)
	conn := testConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	require.NoError(t, e.PrepareInTransaction(t.Context(), conn, handler.NewMultigatewayConnectionState(), "SELECT $1", []uint32{23}))
	assert.Equal(t, 2, exec.calls())
}
