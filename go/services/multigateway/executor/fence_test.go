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

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/common/pgprotocol/server"
	"github.com/multigres/multigres/go/common/preparedstatement"
	"github.com/multigres/multigres/go/common/sqltypes"
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

// fakeSnapshots is RoutingSnapshots over a settable routing_version.
type fakeSnapshots struct {
	mu      sync.Mutex
	version int64
	changed chan struct{}
	waits   []int64
}

func newFakeSnapshots(version int64) *fakeSnapshots {
	return &fakeSnapshots{version: version, changed: make(chan struct{})}
}

func (r *fakeSnapshots) setVersion(v int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.version = v
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *fakeSnapshots) RoutingVersion(string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.version
}

func (r *fakeSnapshots) WaitForRoutingVersion(ctx context.Context, _ string, after int64) error {
	r.mu.Lock()
	r.waits = append(r.waits, after)
	r.mu.Unlock()
	for {
		r.mu.Lock()
		v, changed := r.version, r.changed
		r.mu.Unlock()
		if v > after {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *fakeSnapshots) waitAfters() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.waits...)
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

// The statement runs again only once a snapshot newer than the one it
// planned against is installed.
func TestFence_RunsAgainOnANewerSnapshot(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(5)
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	done := make(chan error, 1)
	go func() { done <- runSelect(t.Context(), t, e, testConn(), noopCallback) }()

	require.Eventually(t, func() bool { return len(snapshots.waitAfters()) == 1 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, []int64{5}, snapshots.waitAfters(), "waits past the version it planned against")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, exec.calls(), "no retry on the snapshot already used")

	snapshots.setVersion(6)
	require.NoError(t, <-done)
	assert.Equal(t, 2, exec.calls())
}

// The portal path retries the same way.
func TestFence_PortalRunsAgain(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(5)
	exec.onCall = func(int) { snapshots.setVersion(6) }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	res, err := e.PortalStreamExecute(t.Context(), testConn(), handler.NewMultigatewayConnectionState(),
		makePortalInfo(t, "SELECT * FROM users WHERE id = $1"), 0, false, noopCallback)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 2, exec.calls())
	assert.Equal(t, []int64{5}, snapshots.waitAfters())
}

// A gateway a version behind the fence first runs again on the snapshot that
// fenced the target, is refused again, and is served on the next one, which
// lifts the fence.
func TestFence_OneVersionBehindRunsTwiceMore(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New(), mterrors.MTF02.New())
	snapshots := newFakeSnapshots(6)
	exec.onCall = func(i int) { snapshots.setVersion(int64(7 + i)) }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	require.NoError(t, runSelect(t.Context(), t, e, testConn(), noopCallback))
	assert.Equal(t, 3, exec.calls())
	assert.Equal(t, []int64{6, 7}, snapshots.waitAfters())
}

// The total wait is bounded by the window; the statement then fails with the
// MTF02, which tells the client to retry.
func TestFence_StopsAtTheWaitWindow(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	e := newFenceTestExecutor(t, exec, newFakeSnapshots(5), 50*time.Millisecond)

	start := time.Now()
	err := runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
}

// Retries are bounded even while the snapshot keeps moving.
func TestFence_StopsAtTheRetryBound(t *testing.T) {
	errs := make([]error, maxFenceRetries+2)
	for i := range errs {
		errs[i] = mterrors.MTF02.New()
	}
	exec := newFencedExec(errs...)
	snapshots := newFakeSnapshots(0)
	exec.onCall = func(i int) { snapshots.setVersion(int64(i + 1)) }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	err := runSelect(t.Context(), t, e, testConn(), noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1+maxFenceRetries, exec.calls())
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
	snapshots := newFakeSnapshots(5)
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runSelect(ctx, t, e, testConn(), noopCallback) }()
	require.Eventually(t, func() bool { return len(snapshots.waitAfters()) == 1 }, 5*time.Second, time.Millisecond)
	cancel()
	err := <-done
	assert.True(t, mterrors.IsErrorCode(err, mterrors.PgSSQueryCanceled), "got %v", err)
	assert.Equal(t, 1, exec.calls())
}

// A statement in a transaction, explicit or implicit, is not run again: the
// client gets the MTF02 and retries the transaction.
func TestFence_NotRetriedInATransaction(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	snapshots := newFakeSnapshots(5)
	exec.onCall = func(int) { snapshots.setVersion(6) }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)
	conn := testConn()
	conn.SetTxnStatus(protocol.TxnStatusInBlock)

	err := runSelect(t.Context(), t, e, conn, noopCallback)
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
	assert.Empty(t, snapshots.waitAfters())
}

// A statement that already sent output to the client is not run again: an
// earlier Route of its plan may have streamed rows before a later one was
// refused.
func TestFence_NotRetriedAfterOutputReachedTheClient(t *testing.T) {
	exec := newFencedExec(mterrors.MTF02.New())
	exec.streamFirst = true
	snapshots := newFakeSnapshots(5)
	exec.onCall = func(int) { snapshots.setVersion(6) }
	e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)

	var sent int
	err := runSelect(t.Context(), t, e, testConn(), func(context.Context, *sqltypes.Result) error {
		sent++
		return nil
	})
	assert.True(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID), "got %v", err)
	assert.Equal(t, 1, exec.calls())
	assert.Equal(t, 1, sent)
	assert.Empty(t, snapshots.waitAfters())
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
			snapshots := newFakeSnapshots(5)
			e := newFenceTestExecutor(t, exec, snapshots, 10*time.Second)
			err := runSelect(t.Context(), t, e, testConn(), noopCallback)
			require.Error(t, err)
			assert.False(t, mterrors.IsErrorCode(err, mterrors.MTF02.ID))
			assert.Equal(t, 1, exec.calls())
			assert.Empty(t, snapshots.waitAfters())
		})
	}
}
