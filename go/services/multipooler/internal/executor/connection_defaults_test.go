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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/multigres/multigres/go/common/pgprotocol/protocol"
	"github.com/multigres/multigres/go/pb/query"
)

// pendingMarkerSpy is a lightweight pendingDefaultsMarker for asserting the
// defer-to-COMMIT branch without a live reserved connection.
type pendingMarkerSpy struct{ marked bool }

func (s *pendingMarkerSpy) MarkPendingDefaultsInvalidation() { s.marked = true }

func TestApplyConnectionDefaultsInvalidation(t *testing.T) {
	ctx := context.Background()
	newExec := func() (*Executor, *stubPoolManager) {
		pm := &stubPoolManager{}
		return &Executor{logger: slog.Default(), poolManager: pm}, pm
	}
	flagged := &query.ExecuteOptions{InvalidatesConnectionDefaults: true}

	t.Run("nil options is a no-op", func(t *testing.T) {
		e, pm := newExec()
		e.applyConnectionDefaultsInvalidationWithStatus(ctx, nil, protocol.TxnStatusIdle, nil, nil)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls)
	})

	t.Run("flag unset is a no-op", func(t *testing.T) {
		e, pm := newExec()
		e.applyConnectionDefaultsInvalidationWithStatus(ctx, &query.ExecuteOptions{}, protocol.TxnStatusIdle, nil, nil)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls)
	})

	t.Run("autocommit regular connection bumps immediately", func(t *testing.T) {
		e, pm := newExec()
		e.applyConnectionDefaultsInvalidationWithStatus(ctx, flagged, protocol.TxnStatusIdle, nil, nil)
		assert.Equal(t, 1, pm.invalidateDefaultsCalls)
	})

	t.Run("idle reserved connection bumps immediately", func(t *testing.T) {
		e, pm := newExec()
		spy := &pendingMarkerSpy{}
		e.applyConnectionDefaultsInvalidationWithStatus(ctx, flagged, protocol.TxnStatusIdle, spy, nil)
		assert.Equal(t, 1, pm.invalidateDefaultsCalls)
		assert.False(t, spy.marked, "an idle (autocommitted) statement must not defer")
	})

	t.Run("in-transaction defers to COMMIT", func(t *testing.T) {
		e, pm := newExec()
		spy := &pendingMarkerSpy{}
		e.applyConnectionDefaultsInvalidationWithStatus(ctx, flagged, protocol.TxnStatusInBlock, spy, nil)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls, "must not bump until COMMIT")
		assert.True(t, spy.marked, "must mark the reserved connection pending")
	})

	t.Run("failed transaction block defers (COMMIT becomes ROLLBACK in PG)", func(t *testing.T) {
		e, pm := newExec()
		spy := &pendingMarkerSpy{}
		e.applyConnectionDefaultsInvalidationWithStatus(ctx, flagged, protocol.TxnStatusFailed, spy, nil)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls)
		assert.True(t, spy.marked)
	})
}

func TestShouldBumpDefaultsAfterCommit(t *testing.T) {
	assert.False(t, shouldBumpDefaultsAfterCommit(false, protocol.TxnStatusIdle))
	assert.False(t, shouldBumpDefaultsAfterCommit(false, protocol.TxnStatusFailed))
	assert.True(t, shouldBumpDefaultsAfterCommit(true, protocol.TxnStatusIdle))
	assert.True(t, shouldBumpDefaultsAfterCommit(true, protocol.TxnStatusInBlock))
	assert.False(t, shouldBumpDefaultsAfterCommit(true, protocol.TxnStatusFailed),
		"COMMIT on an aborted block must not bump pool defaults")
}
