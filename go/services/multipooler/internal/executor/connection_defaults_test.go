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

func TestNoteConnectionDefaultsChange(t *testing.T) {
	newExec := func() (*Executor, *stubPoolManager) {
		pm := &stubPoolManager{}
		return &Executor{logger: slog.Default(), poolManager: pm}, pm
	}
	flagged := &query.ExecuteOptions{InvalidatesConnectionDefaults: true}

	t.Run("nil options is a no-op", func(t *testing.T) {
		e, pm := newExec()
		e.noteConnectionDefaultsChange(nil, protocol.TxnStatusIdle, nil)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls)
	})

	t.Run("flag unset is a no-op", func(t *testing.T) {
		e, pm := newExec()
		e.noteConnectionDefaultsChange(&query.ExecuteOptions{}, protocol.TxnStatusIdle, nil)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls)
	})

	t.Run("autocommit regular connection bumps immediately", func(t *testing.T) {
		e, pm := newExec()
		e.noteConnectionDefaultsChange(flagged, protocol.TxnStatusIdle, nil)
		assert.Equal(t, 1, pm.invalidateDefaultsCalls)
	})

	t.Run("idle reserved connection bumps immediately", func(t *testing.T) {
		e, pm := newExec()
		spy := &pendingMarkerSpy{}
		e.noteConnectionDefaultsChange(flagged, protocol.TxnStatusIdle, spy)
		assert.Equal(t, 1, pm.invalidateDefaultsCalls)
		assert.False(t, spy.marked, "an idle (autocommitted) statement must not defer")
	})

	t.Run("in-transaction defers to COMMIT", func(t *testing.T) {
		e, pm := newExec()
		spy := &pendingMarkerSpy{}
		e.noteConnectionDefaultsChange(flagged, protocol.TxnStatusInBlock, spy)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls, "must not bump until COMMIT")
		assert.True(t, spy.marked, "must mark the reserved connection pending")
	})

	t.Run("failed transaction block defers (COMMIT becomes ROLLBACK in PG)", func(t *testing.T) {
		e, pm := newExec()
		spy := &pendingMarkerSpy{}
		e.noteConnectionDefaultsChange(flagged, protocol.TxnStatusFailed, spy)
		assert.Equal(t, 0, pm.invalidateDefaultsCalls)
		assert.True(t, spy.marked)
	})
}
