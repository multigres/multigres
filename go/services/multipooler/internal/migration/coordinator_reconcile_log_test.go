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

package migration

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	reconcileFailedMsg    = "reconcile migration failed"
	reconcileRecoveredMsg = "reconcile migration succeeded again"
)

// logCapture routes the coordinator's logs into a buffer and counts lines.
type logCapture struct{ buf bytes.Buffer }

func (l *logCapture) count(substr string) int { return strings.Count(l.buf.String(), substr) }

// countLines counts log lines containing every one of substrs.
func (l *logCapture) countLines(substrs ...string) int {
	n := 0
	for line := range strings.SplitSeq(l.buf.String(), "\n") {
		match := true
		for _, sub := range substrs {
			match = match && strings.Contains(line, sub)
		}
		if match {
			n++
		}
	}
	return n
}

func newReconcileLogCoord(t *testing.T) (*testCoord, *logCapture) {
	t.Helper()
	tc := newTestCoord(t)
	logs := &logCapture{}
	tc.c.logger = slog.New(slog.NewTextHandler(&logs.buf, nil))
	return tc, logs
}

func reconcileN(t *testing.T, tc *testCoord, n int) {
	t.Helper()
	for range n {
		require.NoError(t, tc.c.Reconcile(context.Background()))
	}
}

func TestReconcile_RepeatedFailureLogsOnce(t *testing.T) {
	tc, logs := newReconcileLogCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.statusErr = errors.New("status unavailable")

	reconcileN(t, tc, 5)
	require.Equal(t, 1, logs.count(reconcileFailedMsg))
	require.Contains(t, logs.buf.String(), "level=WARN")

	// A different error text is a new failure.
	tc.tgt.statusErr = errors.New("status still unavailable")
	reconcileN(t, tc, 3)
	require.Equal(t, 2, logs.count(reconcileFailedMsg))
	require.Equal(t, 0, logs.count(reconcileRecoveredMsg))

	// Recovery logs once at info, and a later failure logs again.
	tc.tgt.statusErr = nil
	tc.tgt.status = &SubscriptionStatus{}
	reconcileN(t, tc, 3)
	require.Equal(t, 1, logs.count(reconcileRecoveredMsg))
	require.Equal(t, 1, logs.countLines("level=INFO", reconcileRecoveredMsg))

	tc.tgt.statusErr = errors.New("status unavailable")
	reconcileN(t, tc, 3)
	require.Equal(t, 3, logs.count(reconcileFailedMsg))
}

func TestReconcile_FailuresAreTrackedPerMigration(t *testing.T) {
	tc, logs := newReconcileLogCoord(t)
	a := tc.seed(&Migration{ID: 1, Phase: PhaseCopying})
	b := tc.seed(&Migration{ID: 2, Phase: PhaseCopying})
	failure := errors.New("status unavailable")
	tc.tgt.statusErrBySub = map[string]error{a.SubscriptionName(): failure}

	reconcileN(t, tc, 4)
	require.Equal(t, 1, logs.count(reconcileFailedMsg))

	// B starts failing with the same text while A is still failing: logged once for B.
	tc.tgt.statusErrBySub[b.SubscriptionName()] = failure
	reconcileN(t, tc, 4)
	require.Equal(t, 2, logs.count(reconcileFailedMsg))
	require.Equal(t, 1, logs.count("migration=2"))

	// A recovering does not clear B.
	delete(tc.tgt.statusErrBySub, a.SubscriptionName())
	reconcileN(t, tc, 4)
	require.Equal(t, 2, logs.count(reconcileFailedMsg))
	require.Equal(t, 1, logs.count(reconcileRecoveredMsg))
	require.Equal(t, 1, logs.countLines(reconcileRecoveredMsg, "migration=1"))
}

func TestReconcile_DisappearedMigrationLogsAfresh(t *testing.T) {
	tc, logs := newReconcileLogCoord(t)
	m := tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.statusErr = errors.New("status unavailable")

	reconcileN(t, tc, 3)
	require.Equal(t, 1, logs.count(reconcileFailedMsg))

	// Gone from the list: the entry is dropped without a recovery line.
	delete(tc.store.migs, m.ID)
	reconcileN(t, tc, 2)
	require.Equal(t, 0, logs.count(reconcileRecoveredMsg))

	// The same failure on a migration that reappears under that ID logs again.
	tc.seed(&Migration{ID: m.ID, Phase: PhaseCopying})
	reconcileN(t, tc, 3)
	require.Equal(t, 2, logs.count(reconcileFailedMsg))
}

func TestForgetCachedMigrations_ResetsReconcileFailureLog(t *testing.T) {
	tc, logs := newReconcileLogCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.statusErr = errors.New("status unavailable")

	reconcileN(t, tc, 3)
	require.Equal(t, 1, logs.count(reconcileFailedMsg))

	tc.c.ForgetCachedMigrations()
	reconcileN(t, tc, 3)
	require.Equal(t, 2, logs.count(reconcileFailedMsg), "a new primary tenure logs the standing failure afresh")
}
