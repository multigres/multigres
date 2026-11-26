// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReconcile_ReturnsOutcomePerMigration(t *testing.T) {
	tc := newTestCoord(t)
	failing := tc.seed(&Migration{ID: 1, Phase: PhaseCopying})
	healthy := tc.seed(&Migration{ID: 2, Phase: PhaseCopying})
	failure := errors.New("status unavailable")
	tc.tgt.statusErrBySub = map[string]error{failing.SubscriptionName(): failure}
	tc.tgt.status = &SubscriptionStatus{TotalRelations: 1, ReadyRelations: 1}

	results, err := tc.c.Reconcile(context.Background())
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, ReconcileResult{Phase: PhaseCopying, Err: failure}, results[failing.ID])
	require.Equal(t, PhaseCopying, results[healthy.ID].Phase, "the phase is the one before the attempt, which advanced it")
	require.NoError(t, results[healthy.ID].Err, "a successful attempt is reported too")
	require.Equal(t, PhaseImporting, tc.store.migs[healthy.ID].Phase)
}

func TestReconcile_EmptyListReturnsEmptyResults(t *testing.T) {
	tc := newTestCoord(t)
	results, err := tc.c.Reconcile(context.Background())
	require.NoError(t, err)
	require.NotNil(t, results)
	require.Empty(t, results)
}

func TestReconcile_ListFailureReturnsErrorOnly(t *testing.T) {
	tc := newTestCoord(t)
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.store.listErr = errors.New("list unavailable")

	results, err := tc.c.Reconcile(context.Background())
	require.ErrorContains(t, err, "list unavailable")
	require.Nil(t, results)
}

func TestReconcile_DoesNotLog(t *testing.T) {
	tc := newTestCoord(t)
	var buf bytes.Buffer
	tc.c.logger = slog.New(slog.NewTextHandler(&buf, nil))
	tc.seed(&Migration{Phase: PhaseCopying})
	tc.tgt.statusErr = errors.New("status unavailable")

	results, err := tc.c.Reconcile(context.Background())
	require.NoError(t, err)
	require.Error(t, results[testMigID].Err)
	require.Empty(t, buf.String(), "the caller decides what to report")
}
