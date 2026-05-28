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

package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockBegin_RoutesQueriesThroughMatcher(t *testing.T) {
	m := NewQueryService()
	m.AddQueryPattern("INSERT INTO t.*", MakeQueryResult([]string{"ok"}, [][]any{{"1"}}))

	ctx := context.Background()
	tx, err := m.Begin(ctx)
	require.NoError(t, err)
	require.NotNil(t, tx)

	res, err := tx.Query(ctx, "INSERT INTO t VALUES (1)")
	require.NoError(t, err)
	require.NotNil(t, res)

	// QueryArgs matches on the query string just like Query.
	res, err = tx.QueryArgs(ctx, "INSERT INTO t VALUES ($1)", 2)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.NoError(t, tx.Commit(ctx))
}

func TestMockBegin_UnmatchedControlStatementsSucceedSilently(t *testing.T) {
	// No BEGIN/COMMIT/ROLLBACK patterns registered: those control statements
	// must pass without requiring callers to register them.
	m := NewQueryService()
	ctx := context.Background()

	tx, err := m.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	tx2, err := m.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx2.Rollback(ctx))
}

func TestMockBegin_SetBeginError(t *testing.T) {
	m := NewQueryService()
	wantErr := errors.New("boom")
	m.SetBeginError(wantErr)

	tx, err := m.Begin(context.Background())
	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, tx)

	// Clearing the error restores normal behavior.
	m.SetBeginError(nil)
	tx, err = m.Begin(context.Background())
	require.NoError(t, err)
	require.NotNil(t, tx)
}

func TestMockBegin_BeginPatternErrorPropagates(t *testing.T) {
	m := NewQueryService()
	m.AddQueryPatternWithError("BEGIN", errors.New("cannot begin"))

	tx, err := m.Begin(context.Background())
	require.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "cannot begin")
}

func TestMockTx_CommitAndRollbackPatternErrorsPropagate(t *testing.T) {
	ctx := context.Background()

	t.Run("commit error", func(t *testing.T) {
		m := NewQueryService()
		m.AddQueryPatternWithError("COMMIT", errors.New("commit failed"))
		tx, err := m.Begin(ctx)
		require.NoError(t, err)
		err = tx.Commit(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit failed")
	})

	t.Run("rollback error", func(t *testing.T) {
		m := NewQueryService()
		m.AddQueryPatternWithError("ROLLBACK", errors.New("rollback failed"))
		tx, err := m.Begin(ctx)
		require.NoError(t, err)
		err = tx.Rollback(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rollback failed")
	})
}

func TestMockTx_UseAfterFinished(t *testing.T) {
	ctx := context.Background()
	m := NewQueryService()

	tx, err := m.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	_, err = tx.Query(ctx, "SELECT 1")
	require.Error(t, err)
	_, err = tx.QueryArgs(ctx, "SELECT 1")
	require.Error(t, err)
	require.Error(t, tx.Commit(ctx))

	// Rollback after finish is a no-op so it is safe to defer.
	require.NoError(t, tx.Rollback(ctx))
}
