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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// fakeQS is an in-memory executor.InternalQueryService that records admin
// queries and returns a scripted result for the migration SELECT.
type fakeQS struct {
	selectResult *sqltypes.Result
	adminQueries []string
	adminArgs    []qsArgCall
	tx           *fakeTx
}

type qsArgCall struct {
	sql  string
	args []any
}

func emptyResult() *sqltypes.Result { return &sqltypes.Result{} }

func (f *fakeQS) QueryAdmin(_ context.Context, query string) (*sqltypes.Result, error) {
	f.adminQueries = append(f.adminQueries, query)
	if query == selectMigrationSQL && f.selectResult != nil {
		return f.selectResult, nil
	}
	return emptyResult(), nil
}

func (f *fakeQS) QueryAdminArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	f.adminArgs = append(f.adminArgs, qsArgCall{sql: query, args: args})
	return emptyResult(), nil
}

func (f *fakeQS) BeginAdmin(context.Context) (executor.InternalTx, error) {
	if f.tx == nil {
		f.tx = &fakeTx{}
	}
	return f.tx, nil
}

// Unused-by-store methods, present to satisfy the interface.
func (f *fakeQS) Query(context.Context, string) (*sqltypes.Result, error) {
	return emptyResult(), nil
}

func (f *fakeQS) QueryArgs(context.Context, string, ...any) (*sqltypes.Result, error) {
	return emptyResult(), nil
}
func (f *fakeQS) QueryMultiStatement(context.Context, string) error      { return nil }
func (f *fakeQS) QueryAdminMultiStatement(context.Context, string) error { return nil }
func (f *fakeQS) Begin(context.Context) (executor.InternalTx, error)     { return &fakeTx{}, nil }

type fakeTx struct {
	calls      []qsArgCall
	committed  bool
	rolledBack bool
}

func (t *fakeTx) QueryArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	t.calls = append(t.calls, qsArgCall{sql: query, args: args})
	return emptyResult(), nil
}

func (t *fakeTx) Query(_ context.Context, query string) (*sqltypes.Result, error) {
	t.calls = append(t.calls, qsArgCall{sql: query})
	return emptyResult(), nil
}
func (t *fakeTx) Commit(context.Context) error   { t.committed = true; return nil }
func (t *fakeTx) Rollback(context.Context) error { t.rolledBack = true; return nil }
func (t *fakeTx) QueryWithRetry(context.Context, string) ([]*sqltypes.Result, error) {
	return nil, nil
}

func (t *fakeTx) QueryArgsWithRetry(context.Context, string, ...any) ([]*sqltypes.Result, error) {
	return nil, nil
}

func TestNullableName(t *testing.T) {
	require.Nil(t, nullableName(""))
	require.Equal(t, "nightly", nullableName("nightly"))
}

func TestStoreInsert(t *testing.T) {
	qs := &fakeQS{}
	s := NewStore(qs)
	s.cache = map[string]*Migration{"stale": {ID: "stale"}} // Insert invalidates it

	m := &Migration{
		ID: "m1", Phase: PhaseCreated,
		Name: "nightly", SourceDSN: "host=h dbname=d", TargetDatabase: "d",
		Tables: []string{"public.orders", "public.items"}, CopyData: true,
	}
	require.NoError(t, s.Insert(context.Background(), m))

	require.True(t, qs.tx.committed) // the deferred Rollback after Commit is a no-op
	require.Contains(t, qs.tx.calls[0].sql, "INSERT INTO multigres.migration")
	// One row per table after the migration row.
	require.Contains(t, qs.tx.calls[1].sql, "migration_tables")
	require.Contains(t, qs.tx.calls[2].sql, "migration_tables")
	require.Nil(t, s.cache, "Insert invalidates the cache")
}

func TestStoreInsertRejectsBadTableName(t *testing.T) {
	qs := &fakeQS{}
	s := NewStore(qs)
	m := &Migration{ID: "m1", Phase: PhaseCreated, Tables: []string{"nodot"}}
	err := s.Insert(context.Background(), m)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected schema.table")
	require.False(t, qs.tx.committed, "a failed insert must not commit")
	require.True(t, qs.tx.rolledBack, "a failed insert rolls back")
}

func TestStoreUpdateAndDelete(t *testing.T) {
	qs := &fakeQS{}
	s := NewStore(qs)
	m := &Migration{
		ID: "m1", Phase: PhaseExporting,
		SourceDSN: "host=h dbname=d", Tables: []string{"public.orders"},
	}
	require.NoError(t, s.Update(context.Background(), m))
	require.True(t, qs.tx.committed)
	require.Contains(t, qs.tx.calls[0].sql, "UPDATE multigres.migration")
	// The table set is cleared then re-inserted.
	require.Contains(t, qs.tx.calls[1].sql, "DELETE FROM multigres.migration_tables")
	require.Contains(t, qs.tx.calls[2].sql, "migration_tables")

	require.NoError(t, s.Delete(context.Background(), "m1"))
	require.Contains(t, qs.adminArgs[len(qs.adminArgs)-1].sql, "DELETE FROM multigres.migration")
}

// selectRow builds one migration row in selectMigrationSQL column order.
func selectRow(id, name, createdAt string) *sqltypes.Row {
	return &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value(id),                  // migration_id
		sqltypes.Value("IMPORTING"),         // phase
		sqltypes.Value(name),                // name (COALESCE '')
		sqltypes.Value("host=h dbname=d"),   // source_dsn
		sqltypes.Value("d"),                 // target_database
		sqltypes.Value("0"),                 // target_shard
		sqltypes.Value("0"),                 // sequence_margin
		sqltypes.Value("true"),              // copy_data
		sqltypes.Value("false"),             // skip_schema_copy
		sqltypes.Value("IMPORT"),            // direction
		sqltypes.Value(""),                  // last_error
		sqltypes.Value(createdAt),           // created_at
		nil,                                 // streaming_since (NULL)
		sqltypes.Value(`["public.orders"]`), // tables
	}}
}

func TestStoreGetByRefAndList(t *testing.T) {
	qs := &fakeQS{selectResult: &sqltypes.Result{Rows: []*sqltypes.Row{
		selectRow("m1", "nightly", "2026-01-01T00:00:00Z"),
		selectRow("m2", "", "2026-01-02T00:00:00Z"),
	}}}
	s := NewStore(qs)
	ctx := context.Background()

	// By id.
	m, err := s.GetByRef(ctx, "m1")
	require.NoError(t, err)
	require.Equal(t, "nightly", m.Name)
	require.Equal(t, []string{"public.orders"}, m.Tables)
	require.True(t, m.CopyData)

	// By name resolves to the same migration.
	byName, err := s.GetByRef(ctx, "nightly")
	require.NoError(t, err)
	require.Equal(t, "m1", byName.ID)

	// Unknown ref, and an empty ref, are not found.
	_, err = s.GetByRef(ctx, "nope")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = s.GetByRef(ctx, "")
	require.ErrorIs(t, err, ErrNotFound)

	// Get(id) works; List is ordered by creation time.
	_, err = s.Get(ctx, "m2")
	require.NoError(t, err)
	all, err := s.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, "m1", all[0].ID)
	require.Equal(t, "m2", all[1].ID)
}

func TestScanMigration(t *testing.T) {
	m, err := scanMigration(selectRow("m9", "daily", "2026-03-04T05:06:07Z"))
	require.NoError(t, err)
	require.Equal(t, "m9", m.ID)
	require.Equal(t, "daily", m.Name)
	require.Equal(t, PhaseImporting, m.Phase)
	require.Equal(t, DirectionImport, m.Direction, "direction column round-trips into the row")
	require.Nil(t, m.StreamingSince)
	require.Equal(t, []string{"public.orders"}, m.Tables)

	_, err = scanMigration(&sqltypes.Row{Values: []sqltypes.Value{sqltypes.Value("only-one")}})
	require.Error(t, err, "too few columns must error")
}

func TestStorePersistsDirection(t *testing.T) {
	ctx := context.Background()

	// With no explicit Direction, the persisted value is derived from the phase, so
	// an EXPORTING migration is stored as EXPORT.
	qs := &fakeQS{}
	require.NoError(t, NewStore(qs).Update(ctx,
		&Migration{ID: "m1", Phase: PhaseExporting, SourceDSN: "host=h dbname=d"}))
	require.Contains(t, qs.tx.calls[0].sql, "direction=")
	require.Contains(t, qs.tx.calls[0].args, "EXPORT",
		"a streaming phase persists its derived direction")

	// A COMPLETING row carries no direction of its own, so the explicit Direction is
	// what gets stored — this is what lets a crashed drop finish correctly.
	qsC := &fakeQS{}
	require.NoError(t, NewStore(qsC).Update(ctx,
		&Migration{ID: "m1", Phase: PhaseCompleting, Direction: DirectionExport, SourceDSN: "host=h dbname=d"}))
	require.Contains(t, qsC.tx.calls[0].args, "EXPORT",
		"COMPLETING persists the explicit direction, not the phase default")

	// Insert persists the direction too (CREATED derives IMPORT).
	qsI := &fakeQS{}
	require.NoError(t, NewStore(qsI).Insert(ctx,
		&Migration{ID: "m1", Phase: PhaseCreated, SourceDSN: "host=h dbname=d"}))
	require.Contains(t, qsI.tx.calls[0].sql, "direction")
	require.Contains(t, qsI.tx.calls[0].args, "IMPORT")
}
