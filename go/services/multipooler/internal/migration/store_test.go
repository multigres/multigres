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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// fakeQS is an in-memory executor.InternalQueryService that records admin
// queries and returns a scripted result for the migration SELECT.
type fakeQS struct {
	selectResult  *sqltypes.Result
	journalResult *sqltypes.Result
	// scriptedResults maps an exact SQL string to the result QueryAdminArgs
	// returns for it; any query not in the map falls back to emptyResult() (0
	// rows), i.e. "not present" — the same shape a real pg_replication_slots miss
	// returns, so tests that want a 0-row result need not script anything.
	scriptedResults map[string]*sqltypes.Result
	adminQueries    []string
	adminArgs       []qsArgCall
	tx              *fakeTx
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
	if res, ok := f.scriptedResults[query]; ok {
		return res, nil
	}
	return emptyResult(), nil
}

func (f *fakeQS) QueryAdminArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	f.adminArgs = append(f.adminArgs, qsArgCall{sql: query, args: args})
	if res, ok := f.scriptedResults[query]; ok {
		return res, nil
	}
	if query == selectJournalSQL && f.journalResult != nil {
		return f.journalResult, nil
	}
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

func TestNullableString(t *testing.T) {
	require.Nil(t, nullableString(""))
	require.Equal(t, "nightly", nullableString("nightly"))
}

func TestStoreInsert(t *testing.T) {
	qs := &fakeQS{}
	s := NewStore(qs)
	s.cache = map[int64]*Migration{7: {ID: 7}} // Insert invalidates it

	m := &Migration{
		ID: 1, Phase: PhaseCreated,
		Name: "nightly", ConnectionID: 42, TargetDatabase: "d",
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
	m := &Migration{ID: 1, Phase: PhaseCreated, Tables: []string{"nodot"}}
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
		ID: 1, Phase: PhaseExporting,
		ConnectionID: 42, Tables: []string{"public.orders"},
	}
	require.NoError(t, s.Update(context.Background(), m))
	require.True(t, qs.tx.committed)
	require.Contains(t, qs.tx.calls[0].sql, "UPDATE multigres.migration")
	// The table set is cleared then re-inserted.
	require.Contains(t, qs.tx.calls[1].sql, "DELETE FROM multigres.migration_tables")
	require.Contains(t, qs.tx.calls[2].sql, "migration_tables")

	require.NoError(t, s.Delete(context.Background(), 1))
	require.Contains(t, qs.adminArgs[len(qs.adminArgs)-1].sql, "DELETE FROM multigres.migration")
}

// selectRow builds one migration row in selectMigrationSQL column order.
func selectRow(id int64, name, createdAt string) *sqltypes.Row {
	return &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value(strconv.FormatInt(id, 10)), // migration_id
		sqltypes.Value("IMPORTING"),               // migration_phase
		sqltypes.Value(name),                      // migration_name (COALESCE '')
		sqltypes.Value("42"),                      // connection_id
		sqltypes.Value("d"),                       // (migration_target).database
		sqltypes.Value("0"),                       // (migration_target).shard
		sqltypes.Value("default"),                 // (migration_target).table_group
		sqltypes.Value("0"),                       // sequence_margin
		sqltypes.Value("true"),                    // copy_data
		sqltypes.Value("false"),                   // skip_schema_copy
		sqltypes.Value("IMPORT"),                  // direction
		sqltypes.Value("[]"),                      // quiesce_roles
		sqltypes.Value("true"),                    // public_had_connect
		sqltypes.Value("false"),                   // quiesce_applied
		sqltypes.Value("{}"),                      // quiesce_role_conn_limits
		sqltypes.Value(""),                        // last_error
		sqltypes.Value(""),                        // reverse_link_error
		sqltypes.Value(createdAt),                 // created_at
		nil,                                       // streaming_since (NULL)
		sqltypes.Value(`["public.orders"]`),       // tables
	}}
}

func TestStoreGetByRefAndList(t *testing.T) {
	qs := &fakeQS{selectResult: &sqltypes.Result{Rows: []*sqltypes.Row{
		selectRow(1, "nightly", "2026-01-01T00:00:00Z"),
		selectRow(2, "", "2026-01-02T00:00:00Z"),
	}}}
	s := NewStore(qs)
	ctx := context.Background()

	// By id.
	m, err := s.GetByRef(ctx, Ref{ID: 1})
	require.NoError(t, err)
	require.Equal(t, "nightly", m.Name)
	require.Equal(t, []string{"public.orders"}, m.Tables)
	require.True(t, m.CopyData)

	// By name resolves to the same migration.
	byName, err := s.GetByRef(ctx, Ref{Name: "nightly"})
	require.NoError(t, err)
	require.Equal(t, int64(1), byName.ID)

	// Unknown ref, and an empty ref, are not found.
	_, err = s.GetByRef(ctx, Ref{Name: "nope"})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = s.GetByRef(ctx, Ref{})
	require.ErrorIs(t, err, ErrNotFound)

	// Get(id) works; List is ordered by creation time.
	_, err = s.Get(ctx, 2)
	require.NoError(t, err)
	all, err := s.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, int64(1), all[0].ID)
	require.Equal(t, int64(2), all[1].ID)
}

func TestScanMigration(t *testing.T) {
	m, err := scanMigration(selectRow(9, "daily", "2026-03-04T05:06:07Z"))
	require.NoError(t, err)
	require.Equal(t, int64(9), m.ID)
	require.Equal(t, "daily", m.Name)
	require.Equal(t, PhaseImporting, m.Phase)
	require.Equal(t, DirectionImport, m.Direction, "direction column round-trips into the row")
	require.Nil(t, m.StreamingSince)
	require.Equal(t, []string{"public.orders"}, m.Tables)
	require.Equal(t, "default", m.TargetTableGroup)

	_, err = scanMigration(&sqltypes.Row{Values: []sqltypes.Value{sqltypes.Value("only-one")}})
	require.Error(t, err, "too few columns must error")
}

// TestScanMigration_QuiesceRoleConnLimits covers the round-trip this guards
// against: a non-empty quiesce_role_conn_limits column (an operator-set custom
// CONNECTION LIMIT recorded before the ACTIVATE cutover zeroed it) must survive
// scanning, not just the degenerate "{}" case every other fixture row uses.
func TestScanMigration_QuiesceRoleConnLimits(t *testing.T) {
	row := selectRow(9, "daily", "2026-03-04T05:06:07Z")
	row.Values[14] = sqltypes.Value(`{"app":5,"reporting":-1}`) // quiesce_role_conn_limits
	m, err := scanMigration(row)
	require.NoError(t, err)
	require.Equal(t, map[string]int32{"app": 5, "reporting": -1}, m.QuiesceRoleConnLimits)
}

func TestStorePersistsDirection(t *testing.T) {
	ctx := context.Background()

	// With no explicit Direction, the persisted value is derived from the phase, so
	// an EXPORTING migration is stored as EXPORT.
	qs := &fakeQS{}
	require.NoError(t, NewStore(qs).Update(ctx,
		&Migration{ID: 1, Phase: PhaseExporting, ConnectionID: 42}))
	require.Contains(t, qs.tx.calls[0].sql, "direction=")
	require.Contains(t, qs.tx.calls[0].args, "EXPORT",
		"a streaming phase persists its derived direction")

	// A COMPLETING row carries no direction of its own, so the explicit Direction is
	// what gets stored — this is what lets a crashed drop finish correctly.
	qsC := &fakeQS{}
	require.NoError(t, NewStore(qsC).Update(ctx,
		&Migration{ID: 1, Phase: PhaseCompleting, Direction: DirectionExport, ConnectionID: 42}))
	require.Contains(t, qsC.tx.calls[0].args, "EXPORT",
		"COMPLETING persists the explicit direction, not the phase default")

	// Insert persists the direction too (CREATED derives IMPORT).
	qsI := &fakeQS{}
	require.NoError(t, NewStore(qsI).Insert(ctx,
		&Migration{ID: 1, Phase: PhaseCreated, ConnectionID: 42}))
	require.Contains(t, qsI.tx.calls[0].sql, "direction")
	require.Contains(t, qsI.tx.calls[0].args, "IMPORT")
}

// journalRow builds one migration_journal row in selectJournalSQL column order.
func journalRow(seq, id int64, name, event, phase, direction, fromLSN, toLSN, lastErr, detail, createdAt string) *sqltypes.Row {
	return &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value(strconv.FormatInt(seq, 10)), // seq
		sqltypes.Value(strconv.FormatInt(id, 10)),  // migration_id
		sqltypes.Value(name),                       // migration_name
		sqltypes.Value(event),                      // event
		sqltypes.Value(phase),                      // phase
		sqltypes.Value(direction),                  // direction
		sqltypes.Value(fromLSN),                    // from_lsn
		sqltypes.Value(toLSN),                      // to_lsn
		sqltypes.Value(lastErr),                    // last_error
		sqltypes.Value(detail),                     // detail
		sqltypes.Value(createdAt),                  // created_at
	}}
}

func TestStoreInsertJournal(t *testing.T) {
	qs := &fakeQS{}
	s := NewStore(qs)
	// A pre-loaded cache must NOT be disturbed by a journal append (journal is a
	// separate concern from the cached migration state).
	s.cache = map[int64]*Migration{1: {ID: 1}}

	err := s.InsertJournal(context.Background(), &JournalEntry{
		MigrationID: 1, MigrationName: "nightly", Event: JournalEventActivate,
		Phase: PhaseExporting, Direction: DirectionExport,
		FromLSN: "0/1000", ToLSN: "0/2000", Detail: "cutover",
	})
	require.NoError(t, err)

	last := qs.adminArgs[len(qs.adminArgs)-1]
	require.Contains(t, last.sql, "INSERT INTO multigres.migration_journal")
	require.Equal(t, []any{int64(1), "nightly", "ACTIVATE", "EXPORTING", "EXPORT", "0/1000", "0/2000", "", "cutover"}, last.args)
	require.NotNil(t, s.cache, "journal append leaves the migration cache intact")
}

func TestStoreListJournal(t *testing.T) {
	qs := &fakeQS{journalResult: &sqltypes.Result{Rows: []*sqltypes.Row{
		journalRow(1, 1, "nightly", "CREATE", "CREATED", "IMPORT", "", "", "", "", "2026-01-01T00:00:00Z"),
		journalRow(2, 1, "nightly", "ACTIVATE", "EXPORTING", "EXPORT", "0/1000", "0/2000", "", "cutover", "2026-01-02T00:00:00Z"),
	}}}
	s := NewStore(qs)

	entries, err := s.ListJournal(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.Equal(t, int64(1), qs.adminArgs[len(qs.adminArgs)-1].args[0], "filters by migration_id")

	require.Equal(t, int64(1), entries[0].Seq)
	require.Equal(t, int64(1), entries[0].MigrationID)
	require.Equal(t, JournalEventCreate, entries[0].Event)
	require.Equal(t, PhaseCreated, entries[0].Phase)
	require.Equal(t, DirectionImport, entries[0].Direction)

	require.Equal(t, JournalEventActivate, entries[1].Event)
	require.Equal(t, PhaseExporting, entries[1].Phase)
	require.Equal(t, DirectionExport, entries[1].Direction)
	require.Equal(t, "0/1000", entries[1].FromLSN, "handoff drained-to LSN round-trips")
	require.Equal(t, "0/2000", entries[1].ToLSN, "handoff new-writer LSN round-trips")
	require.Equal(t, "cutover", entries[1].Detail)
}

func TestScanJournalEntry(t *testing.T) {
	e, err := scanJournalEntry(journalRow(7, 9, "daily", "DEACTIVATE", "IMPORTING", "IMPORT", "0/A", "0/B", "boom", "roll back", "2026-03-04T05:06:07Z"))
	require.NoError(t, err)
	require.Equal(t, int64(7), e.Seq)
	require.Equal(t, int64(9), e.MigrationID)
	require.Equal(t, "daily", e.MigrationName)
	require.Equal(t, JournalEventDeactivate, e.Event)
	require.Equal(t, "boom", e.LastError)

	_, err = scanJournalEntry(&sqltypes.Row{Values: []sqltypes.Value{sqltypes.Value("only-one")}})
	require.Error(t, err, "too few columns must error")
}

// TestEnsureSchemaCreatesJournal proves EnsureSchema issues the journal DDL (and
// its index) alongside the migration tables.
func TestEnsureSchemaCreatesJournal(t *testing.T) {
	qs := &fakeQS{}
	require.NoError(t, NewStore(qs).EnsureSchema(context.Background()))
	joined := strings.Join(qs.adminQueries, "\n")
	require.Contains(t, joined, "CREATE TABLE IF NOT EXISTS multigres.migration_journal")
	require.Contains(t, joined, "migration_journal_migration_id_idx")
	require.NotContains(t, CreateMigrationJournalSQL, "REFERENCES",
		"journal must have no FK so it is retained after the migration row is dropped")
}
