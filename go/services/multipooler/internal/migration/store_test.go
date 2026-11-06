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
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
	"github.com/multigres/multigres/go/services/multipooler/internal/shardref"
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
	// scriptedErrs maps an exact SQL string to the error QueryAdmin and
	// QueryAdminArgs return for it.
	scriptedErrs map[string]error
	adminQueries []string
	adminArgs    []qsArgCall
	// noQueryTextQueries records calls through QueryAdminNoQueryText, kept
	// separate from adminQueries so a test can assert a query ran through the
	// secret-safe path rather than the traced one.
	noQueryTextQueries []string
	tx                 *fakeTx
}

type qsArgCall struct {
	sql  string
	args []any
}

func emptyResult() *sqltypes.Result { return &sqltypes.Result{} }

func (f *fakeQS) QueryAdmin(_ context.Context, query string) (*sqltypes.Result, error) {
	f.adminQueries = append(f.adminQueries, query)
	if err, ok := f.scriptedErrs[query]; ok {
		return nil, err
	}
	if query == selectMigrationSQL && f.selectResult != nil {
		return f.selectResult, nil
	}
	if res, ok := f.scriptedResults[query]; ok {
		return res, nil
	}
	return emptyResult(), nil
}

// QueryAdminNoQueryText records into noQueryTextQueries, not adminQueries: a
// test asserting on adminQueries alone would otherwise miss that a query ran
// through the wrong (tracing) path, defeating the point of this fake.
func (f *fakeQS) QueryAdminNoQueryText(_ context.Context, query string) (*sqltypes.Result, error) {
	f.noQueryTextQueries = append(f.noQueryTextQueries, query)
	if res, ok := f.scriptedResults[query]; ok {
		return res, nil
	}
	return emptyResult(), nil
}

func (f *fakeQS) QueryAdminArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	f.adminArgs = append(f.adminArgs, qsArgCall{sql: query, args: args})
	if err, ok := f.scriptedErrs[query]; ok {
		return nil, err
	}
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
	// scriptedErrs maps an exact SQL string to the error QueryArgs returns for it.
	scriptedErrs map[string]error
}

func (t *fakeTx) QueryArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	t.calls = append(t.calls, qsArgCall{sql: query, args: args})
	if err, ok := t.scriptedErrs[query]; ok {
		return nil, err
	}
	return emptyResult(), nil
}

func (t *fakeTx) Query(_ context.Context, query string) (*sqltypes.Result, error) {
	t.calls = append(t.calls, qsArgCall{sql: query})
	return emptyResult(), nil
}
func (t *fakeTx) Commit(context.Context) error   { t.committed = true; return nil }
func (t *fakeTx) Rollback(context.Context) error { t.rolledBack = true; return nil }
func (t *fakeTx) StatelessQuery(context.Context, string) ([]*sqltypes.Result, error) {
	return nil, nil
}

func (t *fakeTx) StatelessQueryArgs(context.Context, string, ...any) ([]*sqltypes.Result, error) {
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

// noLegacyShardRefResult answers shardref.CheckLegacyTypeSQL for a shard with
// neither the legacy multigres.shard_key composite nor multigres.shard_ref.
func noLegacyShardRefResult() *sqltypes.Result {
	return &sqltypes.Result{Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value("f"), sqltypes.Value("f")}}}}
}

// boolResult is a one-row, one-column boolean result.
func boolResult(v bool) *sqltypes.Result {
	val := "f"
	if v {
		val = "t"
	}
	return &sqltypes.Result{Rows: []*sqltypes.Row{{Values: []sqltypes.Value{sqltypes.Value(val)}}}}
}

// TestEnsureSchemaCreatesStatViewOnlyWhereConnectionsExist proves the foreign
// key into multigres.connections and the stat_migration view, which joins it,
// are created where that table exists and skipped, with ErrNoConnectionsTable,
// where it does not. The migration tables themselves are created on both.
func TestEnsureSchemaCreatesStatViewOnlyWhereConnectionsExist(t *testing.T) {
	for _, tc := range []struct {
		name            string
		haveConnections bool
	}{{"catalog shard", true}, {"other shard", false}} {
		t.Run(tc.name, func(t *testing.T) {
			qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
				shardref.CheckLegacyTypeSQL: noLegacyShardRefResult(),
				connectionsPresentSQL:       boolResult(tc.haveConnections),
			}}
			err := NewStore(qs).EnsureSchema(context.Background())
			if tc.haveConnections {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrNoConnectionsTable, "the caller decides whether that is expected")
			}
			joined := strings.Join(qs.adminQueries, "\n")
			require.Contains(t, joined, "CREATE TABLE IF NOT EXISTS multigres.migration ")
			require.NotContains(t, joined, "multigres.migration_connection", "no shard creates multigres.migration_connection")
			require.Equal(t, tc.haveConnections, strings.Contains(joined, AddConnectionForeignKeySQL), "the foreign key only where connections exists")
			require.Equal(t, tc.haveConnections, strings.Contains(joined, CreateStatMigrationViewSQL))
			require.Equal(t, tc.haveConnections, strings.Contains(joined, GrantStatMigrationSQL))
		})
	}
}

// TestAddConnectionForeignKeyIsGuardedAndNotValid: the foreign key is added only when it is
// missing (so a run where it exists takes no lock), NOT VALID (so an old dangling row does not
// fail the ensure), and names its relations with to_regclass, not a ::regclass cast, which is
// resolved at plan time and raises when a relation is missing.
func TestAddConnectionForeignKeyIsGuardedAndNotValid(t *testing.T) {
	fk := AddConnectionForeignKeySQL
	require.Contains(t, fk, "FOREIGN KEY (connection_id)")
	require.Contains(t, fk, "REFERENCES multigres.connections (connection_id)")
	require.Contains(t, fk, "IF NOT EXISTS (")
	require.Contains(t, fk, "conrelid = to_regclass('multigres.migration')")
	require.Contains(t, fk, "confrelid = to_regclass('multigres.connections')")
	require.NotContains(t, fk, "::regclass")
	require.Contains(t, fk, "NOT VALID")
}

// TestEnsureSchemaAddsTheForeignKeyAfterTheMigrationTable: the key is added once multigres.migration
// exists, and before the view.
func TestEnsureSchemaAddsTheForeignKeyAfterTheMigrationTable(t *testing.T) {
	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
		shardref.CheckLegacyTypeSQL: noLegacyShardRefResult(),
		connectionsPresentSQL:       boolResult(true),
	}}
	require.NoError(t, NewStore(qs).EnsureSchema(context.Background()))
	idx := func(q string) int {
		for i, got := range qs.adminQueries {
			if got == q {
				return i
			}
		}
		return -1
	}
	table, fk, view := idx(CreateMigrationSQL), idx(AddConnectionForeignKeySQL), idx(CreateStatMigrationViewSQL)
	require.GreaterOrEqual(t, table, 0)
	require.Greater(t, fk, table)
	require.Greater(t, view, fk)
}

// TestMigrationHasNoForeignKeyToConnections pins that multigres.migration, which
// every shard creates, does not reference the catalog-only connections table.
func TestMigrationHasNoForeignKeyToConnections(t *testing.T) {
	require.NotContains(t, CreateMigrationSQL, "REFERENCES")
	require.Contains(t, CreateMigrationSQL, "connection_id BIGINT NOT NULL,")
}

// TestEnsureSchemaCreatesJournal proves EnsureSchema issues the journal DDL (and
// its index) alongside the migration tables.
func TestEnsureSchemaCreatesJournal(t *testing.T) {
	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
		shardref.CheckLegacyTypeSQL: noLegacyShardRefResult(),
		connectionsPresentSQL:       boolResult(true),
	}}
	require.NoError(t, NewStore(qs).EnsureSchema(context.Background()))
	joined := strings.Join(qs.adminQueries, "\n")
	require.Contains(t, joined, shardref.CreateTypeSQL, "the shared shard_ref type is ensured first")
	require.Contains(t, joined, "multigres.shard_ref NOT NULL", "migration_target is typed shard_ref")
	require.Contains(t, joined, "CREATE TABLE IF NOT EXISTS multigres.migration_journal")
	require.Contains(t, joined, "migration_journal_migration_id_idx")
	require.NotContains(t, CreateMigrationJournalSQL, "REFERENCES",
		"journal must have no FK so it is retained after the migration row is dropped")
}

func connectionRow(id int64, name, dsn string) *sqltypes.Row {
	return &sqltypes.Row{Values: []sqltypes.Value{
		sqltypes.Value(strconv.FormatInt(id, 10)), // connection_id
		sqltypes.Value(name),                      // name
		sqltypes.Value(dsn),                       // dsn
		sqltypes.Value(""),                        // owner_role
		sqltypes.Value("2026-01-01T00:00:00Z"),    // created_at
	}}
}

func TestStoreConnectionsUseTheSharedTable(t *testing.T) {
	ctx := context.Background()

	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{
		selectConnectionByIDSQL:   {Rows: []*sqltypes.Row{connectionRow(7, "src", "host=h dbname=app")}},
		selectConnectionByNameSQL: {Rows: []*sqltypes.Row{connectionRow(7, "src", "host=h dbname=app")}},
		selectConnectionsSQL:      {Rows: []*sqltypes.Row{connectionRow(9, "b", "host=b"), connectionRow(7, "a", "host=a")}},
	}}
	s := NewStore(qs)

	// The id is assigned by the caller, not by the table: the migrator keeps
	// its own id scheme.
	require.NoError(t, s.InsertConnection(ctx, &Connection{ID: 7, Name: "src", DSN: "host=h", OwnerRole: "alice"}))
	require.Len(t, qs.adminArgs, 1)
	require.Contains(t, qs.adminArgs[0].sql, "INSERT INTO multigres.connections")
	require.Equal(t, []any{int64(7), "src", "host=h", "alice"}, qs.adminArgs[0].args)

	c, err := s.GetConnectionByRef(ctx, Ref{ID: 7})
	require.NoError(t, err)
	require.Equal(t, "src", c.Name)
	c, err = s.GetConnectionByRef(ctx, Ref{Name: "src"})
	require.NoError(t, err)
	require.Equal(t, int64(7), c.ID)
	_, err = s.GetConnectionByRef(ctx, Ref{})
	require.ErrorIs(t, err, ErrConnectionNotFound)

	all, err := s.ListConnections(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, []int64{7, 9}, []int64{all[0].ID, all[1].ID}, "listed by id")

	for _, q := range qs.adminArgs {
		require.NotContains(t, q.sql, "migration_connection")
	}
}

// deleteResult is the (deleted, referenced) row deleteConnectionSQL returns.
func deleteResult(deleted, referenced bool) *sqltypes.Result {
	val := func(b bool) sqltypes.Value {
		if b {
			return sqltypes.Value("t")
		}
		return sqltypes.Value("f")
	}
	return &sqltypes.Result{Rows: []*sqltypes.Row{{Values: []sqltypes.Value{val(deleted), val(referenced)}}}}
}

func TestStoreDeleteConnection(t *testing.T) {
	ctx := context.Background()

	t.Run("deleted", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{deleteConnectionSQL: deleteResult(true, false)}}
		require.NoError(t, NewStore(qs).DeleteConnection(ctx, 7))
		require.Len(t, qs.adminArgs, 1, "one statement decides the outcome")
		require.Equal(t, []any{int64(7)}, qs.adminArgs[0].args)
	})

	t.Run("a migration references it", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{deleteConnectionSQL: deleteResult(false, true)}}
		err := NewStore(qs).DeleteConnection(ctx, 7)
		require.ErrorIs(t, err, ErrConnectionInUse)
		require.Len(t, qs.adminArgs, 1, "no follow-up probe")
	})

	t.Run("no such connection", func(t *testing.T) {
		qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{deleteConnectionSQL: deleteResult(false, false)}}
		err := NewStore(qs).DeleteConnection(ctx, 7)
		require.ErrorIs(t, err, ErrConnectionNotFound)
		require.Len(t, qs.adminArgs, 1, "no follow-up probe")
	})

	t.Run("the reference check and the delete are one statement", func(t *testing.T) {
		require.Contains(t, deleteConnectionSQL, "DELETE FROM multigres.connections")
		require.Contains(t, deleteConnectionSQL, "FROM multigres.migration WHERE connection_id")
		require.Contains(t, deleteConnectionSQL, "NOT (SELECT referenced FROM ref)")
	})
}

// foreignKeyViolation is PostgreSQL's foreign_key_violation on the named constraint.
func foreignKeyViolation(constraint string) error {
	return &mterrors.PgDiagnostic{
		MessageType: 'E', Severity: "ERROR", Code: mterrors.PgSSForeignKeyViolation,
		Message: "violates foreign key constraint \"" + constraint + "\"", Constraint: constraint,
	}
}

// TestStoreMapsTheConnectionsForeignKey: a migration written by another process while a
// connection is being deleted (or the other way around) is caught by the foreign key, not
// by the statements' own checks. Only that constraint is mapped; another constraint's
// foreign_key_violation, or another code on that constraint, is a plain error.
func TestStoreMapsTheConnectionsForeignKey(t *testing.T) {
	ctx := context.Background()
	fk := foreignKeyViolation(migrationConnectionsFKey)
	other := foreignKeyViolation("migration_tables_migration_id_fkey")
	wrongCode := &mterrors.PgDiagnostic{
		MessageType: 'E', Severity: "ERROR", Code: mterrors.PgSSUndefinedTable, Constraint: migrationConnectionsFKey,
	}

	t.Run("the constraint named by the catalog DDL", func(t *testing.T) {
		require.Contains(t, AddConnectionForeignKeySQL, "ADD CONSTRAINT "+migrationConnectionsFKey+" ")
	})

	deleteWith := func(err error) error {
		qs := &fakeQS{scriptedErrs: map[string]error{deleteConnectionSQL: err}}
		return NewStore(qs).DeleteConnection(ctx, 7)
	}
	insertWith := func(err error) error {
		qs := &fakeQS{tx: &fakeTx{scriptedErrs: map[string]error{insertMigrationSQL: err}}}
		return NewStore(qs).Insert(ctx, &Migration{ID: 1, Phase: PhaseCreated, ConnectionID: 7})
	}

	t.Run("delete: a reference written while deleting is in use", func(t *testing.T) {
		require.ErrorIs(t, deleteWith(fk), ErrConnectionInUse)
	})
	t.Run("insert: a connection deleted meanwhile is not found", func(t *testing.T) {
		err := insertWith(fk)
		require.ErrorIs(t, err, ErrConnectionNotFound)
		require.NotErrorIs(t, err, ErrConnectionInUse)
	})
	t.Run("another constraint is not remapped", func(t *testing.T) {
		err := deleteWith(other)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrConnectionInUse)
		require.NotErrorIs(t, err, ErrConnectionNotFound)
		err = insertWith(other)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrConnectionInUse)
		require.NotErrorIs(t, err, ErrConnectionNotFound)
	})
	t.Run("another code on that constraint is not remapped", func(t *testing.T) {
		require.NotErrorIs(t, deleteWith(wrongCode), ErrConnectionInUse)
		require.NotErrorIs(t, insertWith(wrongCode), ErrConnectionNotFound)
	})
	t.Run("an insert that fails does not commit", func(t *testing.T) {
		qs := &fakeQS{tx: &fakeTx{scriptedErrs: map[string]error{insertMigrationSQL: fk}}}
		require.Error(t, NewStore(qs).Insert(ctx, &Migration{ID: 1, Phase: PhaseCreated, ConnectionID: 7}))
		require.False(t, qs.tx.committed)
		require.True(t, qs.tx.rolledBack)
	})
}

// undefinedTable is PostgreSQL's undefined_table error for the named relation.
func undefinedTable(relation string) error {
	return &mterrors.PgDiagnostic{
		MessageType: 'E', Severity: "ERROR", Code: mterrors.PgSSUndefinedTable,
		Message: `relation "` + relation + `" does not exist`,
	}
}

// TestStoreConnectionQueriesReportAMissingConnectionsTable: every connection query
// maps undefined_table on multigres.connections to ErrNoConnectionsTable, so the
// service can say where connections live instead of leaking a raw SQL error.
func TestStoreConnectionQueriesReportAMissingConnectionsTable(t *testing.T) {
	ctx := context.Background()
	missing := undefinedTable("multigres.connections")

	cases := map[string]struct {
		failing string
		call    func(*Store) error
	}{
		"insert":      {insertConnectionSQL, func(s *Store) error { return s.InsertConnection(ctx, &Connection{ID: 1, Name: "src"}) }},
		"get by id":   {selectConnectionByIDSQL, func(s *Store) error { _, err := s.GetConnectionByRef(ctx, Ref{ID: 1}); return err }},
		"get by name": {selectConnectionByNameSQL, func(s *Store) error { _, err := s.GetConnectionByRef(ctx, Ref{Name: "src"}); return err }},
		"list":        {selectConnectionsSQL, func(s *Store) error { _, err := s.ListConnections(ctx); return err }},
		"delete":      {deleteConnectionSQL, func(s *Store) error { return s.DeleteConnection(ctx, 1) }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			qs := &fakeQS{scriptedErrs: map[string]error{tc.failing: missing}}
			err := tc.call(NewStore(qs))
			require.ErrorIs(t, err, ErrNoConnectionsTable)
		})
	}

	t.Run("another missing relation is not that condition", func(t *testing.T) {
		qs := &fakeQS{scriptedErrs: map[string]error{deleteConnectionSQL: undefinedTable("multigres.migration")}}
		err := NewStore(qs).DeleteConnection(ctx, 1)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrNoConnectionsTable)
	})

	t.Run("other errors pass through", func(t *testing.T) {
		boom := errors.New("connection reset")
		qs := &fakeQS{scriptedErrs: map[string]error{selectConnectionsSQL: boom}}
		_, err := NewStore(qs).ListConnections(ctx)
		require.ErrorIs(t, err, boom)
		require.NotErrorIs(t, err, ErrNoConnectionsTable)
	})
}
