// Copyright 2026 Supabase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
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
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/sqltypes"
	"github.com/multigres/multigres/go/services/multipooler/internal/executor"
)

// copySQLTx scripts catalog results while retaining the ordering of statements.
// Ordering matters: the snapshot must follow the subscription lock, and table
// identity and membership must be rechecked under relation locks before TRUNCATE.
type copySQLTx struct {
	fakeTx
	results            map[string]*sqltypes.Result
	tableResults       map[string]*sqltypes.Result
	errs               map[string]error
	rollbackContextErr error
}

func (tx *copySQLTx) Query(ctx context.Context, query string) (*sqltypes.Result, error) {
	return tx.QueryArgs(ctx, query)
}

func (tx *copySQLTx) QueryArgs(_ context.Context, query string, args ...any) (*sqltypes.Result, error) {
	tx.calls = append(tx.calls, qsArgCall{sql: query, args: args})
	if err := tx.errs[query]; err != nil {
		return nil, err
	}
	if query == copyTruncateTableSQL && len(args) == 1 {
		return tx.tableResults[args[0].(string)], nil
	}
	if result := tx.results[query]; result != nil {
		return result, nil
	}
	return emptyResult(), nil
}

func (tx *copySQLTx) Rollback(ctx context.Context) error {
	tx.rollbackContextErr = ctx.Err()
	return tx.fakeTx.Rollback(ctx)
}

type copySQLQS struct {
	fakeQS
	copyTx *copySQLTx
	begun  bool
}

func (qs *copySQLQS) BeginAdmin(context.Context) (executor.InternalTx, error) {
	qs.begun = true
	return qs.copyTx, nil
}

func copySQLRow(values ...string) *sqltypes.Result {
	row := &sqltypes.Row{}
	for _, value := range values {
		row.Values = append(row.Values, sqltypes.Value(value))
	}
	return &sqltypes.Result{Rows: []*sqltypes.Row{row}}
}

func copySQLSnapshotRow() *sqltypes.Result {
	return copySQLRow("7", "9", `["migration_pub"]`, "main_slot", "t",
		`[{"name":"public.healthy","oid":21,"state":"r","ordinary":true}]`)
}

func copySQLRecovery() *CopyRecovery {
	return &CopyRecovery{
		Stage: CopyRecoveryDetach, SubscriptionOID: 7, SystemID: "9",
		Tables: []CopyRecoveryTable{{Name: "public.stuck", OID: 20}},
	}
}

func copySQLStatements(tx *copySQLTx) []string {
	statements := make([]string, 0, len(tx.calls))
	for _, call := range tx.calls {
		statements = append(statements, call.sql)
	}
	return statements
}

func TestCopySnapshotIdentityAndRelations(t *testing.T) {
	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{copySnapshotSQL: copySQLSnapshotRow()}}
	snapshot, err := newTarget(qs).CopySnapshot(t.Context(), "migration_sub")
	require.NoError(t, err)
	require.Equal(t, &CopySnapshot{
		SubscriptionOID: 7, SystemID: "9", Publications: []string{"migration_pub"},
		SlotName: "main_slot", Enabled: true,
		Relations: []CopyRelation{{Name: "public.healthy", OID: 21, State: "r", Ordinary: true}},
	}, snapshot)
	require.Equal(t, []any{"migration_sub"}, qs.adminArgs[0].args)

	qs.scriptedResults[copySnapshotSQL] = emptyResult()
	_, err = newTarget(qs).CopySnapshot(t.Context(), "migration_sub")
	require.ErrorContains(t, err, "absent from the writable target database")
}

func TestWithCopyLockReadsAfterLockAndAlwaysRollsBack(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	tx := &copySQLTx{results: map[string]*sqltypes.Result{copySnapshotSQL: copySQLSnapshotRow()}}
	qs := &copySQLQS{copyTx: tx}
	callbackErr := errors.New("diagnosis failed")
	err := newTarget(qs).WithCopyLock(ctx, `migration"sub`, func(s *CopySnapshot) error {
		require.Equal(t, int64(7), s.SubscriptionOID)
		require.Equal(t, []string{
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", "SET LOCAL lock_timeout = '1s'",
			`ALTER SUBSCRIPTION "migration""sub" SET (synchronous_commit = on)`, copySnapshotSQL,
		}, copySQLStatements(tx))
		cancel()
		return callbackErr
	})
	require.ErrorIs(t, err, callbackErr)
	require.False(t, tx.committed)
	require.True(t, tx.rolledBack)
	require.NoError(t, tx.rollbackContextErr, "cleanup must release the reserved connection even if its caller canceled")
}

func TestWithCopyLockBusyDefersWithoutSnapshot(t *testing.T) {
	lock := `ALTER SUBSCRIPTION migration_sub SET (synchronous_commit = on)`
	tx := &copySQLTx{errs: map[string]error{lock: &pgconn.PgError{Code: "55P03"}}}
	qs := &copySQLQS{copyTx: tx}
	err := newTarget(qs).WithCopyLock(t.Context(), "migration_sub", func(*CopySnapshot) error {
		t.Fatal("a live handoff must defer diagnosis")
		return nil
	})
	require.ErrorIs(t, err, ErrCopyLockBusy)
	require.NotContains(t, copySQLStatements(tx), copySnapshotSQL)
	require.True(t, tx.rolledBack)
	require.False(t, tx.committed)
}

func TestSetSubscriptionPublicationsChecksIdentity(t *testing.T) {
	qs := &fakeQS{scriptedResults: map[string]*sqltypes.Result{copySnapshotSQL: copySQLSnapshotRow()}}
	target := newTarget(qs)
	recovery := copySQLRecovery()
	recovery.SystemID = "different"
	err := target.SetSubscriptionPublications(t.Context(), "migration_sub", "recovery_pub", recovery)
	require.ErrorContains(t, err, "identity changed")
	require.Empty(t, qs.adminQueries, "identity drift must stop before changing membership")

	recovery.SystemID = "9"
	require.NoError(t, target.SetSubscriptionPublications(t.Context(), "migration_sub", `recovery"pub`, recovery))
	require.Equal(t, []string{`ALTER SUBSCRIPTION migration_sub SET PUBLICATION "recovery""pub" WITH (refresh = true, copy_data = true)`}, qs.adminQueries)
}

func TestTruncateCopyTablesLocksChecksAndDurablyCommits(t *testing.T) {
	tx := &copySQLTx{
		results:      map[string]*sqltypes.Result{copySnapshotSQL: copySQLSnapshotRow()},
		tableResults: map[string]*sqltypes.Result{"public.stuck": copySQLRow("20", "t", "f")},
	}
	qs := &copySQLQS{copyTx: tx}
	require.NoError(t, newTarget(qs).TruncateCopyTables(t.Context(), "migration_sub", copySQLRecovery()))
	statements := copySQLStatements(tx)
	lock := `LOCK TABLE ONLY public.stuck IN ACCESS EXCLUSIVE MODE`
	truncate := `TRUNCATE ONLY public.stuck CONTINUE IDENTITY RESTRICT`
	require.Less(t, slices.Index(statements, lock), slices.Index(statements, copySnapshotSQL))
	require.Less(t, slices.Index(statements, copySnapshotSQL), slices.Index(statements, copyTruncateTableSQL))
	require.Less(t, slices.Index(statements, copyTruncateTableSQL), slices.Index(statements, truncate))
	require.Contains(t, statements, "SET LOCAL synchronous_commit = on")
	require.True(t, tx.committed, "TRUNCATE must commit before the coordinator records the attach stage")
}

func TestTruncateCopyTablesRefusesChangedIdentityOrMembership(t *testing.T) {
	for _, tc := range []struct {
		name     string
		row      *sqltypes.Result
		recovery func(*CopyRecovery)
	}{
		{name: "recreated relation", row: copySQLRow("999", "t", "f")},
		{name: "partition or inherited table", row: copySQLRow("20", "f", "f")},
		{name: "another subscription", row: copySQLRow("20", "t", "t")},
		{name: "subscription recreated", recovery: func(r *CopyRecovery) { r.SubscriptionOID++ }},
		{name: "different target", recovery: func(r *CopyRecovery) { r.SystemID = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &copySQLTx{
				results:      map[string]*sqltypes.Result{copySnapshotSQL: copySQLSnapshotRow()},
				tableResults: map[string]*sqltypes.Result{"public.stuck": tc.row},
			}
			qs := &copySQLQS{copyTx: tx}
			recovery := copySQLRecovery()
			if tc.recovery != nil {
				tc.recovery(recovery)
			}
			require.Error(t, newTarget(qs).TruncateCopyTables(t.Context(), "migration_sub", recovery))
			for _, sql := range copySQLStatements(tx) {
				require.False(t, strings.HasPrefix(sql, "TRUNCATE "), "must refuse before destructive statement")
			}
			require.False(t, tx.committed)
			require.True(t, tx.rolledBack)
		})
	}
}

func TestTruncateCopyTablesRequiresDetachStage(t *testing.T) {
	qs := &copySQLQS{}
	recovery := copySQLRecovery()
	recovery.Stage = CopyRecoveryAttach
	require.ErrorContains(t, newTarget(qs).TruncateCopyTables(t.Context(), "migration_sub", recovery), "detach-stage")
	require.False(t, qs.begun)
}

func TestValidatePlainCopyPublication(t *testing.T) {
	tables := []copyPublicationTable{{Name: "public.b", Ordinary: true}, {Name: "public.a", Ordinary: true}}
	require.NoError(t, validatePlainCopyPublication(false, false, true, false, true, tables, []string{"public.a", "public.b"}))
	require.NoError(t, validatePlainCopyPublication(false, false, true, false, true, nil, nil))
	for _, tc := range []struct {
		name                             string
		all, root, changes, schemas, gen bool
	}{
		{name: "all tables", all: true, changes: true, gen: true},
		{name: "partition roots", root: true, changes: true, gen: true},
		{name: "omitted change kinds", gen: true},
		{name: "schemas", schemas: true, changes: true, gen: true},
		{name: "generated columns", changes: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validatePlainCopyPublication(tc.all, tc.root, tc.changes, tc.schemas, tc.gen, tables, []string{"public.a", "public.b"}))
		})
	}
	for _, table := range []copyPublicationTable{
		{Name: "public.a"},
		{Name: "public.a", Ordinary: true, Filtered: true},
		{Name: "public.a", Ordinary: true, Columns: true},
	} {
		require.Error(t, validatePlainCopyPublication(false, false, true, false, true, []copyPublicationTable{table}, []string{"public.a"}))
	}
	require.ErrorContains(t, validatePlainCopyPublication(false, false, true, false, true, tables, []string{"public.a"}), "table set changed")
}

func TestCopyPublicationSQLSupportsPostgreSQL14(t *testing.T) {
	query14, err := copyPublicationSQLForVersion(140023)
	require.NoError(t, err)
	require.NotContains(t, query14, "pg_publication_namespace")
	require.NotContains(t, query14, "pr.prqual")
	require.NotContains(t, query14, "pr.prattrs")
	require.Contains(t, query14, "(to_jsonb(pr)->>'prqual') IS NOT NULL")
	query17, err := copyPublicationSQLForVersion(170011)
	require.NoError(t, err)
	require.Equal(t, copyPublicationSQL, query17)
	_, err = copyPublicationSQLForVersion(130025)
	require.ErrorContains(t, err, "PostgreSQL 14 or newer")
}
