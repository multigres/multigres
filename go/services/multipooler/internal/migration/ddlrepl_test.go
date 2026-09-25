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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeDDLConn records the SQL a ddlrepl helper issues and returns scripted
// counts, so the capture/apply setup and teardown flows can be exercised in
// process without a real Postgres.
type fakeDDLConn struct {
	execs    []string
	argCalls []ddlArgCall
	// countFn returns the value queryCount should report for a query; the query
	// is matched by substring. Defaults to 0 when nil or unmatched.
	countFn func(sql string) int64
}

type ddlArgCall struct {
	sql  string
	args []any
}

func (f *fakeDDLConn) exec(_ context.Context, sql string) error {
	f.execs = append(f.execs, sql)
	return nil
}

func (f *fakeDDLConn) execArgs(_ context.Context, sql string, args ...any) error {
	f.argCalls = append(f.argCalls, ddlArgCall{sql: sql, args: args})
	return nil
}

func (f *fakeDDLConn) queryCount(_ context.Context, sql string, _ ...any) (int64, error) {
	if f.countFn != nil {
		return f.countFn(sql), nil
	}
	return 0, nil
}

func TestCreatePublicationWithDDLLogSQL(t *testing.T) {
	sql := createPublicationWithDDLLogSQL("mt_pub_m1", []string{"public.orders", "app.items"}, "m1")
	require.Contains(t, sql, "CREATE PUBLICATION")
	require.Contains(t, sql, "mt_pub_m1") // safe identifiers are not quoted
	require.Contains(t, sql, "orders")
	require.Contains(t, sql, "items")
	// The ddl_log rides the same publication, row-filtered to this migration.
	require.Contains(t, sql, "multigres.ddl_log")
	require.Contains(t, sql, "migration_id = 'm1'")
}

func TestSplitQualified(t *testing.T) {
	s, tbl, err := splitQualified("public.orders")
	require.NoError(t, err)
	require.Equal(t, "public", s)
	require.Equal(t, "orders", tbl)

	for _, bad := range []string{"orders", "public.", ".orders", ""} {
		_, _, err := splitQualified(bad)
		require.Error(t, err, "input %q should be rejected", bad)
	}
}

func TestSetupDDLCapture(t *testing.T) {
	f := &fakeDDLConn{}
	err := setupDDLCapture(context.Background(), f, "m1", []string{"public.orders", "app.items"})
	require.NoError(t, err)
	// Five idempotent shared-object statements, then one registration per table.
	require.Len(t, f.execs, 5)
	require.Len(t, f.argCalls, 2)
	require.Contains(t, f.argCalls[0].sql, "ddl_capture_tables")
	require.Equal(t, []any{"m1", "public", "orders"}, f.argCalls[0].args)

	// A malformed table name is rejected before issuing its INSERT.
	require.Error(t, setupDDLCapture(context.Background(), &fakeDDLConn{}, "m1", []string{"no-dot"}))
}

func TestArmDDLCapture(t *testing.T) {
	// Neither trigger exists yet: both are created.
	f := &fakeDDLConn{countFn: func(string) int64 { return 0 }}
	require.NoError(t, armDDLCapture(context.Background(), f))
	require.Len(t, f.execs, 2)

	// Both already exist (a concurrent migration armed them): no-op.
	f = &fakeDDLConn{countFn: func(string) int64 { return 1 }}
	require.NoError(t, armDDLCapture(context.Background(), f))
	require.Empty(t, f.execs)
}

func TestTeardownDDLCapture(t *testing.T) {
	// Last publishing migration: after deregistering, the shared objects drop.
	f := &fakeDDLConn{countFn: func(string) int64 { return 0 }}
	require.NoError(t, teardownDDLCapture(context.Background(), f, "m1"))
	require.Len(t, f.argCalls, 2) // delete capture_tables + delete ddl_log rows
	require.NotEmpty(t, f.execs)  // shared-object drops

	// Another migration still publishes here: keep the shared objects.
	f = &fakeDDLConn{countFn: func(string) int64 { return 1 }}
	require.NoError(t, teardownDDLCapture(context.Background(), f, "m1"))
	require.Len(t, f.argCalls, 2)
	require.Empty(t, f.execs)
}

func TestSetupDDLApply(t *testing.T) {
	// Apply trigger absent: it (and enable-always) are created.
	f := &fakeDDLConn{countFn: func(string) int64 { return 0 }}
	require.NoError(t, setupDDLApply(context.Background(), f, "m1"))
	require.Len(t, f.execs, 6) // 4 shared objects + create trigger + enable-always
	require.Len(t, f.argCalls, 1)
	require.Contains(t, f.argCalls[0].sql, "ddl_apply")

	// Apply trigger already present: not recreated.
	f = &fakeDDLConn{countFn: func(string) int64 { return 1 }}
	require.NoError(t, setupDDLApply(context.Background(), f, "m1"))
	require.Len(t, f.execs, 4)
}

func TestTeardownDDLApply(t *testing.T) {
	// Another subscriber migration remains: deregister only, keep shared objects.
	f := &fakeDDLConn{countFn: func(sql string) int64 {
		if strings.Contains(sql, "ddl_apply") {
			return 1
		}
		return 0
	}}
	require.NoError(t, teardownDDLApply(context.Background(), f, "m1"))
	require.Len(t, f.argCalls, 2)
	require.Empty(t, f.execs)

	// Last subscriber and not publishing here: shared apply objects + ddl_log drop.
	f = &fakeDDLConn{countFn: func(string) int64 { return 0 }}
	require.NoError(t, teardownDDLApply(context.Background(), f, "m1"))
	require.Len(t, f.argCalls, 2)
	require.NotEmpty(t, f.execs)
}
