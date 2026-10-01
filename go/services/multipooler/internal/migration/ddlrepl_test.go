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
	sql := createPublicationWithDDLLogSQL("mt_pub_m1", []string{"public.orders", "app.items"})
	require.Contains(t, sql, "CREATE PUBLICATION")
	require.Contains(t, sql, "mt_pub_m1") // safe identifiers are not quoted
	require.Contains(t, sql, "orders")
	require.Contains(t, sql, "items")
	// ddl_log rides the same publication as the data tables.
	require.Contains(t, sql, "multigres.ddl_log")
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

func TestIsAlreadyExists(t *testing.T) {
	require.False(t, isAlreadyExists(nil))
	require.True(t, isAlreadyExists(errors.New(`ERROR: event trigger "mg_capture_ddl" already exists (SQLSTATE 42710)`)))
	require.True(t, isAlreadyExists(errors.New("SQLSTATE 42710")))
	require.False(t, isAlreadyExists(errors.New("some other error")))
}

func TestSetupDDLCapture(t *testing.T) {
	f := &fakeDDLConn{}
	err := setupDDLCapture(context.Background(), f, []string{"public.orders", "app.items"})
	require.NoError(t, err)
	// Six idempotent shared-object statements, then one registration per table.
	require.Len(t, f.execs, 6)
	require.Len(t, f.argCalls, 2)
	require.Contains(t, f.argCalls[0].sql, "ddl_capture_tables")
	require.Equal(t, []any{"public", "orders"}, f.argCalls[0].args)

	// A malformed table name is rejected before issuing its INSERT.
	require.Error(t, setupDDLCapture(context.Background(), &fakeDDLConn{}, []string{"no-dot"}))
}

func TestArmDDLCapture(t *testing.T) {
	// None of the three triggers exist yet: all are created.
	f := &fakeDDLConn{countFn: func(string) int64 { return 0 }}
	require.NoError(t, armDDLCapture(context.Background(), f))
	require.Len(t, f.execs, 3)

	// All already exist (a resumed setup): no-op.
	f = &fakeDDLConn{countFn: func(string) int64 { return 1 }}
	require.NoError(t, armDDLCapture(context.Background(), f))
	require.Empty(t, f.execs)
}

func TestArmDDLCaptureToleratesConcurrentCreateRace(t *testing.T) {
	// Both existence checks report absent (a stale read racing a retried setup
	// on the same connection), and the CREATE EVENT TRIGGER a previous attempt
	// already won loses with "already exists" — armDDLCapture must not fail
	// setup over this.
	f := scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "CREATE EVENT TRIGGER",
		execErr:    errors.New(`ERROR: event trigger "mg_capture_ddl" already exists (SQLSTATE 42710)`),
	}
	require.NoError(t, armDDLCapture(context.Background(), f))
}

func TestTeardownDDLCapture(t *testing.T) {
	f := &fakeDDLConn{}
	require.NoError(t, teardownDDLCapture(context.Background(), f))
	require.Len(t, f.execs, len(teardownDDLCaptureStmts))
	require.Contains(t, f.execs[0], "mg_capture_ddl")
	joined := f.execs[len(f.execs)-2] + f.execs[len(f.execs)-1]
	require.Contains(t, joined, "ddl_capture_tables")
	require.Contains(t, joined, "ddl_log")
}

func TestSetupDDLApply(t *testing.T) {
	// Apply trigger absent: it (and enable-always) are created.
	f := &fakeDDLConn{countFn: func(string) int64 { return 0 }}
	require.NoError(t, setupDDLApply(context.Background(), f))
	require.Len(t, f.execs, 5) // 3 shared objects + create trigger + enable-always

	// Apply trigger already present: not recreated.
	f = &fakeDDLConn{countFn: func(string) int64 { return 1 }}
	require.NoError(t, setupDDLApply(context.Background(), f))
	require.Len(t, f.execs, 3)
}

func TestSetupDDLApplyToleratesConcurrentCreateRace(t *testing.T) {
	f := scriptedDDLConn{
		counts:     func(string) int64 { return 0 },
		failExecOn: "CREATE TRIGGER",
		execErr:    errors.New(`ERROR: trigger "mg_apply_ddl" for relation "ddl_log" already exists (SQLSTATE 42710)`),
	}
	require.NoError(t, setupDDLApply(context.Background(), f))
}

func TestTeardownDDLApply(t *testing.T) {
	f := &fakeDDLConn{}
	require.NoError(t, teardownDDLApply(context.Background(), f))
	require.Len(t, f.execs, len(teardownDDLApplyStmts))
	require.Contains(t, f.execs[0], "ddl_log")
	require.Contains(t, f.execs[1], "apply_ddl")
}
