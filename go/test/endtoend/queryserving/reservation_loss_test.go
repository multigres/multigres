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

package queryserving

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// When the reserved connection holding a session's state (a session advisory
// lock, a temp table) is lost, the session must end with FATAL 57P01 rather than
// carry on, without that state, on a fresh backend. When the connection held
// only a transaction, the statement fails, the session stays open, and the
// client can roll back and retry.
//
// The loss is caused by terminating the reserved backend from another session.
// Every other cause (idle reap, failover, a killed primary) reaches the gateway
// the same way: an RPC error with no reservation left.
//
// Each scenario runs the statement that follows the loss once beforehand, so in
// pgx's cache_statement mode it is not described again afterwards. A statement
// Describe on a lost reservation errors without ending the session; the
// Execute that follows does.
func TestReservationLoss(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 5*time.Minute)

	for _, m := range []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"simple", pgx.QueryExecModeSimpleProtocol},
		{"cache_statement", pgx.QueryExecModeCacheStatement},
		{"exec_unnamed", pgx.QueryExecModeExec},
	} {
		t.Run(m.name, func(t *testing.T) {
			t.Run("advisory_lock", func(t *testing.T) {
				const key = 7142
				a := openReservationLossConn(t, ctx, setup, m.mode)
				b := openReservationLossConn(t, ctx, setup, pgx.QueryExecModeSimpleProtocol)

				utils.RequirePgxExec(t, ctx, a, "SELECT pg_advisory_lock($1)", key)
				require.False(t, tryAdvisoryLock(t, ctx, b, key), "B must not get the lock while A holds it")
				utils.RequirePgxExec(t, ctx, a, "SELECT 1")

				terminateReservedBackend(t, ctx, setup, a)

				requireSessionEnded(t, ctx, a, utils.PgxExec(ctx, a, "SELECT 1"), "session_advisory_lock")
				require.True(t, tryAdvisoryLock(t, ctx, b, key), "the lock must be free once A's session ended")
				utils.RequirePgxExec(t, ctx, b, "SELECT pg_advisory_unlock($1)", key)
			})

			t.Run("temp_table_shadows_permanent", func(t *testing.T) {
				table := "rl_shadow_" + m.name
				admin := openReservationLossConn(t, ctx, setup, pgx.QueryExecModeSimpleProtocol)
				utils.RequirePgxExec(t, ctx, admin, fmt.Sprintf("CREATE TABLE public.%s (id int)", table))
				t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table) })
				utils.RequirePgxExec(t, ctx, admin, fmt.Sprintf("INSERT INTO public.%s VALUES (1)", table))

				a := openReservationLossConn(t, ctx, setup, m.mode)
				utils.RequirePgxExec(t, ctx, a, fmt.Sprintf("CREATE TEMP TABLE %s (id int)", table))
				next := fmt.Sprintf("INSERT INTO %s VALUES (999)", table)
				utils.RequirePgxExec(t, ctx, a, next)

				terminateReservedBackend(t, ctx, setup, a)

				// Without the fix the INSERT falls through to the permanent table.
				requireSessionEnded(t, ctx, a, utils.PgxExec(ctx, a, next), "temp_table")
				var count int
				require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM public."+table).Scan(&count))
				require.Equal(t, 1, count, "the permanent table must be untouched")
			})

			t.Run("transaction_only", func(t *testing.T) {
				a := openReservationLossConn(t, ctx, setup, m.mode)
				utils.RequirePgxExec(t, ctx, a, "SELECT 4001")
				utils.RequirePgxExec(t, ctx, a, "BEGIN")

				terminateReservedBackend(t, ctx, setup, a)

				err := utils.PgxExec(ctx, a, "SELECT 4001")
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				require.Equal(t, "ERROR", pgErr.Severity, "a lost transaction must not end the session: %+v", pgErr)
				// The simple protocol reports the lost transaction as 40001; the portal
				// path can surface the dead backend's 08006 instead. Either way the
				// statement fails and the transaction must be rolled back.
				require.Contains(t, []string{"40001", "08006"}, pgErr.Code, "got %+v", pgErr)
				require.False(t, a.IsClosed())

				utils.RequirePgxExec(t, ctx, a, "ROLLBACK")
				utils.RequirePgxExec(t, ctx, a, "BEGIN")
				utils.RequirePgxExec(t, ctx, a, "SELECT 4002")
				utils.RequirePgxExec(t, ctx, a, "COMMIT")
			})
		})
	}
}

// TestReservationLoss_CopyFromStdin loses a temp-table reservation in the middle
// of COPY FROM STDIN. Whichever COPY step observes the loss (sending data,
// finishing, or the abort that follows a failed send), the session must end.
func TestReservationLoss_CopyFromStdin(t *testing.T) {
	skipIfNoRealPostgres(t)
	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)

	a := openReservationLossConn(t, ctx, setup, pgx.QueryExecModeSimpleProtocol)
	utils.RequirePgxExec(t, ctx, a, "CREATE TEMP TABLE rl_copy (id int, pad text)")
	pid := pgxBackendPID(t, ctx, a)

	// Terminate the backend once the COPY is under way, then keep sending
	// enough rows that the data reaches the dead connection.
	src := &terminatingCopySource{rows: 200000, terminate: func() { terminateBackend(t, ctx, setup, pid) }}
	_, err := a.CopyFrom(ctx, pgx.Identifier{"rl_copy"}, []string{"id", "pad"}, src)

	requireSessionEnded(t, ctx, a, err, "temp_table")
}

// terminatingCopySource is a pgx.CopyFromSource that calls terminate before
// producing its first row.
type terminatingCopySource struct {
	rows      int
	n         int
	terminate func()
}

func (s *terminatingCopySource) Next() bool {
	if s.n == 0 {
		s.terminate()
	}
	s.n++
	return s.n <= s.rows
}

func (s *terminatingCopySource) Values() ([]any, error) {
	return []any{s.n, "padding padding padding padding padding padding"}, nil
}

func (s *terminatingCopySource) Err() error { return nil }

func openReservationLossConn(t *testing.T, ctx context.Context, setup shardsetup.Cluster, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5"))
	require.NoError(t, err)
	cfg.DefaultQueryExecMode = mode
	conn, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func tryAdvisoryLock(t *testing.T, ctx context.Context, conn *pgx.Conn, key int) bool {
	t.Helper()
	var got bool
	require.NoError(t, conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&got))
	return got
}

// terminateReservedBackend terminates the backend conn's session is pinned to,
// from another session, and waits for its socket to close.
func terminateReservedBackend(t *testing.T, ctx context.Context, setup shardsetup.Cluster, conn *pgx.Conn) {
	t.Helper()
	terminateBackend(t, ctx, setup, pgxBackendPID(t, ctx, conn))
}

func terminateBackend(t *testing.T, ctx context.Context, setup shardsetup.Cluster, pid int) {
	t.Helper()
	killer := openReservationLossConn(t, ctx, setup, pgx.QueryExecModeSimpleProtocol)
	var terminated bool
	require.NoError(t, killer.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated))
	require.True(t, terminated, "pg_terminate_backend(%d)", pid)
	time.Sleep(time.Second)
}

// requireSessionEnded asserts that err is FATAL 57P01 and that the gateway
// closed the client's connection. The FATAL is either the gateway's, naming the
// lost state, or PostgreSQL's own from the terminated backend, which the gateway
// passes through when the pooler relays it.
func requireSessionEnded(t *testing.T, ctx context.Context, conn *pgx.Conn, err error, lostState string) {
	t.Helper()
	pgErr := utils.RequirePgError(t, err, "57P01")
	require.Equal(t, "FATAL", pgErr.Severity, "got %+v", pgErr)
	if pgErr.Message != "terminating connection due to administrator command" {
		require.Contains(t, pgErr.Detail, lostState)
	}

	require.Error(t, utils.PgxExec(ctx, conn, "SELECT 1"), "the gateway must close the connection after the FATAL")
	require.True(t, conn.IsClosed())
}
