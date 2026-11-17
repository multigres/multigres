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

package queryserving

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestReservedDescribeDeadConnReturnsCleanError is the regression for MTD06
// "describe failed … broken pipe". When the backend behind a reserved connection is
// gone, a Describe must return a clean, retryable "reserved connection terminated"
// error (SQLSTATE 40001) — not an opaque MTD06 broken-pipe — so the client reconnects
// (and recreates any temp replication slot) rather than seeing an internal failure.
func TestReservedDescribeDeadConnReturnsCleanError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	// This is a gateway/pooler-specific path (reserved connections); run against the
	// multigateway target.
	var port int
	for _, target := range setup.ComparisonTargets(t) {
		if target.Name == "multigateway" {
			port = target.Port
		}
	}
	require.NotZero(t, port, "multigateway target port")

	conn := connectLowLevelToPort(t, ctx, port)
	defer conn.Close()
	killer := connectLowLevelToPort(t, ctx, port)
	defer killer.Close()

	// Reserve the connection with a transaction and prepare a statement on its backend.
	_, err := conn.Query(ctx, "BEGIN")
	require.NoError(t, err)
	require.NoError(t, conn.Parse(ctx, "mtd06_d1", "SELECT $1::int", []uint32{23}))

	// Identify the reserved backend pid, then terminate it from another session.
	res, err := conn.Query(ctx, "SELECT pg_backend_pid()")
	require.NoError(t, err)
	require.NotEmpty(t, res)
	pid := string(res[0].Rows[0].Values[0])

	_, err = killer.Query(ctx, "SELECT pg_terminate_backend("+pid+")")
	require.NoError(t, err)

	// Let the socket fully close; the reserved conn stays held (no background health check).
	time.Sleep(1 * time.Second)

	// Describe on the now-dead reserved connection.
	_, err = conn.DescribePrepared(ctx, "mtd06_d1")
	require.Error(t, err, "describe on a dead reserved connection must error")

	msg := err.Error()
	assert.NotContains(t, msg, "MTD06", "must not be the opaque describe-failed code")
	assert.NotContains(t, msg, "broken pipe", "must not surface the raw write error")
	assert.True(t,
		strings.Contains(msg, "reserved connection terminated") || strings.Contains(msg, "40001"),
		"expected a clean retryable reserved-connection-terminated error, got: %s", msg)
}

// TestReservedConnLossDoesNotPoisonNewStatements is the regression for a lost
// reservation that kept failing every *new* extended-protocol statement with 40001.
// After the reserved backend dies, the Describe for a never-seen statement text used
// to leave the gateway pointing at the dead reservation, so each following Parse/Describe
// failed the same way (a retry-on-40001 wrapper looped forever). A reservation that held
// only a transaction must give the client exactly one 40001, after which it runs
// normally on a fresh backend; one that held session state ends the session.
//
// The three pgx modes below are the ones that Parse/Describe each new statement text.
func TestReservedConnLossDoesNotPoisonNewStatements(t *testing.T) {
	skipIfNoRealPostgres(t)

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)
	dsn := shardsetup.GetTestUserDSN("localhost", setup.ClientPort(), "sslmode=disable", "connect_timeout=5")

	modes := []struct {
		name string
		mode pgx.QueryExecMode
	}{
		{"cache_statement", pgx.QueryExecModeCacheStatement},
		{"cache_describe", pgx.QueryExecModeCacheDescribe},
		{"describe_exec", pgx.QueryExecModeDescribeExec},
	}
	reservations := []struct {
		name string
		// reserve pins a reserved backend for conn's session.
		reserve func(t *testing.T, conn *pgx.Conn)
		// afterLoss runs once the client has seen the 40001, before the "next
		// statement works" assertions. An open transaction is failed by the loss and
		// stays so until ROLLBACK, exactly as on PostgreSQL.
		afterLoss func(t *testing.T, conn *pgx.Conn)
		// endsSession: the reservation held session state, so the loss ends the
		// session with FATAL 57P01 instead of a 40001.
		endsSession bool
	}{
		{
			name: "advisory_lock",
			reserve: func(t *testing.T, conn *pgx.Conn) {
				_, err := conn.Exec(ctx, "SELECT pg_advisory_lock(42)")
				require.NoError(t, err)
			},
			afterLoss:   func(*testing.T, *pgx.Conn) {},
			endsSession: true,
		},
		{
			name: "transaction",
			reserve: func(t *testing.T, conn *pgx.Conn) {
				_, err := conn.Exec(ctx, "BEGIN")
				require.NoError(t, err)
			},
			afterLoss: func(t *testing.T, conn *pgx.Conn) {
				_, err := conn.Exec(ctx, "ROLLBACK", pgx.QueryExecModeSimpleProtocol)
				require.NoError(t, err)
			},
		},
	}

	for _, m := range modes {
		for _, r := range reservations {
			t.Run(m.name+"/"+r.name, func(t *testing.T) {
				cfg, err := pgx.ParseConfig(dsn)
				require.NoError(t, err)
				cfg.DefaultQueryExecMode = m.mode
				conn, err := pgx.ConnectConfig(ctx, cfg)
				require.NoError(t, err)
				defer conn.Close(context.Background())

				admin, err := pgx.Connect(ctx, dsn)
				require.NoError(t, err)
				defer admin.Close(context.Background())

				r.reserve(t, conn)

				var pid int32
				require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))
				_, err = admin.Exec(ctx, fmt.Sprintf("SELECT pg_terminate_backend(%d)", pid), pgx.QueryExecModeSimpleProtocol)
				require.NoError(t, err)

				// Let the socket fully close; the reservation stays held (no background
				// health check), so the next statement is what discovers the loss.
				time.Sleep(1 * time.Second)

				// Every statement text is new to this session, so each one goes through
				// Parse/Describe before it can execute.
				queryN := func(n int) (int, error) {
					var got int
					err := conn.QueryRow(ctx, fmt.Sprintf("SELECT %d", n)).Scan(&got)
					return got, err
				}

				_, err = queryN(1001)
				require.Error(t, err, "first statement after the loss must report it")
				var pgErr *pgconn.PgError
				require.True(t, errors.As(err, &pgErr), "expected a PostgreSQL error, got %T: %v", err, err)
				if r.endsSession {
					require.Equal(t, "57P01", pgErr.Code, "expected the session to end, got: %v", err)
					_, err = queryN(1002)
					require.Error(t, err, "the session must be over")
					return
				}
				require.Equal(t, "40001", pgErr.Code, "expected exactly one retryable 40001, got: %v", err)

				r.afterLoss(t, conn)

				for n := 1002; n <= 1005; n++ {
					got, err := queryN(n)
					require.NoError(t, err, "statement %d after the single 40001 must run normally", n)
					require.Equal(t, n, got)
				}

				// The session is on a fresh backend, not the one that was terminated.
				var newPid int32
				require.NoError(t, conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&newPid))
				require.NotEqual(t, pid, newPid)
			})
		}
	}
}
