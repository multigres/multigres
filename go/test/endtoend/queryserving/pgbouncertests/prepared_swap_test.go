// Copyright 2026 Supabase, Inc.
// Portions derived from PgBouncer (ISC License),
// Copyright (c) 2007-2009 Marko Kreen, Skype Technologies OÜ.
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

package pgbouncertests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestPreparedStatementAcrossPooledBackends is the headline pooled-backend
// test, ported from PgBouncer's test_prepared.py::test_prepared_statement.
//
// PgBouncer's scenario: a statement prepared once must keep working as the
// client's session is served by different backend servers. Multigres has no
// ReasonPrepared reservation, so a named Parse does NOT pin the backend; the
// multipooler executor instead re-prepares the statement (ensurePrepared) on
// whichever pooled connection each Execute lands on
// (go/services/multipooler/executor/executor.go). The bug class this guards
// against is a swap where the statement is not re-prepared, surfacing as
// "prepared statement does not exist".
//
// This is a black-box test: it only speaks the PostgreSQL wire protocol to the
// multigateway and only observes what a normal client can — including the
// backend identity, which it reads from its own query results via
// pg_backend_pid(), not from any side channel into the underlying PostgreSQL.
// It prepares once, then executes the statement many times under concurrent
// load that keeps the multipooler's pool rotating. The invariant is that every
// Execute returns correct results; observing the statement run on >=2 distinct
// backends proves the re-prepare path was actually exercised (the test is not
// vacuous).
func TestPreparedStatementAcrossPooledBackends(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 90*time.Second)

	gatewayDSN := shardsetup.GetTestUserDSN("localhost", setup.MultigatewayPgPort, "sslmode=disable", "connect_timeout=5")

	conn := connectGateway(t, ctx, gatewayDSN)
	defer conn.Close(ctx)

	const psName = "ps_swap"
	_, err := conn.Prepare(ctx, psName, "SELECT pg_backend_pid(), $1::int")
	require.NoError(t, err)

	stopChurn := startPoolChurn(t, ctx, gatewayDSN, churnClients)
	defer stopChurn()

	runAcrossBackends(t, ctx, func(ctx context.Context, i int) int {
		var pid, v int
		require.NoError(t, conn.QueryRow(ctx, psName, i).Scan(&pid, &v),
			"execute %d must succeed (statement must be re-prepared on its backend)", i)
		require.Equal(t, i, v, "execute %d returned the wrong bound parameter", i)
		return pid
	})
}

// --- shared helpers for the load-and-observe black-box swap tests ---

const (
	// churnClients is how many background clients keep the multipooler pool
	// rotating so a session's repeated Executes land on different backends.
	churnClients = 8

	// minExecutes is the minimum number of times an across-backends test runs
	// its statement, so the re-prepare path is exercised repeatedly even once
	// the >=2-backends bar is met.
	minExecutes = 40
)

// runAcrossBackends repeatedly calls exec (which runs the statement once with a
// monotonic counter and returns the backend pid it ran on) until the statement
// has been observed on at least two distinct backends AND it has run at least
// minExecutes times — or a deadline passes. exec is expected to assert the
// per-execution result itself, so any failure to re-prepare on a swapped backend
// fails the test immediately. Finally it asserts >=2 distinct backends were
// seen, which keeps the test from passing vacuously when no swap occurred.
func runAcrossBackends(t *testing.T, ctx context.Context, exec func(ctx context.Context, i int) int) {
	t.Helper()
	seen := map[int]bool{}
	deadline := time.Now().Add(45 * time.Second)
	i := 0
	for i < minExecutes || len(seen) < 2 {
		if i >= minExecutes && time.Now().After(deadline) {
			break
		}
		seen[exec(ctx, i)] = true
		i++
	}
	require.GreaterOrEqualf(t, len(seen), 2,
		"prepared statement ran on only %d distinct backend(s) across %d executes under churn; "+
			"expected the pool to serve it from >=2 backends so the re-prepare path is exercised",
		len(seen), i)
	t.Logf("statement ran across %d distinct backends in %d executes", len(seen), i)
}

// startPoolChurn launches n background clients that each run a cheap query in a
// tight loop through the gateway. Their constant borrow-and-return cycling keeps
// returning connections to the top of the multipooler's (LIFO) pool faster than
// a single foreground session loops, so the foreground session's consecutive
// Executes are handed different backends rather than its own just-returned one.
// It returns a stop function that cancels the churn and closes the connections;
// defer it. Pure wire-protocol load — no knowledge of, or dependence on, the
// pool's internals.
func startPoolChurn(t *testing.T, ctx context.Context, gatewayDSN string, n int) func() {
	t.Helper()
	churnCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for range n {
		conn := connectGateway(t, ctx, gatewayDSN)
		wg.Go(func() {
			defer func() {
				closeCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
				defer c()
				_ = conn.Close(closeCtx)
			}()
			for churnCtx.Err() == nil {
				// Cheap round-trip: borrow a pooled backend, return it. The
				// error on cancellation is expected and ends the loop.
				_, _ = conn.Exec(churnCtx, "SELECT 1")
			}
		})
	}
	return func() {
		cancel()
		wg.Wait()
	}
}

// connectGateway opens a pgx connection and fails the test on error.
func connectGateway(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	return conn
}
