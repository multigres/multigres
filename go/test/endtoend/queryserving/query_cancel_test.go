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
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestMultiGateway_QueryCancel tests that a PostgreSQL client can cancel a
// running query through the multigateway using the CancelRequest protocol.
// This validates end-to-end cancel routing: client → multigateway → backend.
func TestMultiGateway_QueryCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping query cancel test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping query cancel tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)

	ctx := utils.WithTimeout(t, 150*time.Second)

	t.Run("pgx context cancel", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		_, err := conn.Exec(cancelCtx, "SELECT pg_sleep(10)")
		elapsed := time.Since(start)

		require.Error(t, err, "query should be cancelled")
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			assert.Equal(t, "57014", pgErr.Code, "should be query_canceled (57014)")
		}
		assert.Less(t, elapsed, 5*time.Second, "cancel should return well before the 10s sleep completes")
	})

	t.Run("database/sql ExecContext cancel", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		err = db.PingContext(ctx)
		require.NoError(t, err, "failed to ping database")

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		_, err = db.ExecContext(cancelCtx, "SELECT pg_sleep(10)")
		elapsed := time.Since(start)

		require.Error(t, err, "ExecContext should be cancelled")
		assert.Less(t, elapsed, 5*time.Second, "cancel should return well before the 10s sleep completes")
	})

	t.Run("database/sql QueryContext cancel", func(t *testing.T) {
		connStr := fmt.Sprintf(
			"host=localhost port=%d user=postgres password=%s dbname=postgres sslmode=disable",
			setup.MultigatewayPgPort, shardsetup.TestPostgresPassword)
		db, err := sql.Open("postgres", connStr)
		require.NoError(t, err)
		defer db.Close()

		err = db.PingContext(ctx)
		require.NoError(t, err, "failed to ping database")

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		rows, err := db.QueryContext(cancelCtx, "SELECT pg_sleep(10)")
		elapsed := time.Since(start)
		if rows != nil {
			defer rows.Close()
		}

		require.Error(t, err, "QueryContext should be cancelled")
		assert.Less(t, elapsed, 5*time.Second, "cancel should return well before the 10s sleep completes")
	})

	t.Run("new connection works after cancel", func(t *testing.T) {
		conn := connectPgx(t, ctx, setup)
		defer conn.Close(ctx)

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		_, err := conn.Exec(cancelCtx, "SELECT pg_sleep(10)")
		require.Error(t, err, "query should be cancelled")

		// pgx closes the connection after context cancellation, which is
		// expected client behavior. Verify that the multigateway's state
		// is not corrupted: a new connection should work fine.
		conn2 := connectPgx(t, ctx, setup)
		defer conn2.Close(ctx)

		var result int
		err = conn2.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "new connection should work after cancel")
		assert.Equal(t, 1, result)
	})

	t.Run("cancel does not affect other connections", func(t *testing.T) {
		conn1 := connectPgx(t, ctx, setup)
		defer conn1.Close(ctx)
		conn2 := connectPgx(t, ctx, setup)
		defer conn2.Close(ctx)

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Start a long query on conn1 in a goroutine.
		errCh := make(chan error, 1)
		go func() {
			_, err := conn1.Exec(cancelCtx, "SELECT pg_sleep(10)")
			errCh <- err
		}()

		// Wait for the query to start executing on the backend.
		time.Sleep(100 * time.Millisecond)

		// conn2 should work fine while conn1 has a running query.
		var result int
		err := conn2.QueryRow(ctx, "SELECT 1").Scan(&result)
		require.NoError(t, err, "conn2 should not be affected by conn1's pending query")
		assert.Equal(t, 1, result)

		// Cancel conn1's query.
		cancel()

		// conn1's query should return with an error.
		select {
		case err = <-errCh:
			require.Error(t, err, "conn1's query should be cancelled")
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for conn1's query to be cancelled")
		}

		// conn2 should still be usable after conn1's cancel.
		err = conn2.QueryRow(ctx, "SELECT 2").Scan(&result)
		require.NoError(t, err, "conn2 should still work after conn1's cancel")
		assert.Equal(t, 2, result)
	})
}
