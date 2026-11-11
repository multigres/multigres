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

package clustersetup

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/common/mterrors"
)

// QueriesRefusedDuringShutdown runs clients that query dsn in a loop, each on
// its own connection, calls terminate (which shuts the server down) once they
// all run queries, and returns the queries the server refused while it shut
// down. A clean shutdown refuses none: a query either succeeds or loses its
// connection, or is ended by an admin_shutdown FATAL, as PostgreSQL ends its
// sessions. A query refused with an error on a connection that stays open,
// such as "no writable primary", tells the client nothing about the server
// going away, so it keeps trying.
func QueriesRefusedDuringShutdown(t *testing.T, dsn string, clients int, terminate func()) []error {
	t.Helper()
	var (
		mu        sync.Mutex
		succeeded int
		refused   []error
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range clients {
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		require.NoError(t, db.PingContext(t.Context()))
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				var one int
				err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
				cancel()
				var pqErr *pq.Error
				mu.Lock()
				switch {
				case err == nil:
					succeeded++
				case errors.As(err, &pqErr) && string(pqErr.Code) != mterrors.PgSSAdminShutdown:
					refused = append(refused, err)
				}
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		})
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return succeeded >= 4*clients
	}, 10*time.Second, 10*time.Millisecond, "the clients must be running queries before shutdown")
	terminate()
	close(stop)
	wg.Wait()
	return refused
}
