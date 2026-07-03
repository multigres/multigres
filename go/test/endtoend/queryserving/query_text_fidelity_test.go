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
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestQueryTextFidelity_PgStatActivity verifies that the statement text the
// backend sees — and therefore pg_stat_activity.query, server logs, and error
// cursor positions — is the client's exact bytes, including casing, spacing,
// literals, and the trailing semicolon. Before the fix, the gateway sent a
// deparsed/normalized form for cacheable DML (e.g. the trailing semicolon was
// dropped), which is the divergence the partition-drop-index-locking isolation
// test caught in its pg_stat_activity sample.
//
// Each subtest runs against both direct PostgreSQL and multigateway to ensure
// the proxy behavior matches native PostgreSQL exactly.
func TestQueryTextFidelity_PgStatActivity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping query text fidelity test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping query text fidelity tests")
	}

	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	// Odd casing, a string literal (exercises the normalize/plan-cache path),
	// and a trailing semicolon — none of which survive deparsing.
	markerQueries := []string{
		"SeLeCt 'qtext_marker_one' AS m ;",
		// Same normalized shape as the first: exercises the plan-cache HIT
		// path, where the plan carries the first query's template.
		"SeLeCt 'qtext_marker_two' AS m ;",
	}

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connStr := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable")

			// Session conn: runs the marker statements over the simple query
			// protocol inside a transaction, pinning one backend so its
			// pg_stat_activity row can't be overwritten by pool activity.
			sess, err := pgconn.Connect(ctx, connStr)
			require.NoError(t, err)
			defer sess.Close(context.Background())

			// Observer conn: samples pg_stat_activity from the outside.
			obs, err := pgconn.Connect(ctx, connStr)
			require.NoError(t, err)
			defer obs.Close(context.Background())

			_, err = sess.Exec(ctx, "BEGIN").ReadAll()
			require.NoError(t, err)
			defer func() { _, _ = sess.Exec(context.Background(), "ROLLBACK").ReadAll() }()

			for _, marker := range markerQueries {
				_, err = sess.Exec(ctx, marker).ReadAll()
				require.NoError(t, err)

				// Build the LIKE pattern by concatenation so the observer's own
				// statement text never matches it.
				results, err := obs.Exec(ctx,
					"SELECT query FROM pg_stat_activity WHERE query LIKE 'SeLeCt ' || '''qtext_marker%'").ReadAll()
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Len(t, results[0].Rows, 1,
					"exactly one backend should show the marker statement")
				assert.Equal(t, marker, string(results[0].Rows[0][0]),
					"pg_stat_activity.query must be the client's exact statement text")
			}
		})
	}
}
