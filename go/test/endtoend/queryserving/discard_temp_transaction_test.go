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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestDiscardTempInATransaction: DISCARD TEMP is transactional, so a rollback of the
// transaction (or of the savepoint) it ran in keeps the session's temp tables, and a
// commit drops them.
func TestDiscardTempInATransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 2*time.Minute)

	cases := []struct {
		name       string
		statements []string
		wantTable  bool
	}{
		{"rollback", []string{"BEGIN", "SELECT 1", "DISCARD TEMP", "ROLLBACK"}, true},
		{"rollback to savepoint", []string{"BEGIN", "SAVEPOINT s", "DISCARD TEMP", "ROLLBACK TO SAVEPOINT s", "COMMIT"}, true},
		{"commit", []string{"BEGIN", "SELECT 1", "DISCARD TEMP", "COMMIT"}, false},
		// The temp tables only need to exist before the scope that is rolled back.
		{"created in the transaction, rollback to savepoint", []string{
			"DROP TABLE discard_txn", "BEGIN", "CREATE TEMP TABLE discard_txn(x int)", "INSERT INTO discard_txn VALUES (1)",
			"SAVEPOINT s", "DISCARD TEMP", "ROLLBACK TO SAVEPOINT s", "COMMIT",
		}, true},
	}
	for _, target := range setup.ComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			cfg, err := pgx.ParseConfig(shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5"))
			require.NoError(t, err)
			cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					conn, err := pgx.ConnectConfig(ctx, cfg)
					require.NoError(t, err)
					defer conn.Close(ctx)

					for _, sql := range append([]string{
						"CREATE TEMP TABLE discard_txn(x int)",
						"INSERT INTO discard_txn VALUES (1)",
					}, tc.statements...) {
						_, err := conn.Exec(ctx, sql)
						require.NoError(t, err, sql)
					}

					var exists bool
					require.NoError(t, conn.QueryRow(ctx, "SELECT to_regclass('pg_temp.discard_txn') IS NOT NULL").Scan(&exists))
					require.Equal(t, tc.wantTable, exists)
					if tc.wantTable {
						var n int
						require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM discard_txn").Scan(&n))
						require.Equal(t, 1, n)
					}
				})
			}
		})
	}
}
