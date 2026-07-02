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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

// TestReservedConnRecoversFromStatementError is the regression for the reserved
// connection being destroyed on a plain SQL error. A runtime statement error inside
// a transaction must only ABORT the transaction (PostgreSQL keeps the backend alive);
// the client must be able to ROLLBACK / ROLLBACK TO SAVEPOINT and keep using the
// connection. Before the fix, the multigateway destroyed the reserved backend and the
// recovery statement returned SQLSTATE 40001.
func TestReservedConnRecoversFromStatementError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end tests in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	ctx := utils.WithTimeout(t, 60*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			connect := func(t *testing.T) *pgx.Conn {
				dsn := shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5")
				conn, err := pgx.Connect(ctx, dsn)
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close(ctx) })
				return conn
			}

			// A prior successful statement establishes the reserved connection, then a
			// runtime error aborts the txn; plain ROLLBACK must recover.
			t.Run("ROLLBACK after runtime error", func(t *testing.T) {
				conn := connect(t)

				_, err := conn.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = conn.Exec(ctx, "SELECT 1") // establishes the reservation
				require.NoError(t, err)

				// QueryRow forces the extended protocol (Parse/Bind/Execute), which is
				// the path PortalStreamExecute runs on and where the bug lives.
				var divResult int
				err = conn.QueryRow(ctx, "SELECT 1/0").Scan(&divResult) // runtime error (division_by_zero)
				require.Error(t, err, "division by zero should error")

				_, err = conn.Exec(ctx, "ROLLBACK")
				require.NoError(t, err, "ROLLBACK after a statement error must recover the transaction")

				var n int
				require.NoError(t, conn.QueryRow(ctx, "SELECT 42").Scan(&n))
				assert.Equal(t, 42, n, "connection must be usable after recovery")
			})

			// Mirrors Postgrex/Ecto `mode: :savepoint` (Realtime authorization uses this):
			// SAVEPOINT, erroring statement, ROLLBACK TO SAVEPOINT, continue.
			t.Run("ROLLBACK TO SAVEPOINT after runtime error", func(t *testing.T) {
				conn := connect(t)

				_, err := conn.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				_, err = conn.Exec(ctx, "SAVEPOINT sp1") // establishes the reservation
				require.NoError(t, err)

				// QueryRow forces the extended protocol (Parse/Bind/Execute), which is
				// the path PortalStreamExecute runs on and where the bug lives.
				var divResult int
				err = conn.QueryRow(ctx, "SELECT 1/0").Scan(&divResult)
				require.Error(t, err)

				_, err = conn.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1")
				require.NoError(t, err, "ROLLBACK TO SAVEPOINT after a statement error must recover")

				var n int
				require.NoError(t, conn.QueryRow(ctx, "SELECT 42").Scan(&n))
				assert.Equal(t, 42, n)

				_, _ = conn.Exec(ctx, "ROLLBACK")
			})
		})
	}
}
