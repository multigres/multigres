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
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestValuesLimitDoesNotWidenDelete(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping VALUES integration test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found")
	}
	setup := getSharedSetup(t)
	setup.SetupTest(t)
	ctx := utils.WithTimeout(t, 30*time.Second)

	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			conn, err := pgx.Connect(ctx, shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable"))
			require.NoError(t, err)
			defer conn.Close(ctx)

			for _, protocol := range []string{"simple", "extended"} {
				t.Run(protocol, func(t *testing.T) {
					tx, err := conn.Begin(ctx)
					require.NoError(t, err)
					defer tx.Rollback(ctx)
					_, err = tx.Exec(ctx, "CREATE TABLE values_limit_delete (id int PRIMARY KEY)")
					require.NoError(t, err)
					_, err = tx.Exec(ctx, "INSERT INTO values_limit_delete VALUES (1), (2)")
					require.NoError(t, err)

					// LIMIT 0 must exclude every candidate, independent of row order.
					const query = "DELETE FROM values_limit_delete WHERE id IN (VALUES (1), (2) LIMIT 0)"
					var deleted int64
					if protocol == "extended" {
						// ExecParams uses extended protocol even without bind arguments.
						result := conn.PgConn().ExecParams(ctx, query, nil, nil, nil, nil).Read()
						require.NoError(t, result.Err)
						deleted = result.CommandTag.RowsAffected()
					} else {
						tag, err := tx.Exec(ctx, query)
						require.NoError(t, err)
						deleted = tag.RowsAffected()
					}
					t.Logf("deleted rows: %d", deleted)
					require.Zero(t, deleted)
					var remaining int
					err = tx.QueryRow(ctx, "SELECT count(*) FROM values_limit_delete").Scan(&remaining)
					require.NoError(t, err)
					require.Equal(t, 2, remaining)
				})
			}
		})
	}
}
