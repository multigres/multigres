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
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/multigres/multigres/go/test/endtoend/shardsetup"
	"github.com/multigres/multigres/go/test/utils"
)

func TestMultiStatementTransactionSemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-statement test in short mode")
	}
	if utils.ShouldSkipRealPostgres() {
		t.Skip("PostgreSQL binaries not found, skipping")
	}

	setup := getSharedSetup(t)
	for _, target := range setup.GetComparisonTargets(t) {
		t.Run(target.Name, func(t *testing.T) {
			ctx := utils.WithTimeout(t, 30*time.Second)
			db, err := sql.Open("postgres", shardsetup.GetTestUserDSN("localhost", target.Port, "sslmode=disable", "connect_timeout=5"))
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)

			_, err = db.ExecContext(ctx, "CREATE TEMP TABLE multi_statement_test(v int)")
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, "INSERT INTO multi_statement_test VALUES (1); SELECT 1/0")
			require.Error(t, err)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM multi_statement_test").Scan(&count))
			assert.Zero(t, count, "implicit batch transaction must roll back as one unit")

			_, err = db.ExecContext(ctx, "SELECT 1; BEGIN; INSERT INTO multi_statement_test VALUES (2)")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "ROLLBACK")
			require.NoError(t, err)
			require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM multi_statement_test").Scan(&count))
			assert.Zero(t, count, "BEGIN inside the batch must survive until the next query")

			_, err = db.ExecContext(ctx, "INSERT INTO multi_statement_test VALUES (7); COMMIT; INSERT INTO multi_statement_test VALUES (8); SELECT 1/0")
			require.Error(t, err)
			var values []int
			rows, err := db.QueryContext(ctx, "SELECT v FROM multi_statement_test ORDER BY v")
			require.NoError(t, err)
			defer rows.Close()
			for rows.Next() {
				var value int
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			assert.Equal(t, []int{7}, values)

			_, err = db.ExecContext(ctx, "DISCARD TEMP")
			require.NoError(t, err)
			err = db.QueryRowContext(ctx, "SELECT count(*) FROM multi_statement_test").Scan(&count)
			require.Error(t, err, "DISCARD TEMP must release the reservation and remove the temp table")
		})
	}
}
